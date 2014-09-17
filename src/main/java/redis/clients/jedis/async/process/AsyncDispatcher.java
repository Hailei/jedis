package redis.clients.jedis.async.process;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.async.callback.AsyncResponseCallback;
import redis.clients.jedis.async.request.RequestBuilder;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class AsyncDispatcher extends Thread {
    private static final int SELECTOR_SELECT_TIMEOUT = 300;

    protected Logger log = Logger.getLogger(getClass().getName());

    private Connection connection;
    private Selector selector;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ByteBuffer readBuffer;
    private AtomicBoolean wakeUp = new AtomicBoolean();
    private volatile boolean inWriteNowLoop;
    private volatile boolean writeSuspended;
    private Deque<AsyncJedisTask> readTaskQueue = new LinkedBlockingDeque<AsyncJedisTask>();
    private Deque<AsyncJedisTask> writeTaskQueue = new LinkedBlockingDeque<AsyncJedisTask>();

    private String password;
    private int writeHandleLimit = 100;

    static {
	/**
	 * this is to avoid the jvm bug: NullPointerException in Selector.open()
	 * http://bugs.sun.com/view_bug.do?bug_id=6427854
	 */
	try {
	    Selector.open().close();
	} catch (IOException ie) {
	}
    }

    public AsyncDispatcher(Connection connection, int bufferSize)
	    throws IOException {
	this.connection = connection;
	connect();

	selector = Selector.open();
	configureSocketChannelToUseSelector();

	if (bufferSize <= 0) {
	    throw new IllegalArgumentException("Buffer bufferSize <= 0");
	}

	readBuffer = ByteBuffer.allocateDirect(bufferSize);
    }

    public void setPassword(String password) {
	this.password = password;
	handleAuth();
    }

    private void configureSocketChannelToUseSelector() {
	try {
	    SocketChannel socketChannel = connection.getSocketChannel();
	    socketChannel.configureBlocking(false);
	    socketChannel.register(selector, SelectionKey.OP_READ);
	} catch (IOException e) {
	    throw new JedisConnectionException(e);
	}
    }

    @Override
    public void run() {
	while ((!shutdown.get()) || writeTaskQueue.peek() != null
		|| readTaskQueue.peek() != null) {

	    try {
		interestOpWriteIfAnyRequestPending();
		wakeUp.set(false);
		int num = selector.select(SELECTOR_SELECT_TIMEOUT);
		if (num < 0) {
		    continue;
		}
		Set selectedKeys = selector.selectedKeys();
		Iterator it = selectedKeys.iterator();
		while (it.hasNext()) {
		    SelectionKey key = (SelectionKey) it.next();

		    if (key.isReadable()) {
			handleRead(key);
		    }

		    if (key.isValid() && key.isWritable()) {
			SocketChannel sc = (SocketChannel) key.channel();
			handleWrite(sc);
		    }

		    it.remove();
		}
	    } catch (IOException e) {
		handleConnectionException(new JedisConnectionException(e));
	    } catch (JedisConnectionException e) {
		handleConnectionException(e);
	    }
	    if (wakeUp.get())
		selector.wakeup();
	}
    }

    public synchronized void registerRequest(AsyncJedisTask task) {
	if (writeSuspended || inWriteNowLoop) {
	    writeTaskQueue.add(task);
	} else {
	    if (!_write(connection.getSocketChannel(), task)) {
		writeTaskQueue.addFirst(task);
	    }
	}

	if (wakeUp.compareAndSet(false, true)) {
	    selector.wakeup();
	}
    }

    public void setShutdown(boolean shutdown) {
	this.shutdown.set(shutdown);
    }

    private synchronized void handleConnectionException(
	    JedisConnectionException e) {
	while (readTaskQueue.peek() != null) {
	    AsyncJedisTask task = readTaskQueue.poll();
	    task.getCallback().execute(null, e);
	}

	// discard connection
	disconnect();

	// reconnect
	while (!shutdown.get() && !connection.isConnected()) {
	    try {
		connection.connect();
		configureSocketChannelToUseSelector();
		handleAuth();
	    } catch (JedisConnectionException e1) {
		try {
		    log.warning("Connection failed to Redis "
			    + connection.getHost() + ":" + connection.getPort()
			    + ", sleeping and retry...");
		    Thread.sleep(1000);
		} catch (InterruptedException e2) {
		}
	    }
	}

	if (shutdown.get()) {
	    while (writeTaskQueue.peek() != null) {
		AsyncJedisTask task = writeTaskQueue.poll();
		task.getCallback().execute(null, e);
	    }
	}
    }

    private void handleAuth() {
	if (password != null) {
	    writeTaskQueue.addFirst(buildAuthRequest());
	}
    }

    private AsyncJedisTask buildAuthRequest() {
	return new AsyncJedisTask(RequestBuilder.build(Protocol.Command.AUTH,
		password), new AsyncResponseCallback<String>() {
	    @Override
	    public void execute(String response, JedisException exc) {
		//
	    }
	});
    }

    private synchronized boolean _write(SocketChannel sc, AsyncJedisTask task) {

	int writtenLen = 0;
	boolean isFinish = false;
	while (task.getWriteBuf().hasRemaining()) {
	    try {
		writtenLen = sc.write(task.getWriteBuf());
	    } catch (IOException e) {
		throw new JedisConnectionException(
			"occuors an error while write on channel", e);
	    }
	    if (writtenLen == 0)
		break;
	}

	if (task.isWriteComplete()) {
	    readTaskQueue.add(task);
	    writeSuspended = false;
	    isFinish = true;
	} else {
	    writeSuspended = true;
	}

	return isFinish;
    }

    private void handleWrite(SocketChannel sc) throws IOException {
	// System.out.println("handler write");

	int completeCount = 0;
	AsyncJedisTask task = writeTaskQueue.peek();
	inWriteNowLoop = true;
	while (task != null && completeCount < writeHandleLimit) {
	    _write(sc, task);
	}
	inWriteNowLoop = false;

    }

    private void handleRead(SelectionKey key) throws IOException {
	// System.out.println("handler Read");
	int numOfBytes;
	do {
	    numOfBytes = readDataFromSocket(key);
	    if (numOfBytes == -1) {
		key.cancel();
		throw new JedisConnectionException(
			"It seems like server has closed the connection.");
	    } else if (numOfBytes > 0) {
		AsyncJedisTask task = readTaskQueue.peek();
		while (readBuffer.hasRemaining()) {
		    if (task == null) {
			throw new JedisConnectionException(
				"Remaining received data but no remaining request");
		    }

		    task.appendPartialResponse(readBuffer.get());
		    if (task.isReadComplete()) {
			// TODO : Thread Pool?
			task.callback();

			readTaskQueue.poll();
			task = readTaskQueue.peek();
		    }
		}
	    }
	} while (numOfBytes > 0);
    }

    private int readDataFromSocket(SelectionKey key) throws IOException {
	// Read the data
	SocketChannel sc = (SocketChannel) key.channel();

	readBuffer.clear();
	int numOfBytes = sc.read(readBuffer);
	readBuffer.flip();

	return numOfBytes;
    }

    private void interestOpWriteIfAnyRequestPending()
	    throws ClosedChannelException {
	if (writeTaskQueue.peek() != null) {
	    // we're interest of write
	    connection.getSocketChannel().register(selector,
		    SelectionKey.OP_READ | SelectionKey.OP_WRITE);
	}
    }

    private void connect() {
	if (!connection.isConnected()) {
	    connection.connect();
	}
    }

    public void disconnect() {
	if (connection.isConnected()) {
	    connection.disconnect();
	}
    }

}
