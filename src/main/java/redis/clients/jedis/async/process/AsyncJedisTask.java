package redis.clients.jedis.async.process;

import java.nio.ByteBuffer;

import redis.clients.jedis.Builder;
import redis.clients.jedis.async.callback.AsyncResponseCallback;
import redis.clients.jedis.async.response.BasicResponseBuilder;
import redis.clients.jedis.exceptions.JedisException;

public class AsyncJedisTask {
    private final ByteBuffer writeBuf;
    private final AsyncResponseCallback callback;
    private BasicResponseBuilder responseBuilder;
    private Builder responseTypeConverter;

    public AsyncJedisTask(byte[] request, AsyncResponseCallback callback) {
	     this.writeBuf = ByteBuffer.wrap(request);
	     this.callback = callback;
    }

    public AsyncJedisTask(byte[] request, Builder responseTypeConverter,
	    AsyncResponseCallback callback) {
         this(request, callback);
	     this.responseTypeConverter = responseTypeConverter;
    }

    public ByteBuffer getWriteBuf() {
	return this.writeBuf;
    }

    public Builder getResponseTypeConverter() {
	return responseTypeConverter;
    }

    public AsyncResponseCallback getCallback() {
	return callback;
    }

    public <T> void initializeResponseBuilder() {
	responseBuilder = new BasicResponseBuilder<T>();
    }

    public void appendPartialResponse(byte b) {
	getResponseBuilder().appendPartialResponse(b);
    }

    public boolean isWriteComplete() {
        
        return !writeBuf.hasRemaining();
    }
    public boolean isReadComplete() {
	return getResponseBuilder().isComplete();
    }

    public void callback() {
	BasicResponseBuilder responseBuilder = getResponseBuilder();
	Builder responseTypeConverter = getResponseTypeConverter();
	Object response = responseBuilder.getResponse();
	JedisException exception = responseBuilder.getException();

	try {
	    if (responseTypeConverter != null && exception == null) {
		response = responseTypeConverter.build(response);
	    }

	    getCallback().execute(response, exception);
	} catch (Exception e) {
	    getCallback().execute(null, new JedisException(e));
	}

    }

    public BasicResponseBuilder getResponseBuilder() {
	if (responseBuilder == null) {
	    initializeResponseBuilder();
	}

	return responseBuilder;
    }
}
