package redis.clients.jedis.tests.commands;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.tests.HostAndPortUtil;
import redis.clients.jedis.tests.JedisTestBase;

public class ClusterCommandsTest extends JedisTestBase {
    private static Jedis node1;
    private static Jedis node2;

    private HostAndPort nodeInfo1 = HostAndPortUtil.getClusterServers().get(0);
    private HostAndPort nodeInfo2 = HostAndPortUtil.getClusterServers().get(1);

    @Before
    public void setUp() throws Exception {

	node1 = new Jedis(nodeInfo1.getHost(), nodeInfo1.getPort());
	node1.connect();
	node1.flushAll();

	node2 = new Jedis(nodeInfo2.getHost(), nodeInfo2.getPort());
	node2.connect();
	node2.flushAll();
    }

    @After
    public void tearDown() {
	node1.disconnect();
	node2.disconnect();
    }

    @AfterClass
    public static void removeSlots() throws InterruptedException {
	String[] nodes = node1.clusterNodes().split("\n");
	String node1Id = nodes[0].split(" ")[0];
	node1.clusterDelSlots(1, 2, 3, 4, 5, 500);
	node1.clusterSetSlotNode(5000, node1Id);
	node1.clusterDelSlots(5000, 10000);
	node1.clusterDelSlots(3000, 3001, 3002);
	node2.clusterDelSlots(4000, 4001, 4002);
	node1.clusterAddSlots(6000);
	node1.clusterDelSlots(6000);
	waitForGossip();
	node2.clusterDelSlots(6000);
	node1.clusterDelSlots(6000);
    }

    private static void waitForGossip() {
	boolean notReady = true;
	while (notReady) {
	    if (node1.clusterNodes().contains("6000")) {
		notReady = false;
	    }
	}
    }

    private static int getClusterAttribute(String clusterInfo,
	    String attributeName) {
	for (String infoElement : clusterInfo.split("\n")) {
	    if (infoElement.contains(attributeName)) {
		return Integer.valueOf(infoElement.split(":")[1].trim());
	    }
	}
	return 0;
    }

    @Test
    public void clusterSetSlotImporting() {
	node2.clusterAddSlots(6000);
	String[] nodes = node1.clusterNodes().split("\n");
	String nodeId = nodes[0].split(" ")[0];
	String status = node1.clusterSetSlotImporting(6000, nodeId);
	assertEquals("OK", status);
    }

    @Test
    public void clusterNodes() {
	String nodes = node1.clusterNodes();
	assertTrue(nodes.split("\n").length > 0);
    }

    @Test
    public void clusterSlots() {
	// please see cluster slot output format from below commit
	// @see:
	// https://github.com/antirez/redis/commit/e14829de3025ffb0d3294e5e5a1553afd9f10b60
	String status = node1.clusterAddSlots(3000, 3001, 3002);
	assertEquals("OK", status);
	status = node2.clusterAddSlots(4000, 4001, 4002);
	assertEquals("OK", status);

	List<Object> slots = node1.clusterSlots();
	assertNotNull(slots);
	assertTrue(slots.size() > 0);

	for (Object slotInfoObj : slots) {
	    List<Object> slotInfo = (List<Object>) slotInfoObj;
	    assertNotNull(slots);
	    assertTrue(slots.size() >= 2);

	    assertTrue(slotInfo.get(0) instanceof Long);
	    assertTrue(slotInfo.get(1) instanceof Long);

	    if (slots.size() > 2) {
		// assigned slots
		assertTrue(slotInfo.get(2) instanceof List);
	    }
	}
    }

    @Test
    public void clusterMeet() {
	String status = node1.clusterMeet("127.0.0.1", nodeInfo2.getPort());
	assertEquals("OK", status);
    }

    @Test
    public void clusterAddSlots() {
	String status = node1.clusterAddSlots(1, 2, 3, 4, 5);
	assertEquals("OK", status);
    }

    @Test
    public void clusterDelSlots() {
	node1.clusterAddSlots(900);
	String status = node1.clusterDelSlots(900);
	assertEquals("OK", status);
    }

    @Test
    public void clusterInfo() {
	String info = node1.clusterInfo();
	assertNotNull(info);
    }

    @Test
    public void clusterGetKeysInSlot() {
	node1.clusterAddSlots(500);
	List<String> keys = node1.clusterGetKeysInSlot(500, 1);
	assertEquals(0, keys.size());
    }

    @Test
    public void clusterSetSlotNode() {
	String[] nodes = node1.clusterNodes().split("\n");
	String nodeId = nodes[0].split(" ")[0];
	String status = node1.clusterSetSlotNode(10000, nodeId);
	assertEquals("OK", status);
    }

    @Test
    public void clusterSetSlotMigrating() {
	node1.clusterAddSlots(5000);
	String[] nodes = node1.clusterNodes().split("\n");
	String nodeId = nodes[0].split(" ")[0];
	String status = node1.clusterSetSlotMigrating(5000, nodeId);
	assertEquals("OK", status);
    }

}