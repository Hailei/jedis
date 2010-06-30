package redis.clients.jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Jedis extends Client {
    public Jedis(String host) {
	super(host);
    }

    public String ping() throws JedisException {
	return sendCommand("PING").getStatusCodeReply();
    }

    public String set(String key, String value) throws JedisException {
	return sendCommand("SET", key, value).getStatusCodeReply();
    }

    public String get(String key) throws JedisException {
	return sendCommand("GET", key).getBulkReply();
    }

    public void quit() throws JedisException {
	sendCommand("QUIT");
    }

    public int exists(String key) throws JedisException {
	return sendCommand("EXISTS", key).getIntegerReply();
    }

    public int del(String... keys) throws JedisException {
	return sendCommand("DEL", keys).getIntegerReply();
    }

    public String type(String key) throws JedisException {
	return sendCommand("TYPE", key).getStatusCodeReply();
    }

    public String flushDB() throws JedisException {
	return sendCommand("FLUSHDB").getStatusCodeReply();
    }

    public List<String> keys(String pattern) throws JedisException {
	return sendCommand("KEYS", pattern).getMultiBulkReply();
    }

    public String randomKey() throws JedisException {
	return sendCommand("RANDOMKEY").getBulkReply();
    }

    public String rename(String oldkey, String newkey) throws JedisException {
	return sendCommand("RENAME", oldkey, newkey).getStatusCodeReply();
    }

    public int renamenx(String oldkey, String newkey) throws JedisException {
	return sendCommand("RENAMENX", oldkey, newkey).getIntegerReply();
    }

    public int dbSize() throws JedisException {
	return sendCommand("DBSIZE").getIntegerReply();
    }

    public int expire(String key, int seconds) throws JedisException {
	return sendCommand("EXPIRE", key, String.valueOf(seconds))
		.getIntegerReply();
    }

    public int expireAt(String key, long unixTime) throws JedisException {
	return sendCommand("EXPIREAT", key, String.valueOf(unixTime))
		.getIntegerReply();
    }

    public int ttl(String key) throws JedisException {
	return sendCommand("TTL", key).getIntegerReply();
    }

    public String select(int index) throws JedisException {
	return sendCommand("SELECT", String.valueOf(index))
		.getStatusCodeReply();
    }

    public int move(String key, int dbIndex) throws JedisException {
	return sendCommand("MOVE", key, String.valueOf(dbIndex))
		.getIntegerReply();
    }

    public String flushAll() throws JedisException {
	return sendCommand("FLUSHALL").getStatusCodeReply();
    }

    public String getSet(String key, String value) throws JedisException {
	return sendCommand("GETSET", key, value).getBulkReply();
    }

    public List<String> mget(String... keys) throws JedisException {
	return sendCommand("MGET", keys).getMultiBulkReply();
    }

    public int setnx(String key, String value) throws JedisException {
	return sendCommand("SETNX", key, value).getIntegerReply();
    }

    public String setex(String key, int seconds, String value)
	    throws JedisException {
	return sendCommand("SETEX", key, String.valueOf(seconds), value)
		.getStatusCodeReply();
    }

    public String mset(String... keysvalues) throws JedisException {
	return sendCommand("MSET", keysvalues).getStatusCodeReply();
    }

    public int msetnx(String... keysvalues) throws JedisException {
	return sendCommand("MSETNX", keysvalues).getIntegerReply();
    }

    public int decrBy(String key, int integer) throws JedisException {
	return sendCommand("DECRBY", key, String.valueOf(integer))
		.getIntegerReply();
    }

    public int decr(String key) throws JedisException {
	return sendCommand("DECR", key).getIntegerReply();
    }

    public int incrBy(String key, int integer) throws JedisException {
	return sendCommand("INCRBY", key, String.valueOf(integer))
		.getIntegerReply();
    }

    public int incr(String key) throws JedisException {
	return sendCommand("INCR", key).getIntegerReply();
    }

    public int append(String key, String value) throws JedisException {
	return sendCommand("APPEND", key, value).getIntegerReply();
    }

    public String substr(String key, int start, int end) throws JedisException {
	return sendCommand("SUBSTR", key, String.valueOf(start),
		String.valueOf(end)).getBulkReply();
    }

    public int hset(String key, String field, String value)
	    throws JedisException {
	return sendCommand("HSET", key, field, value).getIntegerReply();
    }

    public String hget(String key, String field) throws JedisException {
	return sendCommand("HGET", key, field).getBulkReply();
    }

    public int hsetnx(String key, String field, String value)
	    throws JedisException {
	return sendCommand("HSETNX", key, field, value).getIntegerReply();
    }

    public String hmset(String key, Map<String, String> hash)
	    throws JedisException {
	List<String> params = new ArrayList<String>();
	params.add(key);

	for (String field : hash.keySet()) {
	    params.add(field);
	    params.add(hash.get(field));
	}
	return sendCommand("HMSET", params.toArray(new String[params.size()]))
		.getStatusCodeReply();
    }

    public List<String> hmget(String key, String... fields)
	    throws JedisException {
	String[] params = new String[fields.length + 1];
	params[0] = key;
	System.arraycopy(fields, 0, params, 1, fields.length);
	return sendCommand("HMGET", params).getMultiBulkReply();
    }

    public int hincrBy(String key, String field, int value)
	    throws JedisException {
	return sendCommand("HINCRBY", key, field, String.valueOf(value))
		.getIntegerReply();
    }

    public int hexists(String key, String field) throws JedisException {
	return sendCommand("HEXISTS", key, field).getIntegerReply();
    }

    public int hdel(String key, String field) throws JedisException {
	return sendCommand("HDEL", key, field).getIntegerReply();
    }

    public int hlen(String key) throws JedisException {
	return sendCommand("HLEN", key).getIntegerReply();
    }

    public List<String> hkeys(String key) throws JedisException {
	return sendCommand("HKEYS", key).getMultiBulkReply();
    }

    public List<String> hvals(String key) throws JedisException {
	return sendCommand("HVALS", key).getMultiBulkReply();
    }

    public Map<String, String> hgetAll(String key) throws JedisException {
	List<String> flatHash = sendCommand("HGETALL", key).getMultiBulkReply();
	Map<String, String> hash = new HashMap<String, String>();
	Iterator<String> iterator = flatHash.iterator();
	while (iterator.hasNext()) {
	    hash.put(iterator.next(), iterator.next());
	}

	return hash;
    }

    public int rpush(String key, String string) throws JedisException {
	return sendCommand("RPUSH", key, string).getIntegerReply();
    }

    public int lpush(String key, String string) throws JedisException {
	return sendCommand("LPUSH", key, string).getIntegerReply();
    }

    public int llen(String key) throws JedisException {
	return sendCommand("LLEN", key).getIntegerReply();
    }

    public List<String> lrange(String key, int start, int end)
	    throws JedisException {
	return sendCommand("LRANGE", key, String.valueOf(start),
		String.valueOf(end)).getMultiBulkReply();
    }

    public String ltrim(String key, int start, int end) throws JedisException {
	return sendCommand("LTRIM", key, String.valueOf(start),
		String.valueOf(end)).getStatusCodeReply();
    }

    public String lindex(String key, int index) throws JedisException {
	return sendCommand("LINDEX", key, String.valueOf(index)).getBulkReply();
    }

    public String lset(String key, int index, String value)
	    throws JedisException {
	return sendCommand("LSET", key, String.valueOf(index), value)
		.getStatusCodeReply();
    }

    public int lrem(String key, int count, String value) throws JedisException {
	return sendCommand("LREM", key, String.valueOf(count), value)
		.getIntegerReply();
    }

    public String lpop(String key) throws JedisException {
	return sendCommand("LPOP", key).getBulkReply();
    }

    public String rpop(String key) throws JedisException {
	return sendCommand("RPOP", key).getBulkReply();
    }

    public String rpoplpush(String srckey, String dstkey) throws JedisException {
	return sendCommand("RPOPLPUSH", srckey, dstkey).getBulkReply();
    }

    public int sadd(String key, String member) throws JedisException {
	return sendCommand("SADD", key, member).getIntegerReply();
    }

    public Set<String> smembers(String key) throws JedisException {
	List<String> members = sendCommand("SMEMBERS", key).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public int srem(String key, String member) throws JedisException {
	return sendCommand("SREM", key, member).getIntegerReply();
    }

    public String spop(String key) throws JedisException {
	return sendCommand("SPOP", key).getBulkReply();
    }

    public int smove(String srckey, String dstkey, String member)
	    throws JedisException {
	return sendCommand("SMOVE", srckey, dstkey, member).getIntegerReply();
    }

    public int scard(String key) throws JedisException {
	return sendCommand("SCARD", key).getIntegerReply();
    }

    public int sismember(String key, String member) throws JedisException {
	return sendCommand("SISMEMBER", key, member).getIntegerReply();
    }

    public Set<String> sinter(String... keys) throws JedisException {
	List<String> members = sendCommand("SINTER", keys).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public int sinterstore(String dstkey, String... keys) throws JedisException {
	String[] params = new String[keys.length + 1];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	return sendCommand("SINTERSTORE", params).getIntegerReply();
    }

    public Set<String> sunion(String... keys) throws JedisException {
	List<String> members = sendCommand("SUNION", keys).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public int sunionstore(String dstkey, String... keys) throws JedisException {
	String[] params = new String[keys.length + 1];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	return sendCommand("SUNIONSTORE", params).getIntegerReply();
    }

    public Set<String> sdiff(String... keys) throws JedisException {
	List<String> members = sendCommand("SDIFF", keys).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public int sdiffstore(String dstkey, String... keys) throws JedisException {
	String[] params = new String[keys.length + 1];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	return sendCommand("SDIFFSTORE", params).getIntegerReply();
    }

    public String srandmember(String key) throws JedisException {
	return sendCommand("SRANDMEMBER", key).getBulkReply();
    }

    public int zadd(String key, double score, String member)
	    throws JedisException {
	return sendCommand("ZADD", key, String.valueOf(score), member)
		.getIntegerReply();
    }

    public Set<String> zrange(String key, int start, int end)
	    throws JedisException {
	List<String> members = sendCommand("ZRANGE", key,
		String.valueOf(start), String.valueOf(end)).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public int zrem(String key, String member) throws JedisException {
	return sendCommand("ZREM", key, member).getIntegerReply();
    }

    public double zincrby(String key, double score, String member)
	    throws JedisException {
	String newscore = sendCommand("ZINCRBY", key, String.valueOf(score),
		member).getBulkReply();
	return Double.valueOf(newscore);
    }

    public int zrank(String key, String member) throws JedisException {
	return sendCommand("ZRANK", key, member).getIntegerReply();
    }

    public int zrevrank(String key, String member) throws JedisException {
	return sendCommand("ZREVRANK", key, member).getIntegerReply();
    }

    public Set<String> zrevrange(String key, int start, int end)
	    throws JedisException {
	List<String> members = sendCommand("ZREVRANGE", key,
		String.valueOf(start), String.valueOf(end)).getMultiBulkReply();
	return new LinkedHashSet<String>(members);
    }

    public Set<Tuple> zrangeWithScores(String key, int start, int end)
	    throws JedisException {
	List<String> membersWithScores = sendCommand("ZRANGE", key,
		String.valueOf(start), String.valueOf(end), "WITHSCORES")
		.getMultiBulkReply();
	Set<Tuple> set = new LinkedHashSet<Tuple>();
	Iterator<String> iterator = membersWithScores.iterator();
	while (iterator.hasNext()) {
	    set
		    .add(new Tuple(iterator.next(), Double.valueOf(iterator
			    .next())));
	}
	return set;
    }

    public Set<Tuple> zrevrangeWithScores(String key, int start, int end)
	    throws JedisException {
	List<String> membersWithScores = sendCommand("ZREVRANGE", key,
		String.valueOf(start), String.valueOf(end), "WITHSCORES")
		.getMultiBulkReply();
	Set<Tuple> set = new LinkedHashSet<Tuple>();
	Iterator<String> iterator = membersWithScores.iterator();
	while (iterator.hasNext()) {
	    set
		    .add(new Tuple(iterator.next(), Double.valueOf(iterator
			    .next())));
	}
	return set;
    }

    public int zcard(String key) throws JedisException {
	return sendCommand("ZCARD", key).getIntegerReply();
    }

    public double zscore(String key, String member) throws JedisException {
	String score = sendCommand("ZSCORE", key, member).getBulkReply();
	return Double.valueOf(score);
    }
}