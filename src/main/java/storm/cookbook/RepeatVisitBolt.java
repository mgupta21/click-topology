package storm.cookbook;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Bolts will enrich the basic data through the database or remote
 *  API lookups
 *  
 * This bolt will check the client's ID against previous visit records 
 * and emit the enriched tuple with a flag set for unique visits.
 * 
 */

public class RepeatVisitBolt extends BaseRichBolt {

	private OutputCollector collector;

	private Jedis jedis;
	private String host;
	private int port;

	// 1) A Bolt can be executed by multiple workers. The instance of a bolt 
	// is created, serialized and submitted to Nimbus.
	// 2) Nimbus creates worker instance, sends serialized bolt to worker.
	// Worker de-serialize bolt and call prepare method.
	public void prepare(Map conf, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	public boolean isConnected() {
		if (jedis == null)
			return false;
		return jedis.isConnected();
	}

	// Executed for each tuple from subscribed input streams.
	// The tuple from ClickSpout is provided by the cluster. The bolt
	// looks up previous visit flags from Redis, based on the fields
	// in the tuple, and emit the enriched tuple/
	public void execute(Tuple tuple) {
		
		// retrieve tuple from fields emitted by Spout
		String ip = tuple.getStringByField(storm.cookbook.Fields.IP);
		String clientKey = tuple
				.getStringByField(storm.cookbook.Fields.CLIENT_KEY);
		String url = tuple.getStringByField(storm.cookbook.Fields.URL);
		
		String key = url + ":" + clientKey;
		String value = jedis.get(key);
		if (value == null) {
			jedis.set(key, "visited");
			collector.emit(new Values(clientKey, url, Boolean.TRUE.toString()));
		} else {
			collector
					.emit(new Values(clientKey, url, Boolean.FALSE.toString()));
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		
		// emit default stream with id as "default"
		outputFieldsDeclarer.declare(new backtype.storm.tuple.Fields(
				storm.cookbook.Fields.CLIENT_KEY, storm.cookbook.Fields.URL,
				storm.cookbook.Fields.UNIQUE));
		
		// declares output streams
		// outputFieldsDeclarer.declareStream(streamId, fields);
	}

}
