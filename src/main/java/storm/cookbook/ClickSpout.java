package storm.cookbook;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Topology assumes that the web server pushes messages onto a Redis queue.
 * This spout injects messages into the Storm cluster as a stream
 * 
 */

public class ClickSpout extends BaseRichSpout {

	public static Logger LOG = Logger.getLogger(ClickSpout.class);

	private Jedis jedis;
	private String host;
	private int port;
	// provides api to emit tuples, tag id to message to ensure each message is processed at-least once 
	private SpoutOutputCollector collector;

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

		// uses default streamId "default"
		// Bolts can subscribe to multiple streams from spouts and bolts.
		// emit tuple (object) with fields IP, URL, Client_Key
		outputFieldsDeclarer.declare(new Fields(storm.cookbook.Fields.IP,
				storm.cookbook.Fields.URL, storm.cookbook.Fields.CLIENT_KEY));
		
		// declares schema for streams,
		// spout can emit data to multiple streams using declareStream
		// and by specifying streamID while emitting tuple
		// outputFieldsDeclarer.declareStream(String streamId, Fields fields);

	}

	// called only once when spout is initialized
	// defines logic to connect and fetch from external source
	public void open(Map conf, TopologyContext topologyContext,
			SpoutOutputCollector spoutOutputCollector) {
		
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
		this.collector = spoutOutputCollector;
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
	}

	// Storm calls this method to call spout to emit tuples to output collector
	public void nextTuple() {
		
		// get data from external source
		String content = jedis.rpop("count");
		if (content == null || "nil".equals(content)) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
			}
		} else {
			JSONObject obj = (JSONObject) JSONValue.parse(content);
			String ip = obj.get(storm.cookbook.Fields.IP).toString();
			String url = obj.get(storm.cookbook.Fields.URL).toString();
			String clientKey = obj.get(storm.cookbook.Fields.CLIENT_KEY)
					.toString();
			
			// emit data/streams to instance of SpoutOutputCollector
			collector.emit(new Values(ip, url, clientKey));
		}
	}

}

