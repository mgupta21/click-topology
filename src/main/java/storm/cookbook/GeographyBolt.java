package storm.cookbook;

import java.util.Map;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt will emit an enriched tuple by looking up the country and city
 *  of the client's IP address through a remote API call. 
 * 
 * The GeographyBolt class delegates the actual call to an injected
 *   IP resolver in order to increase the testability of the class.
 * 
 */
public class GeographyBolt extends BaseRichBolt {

	private IPResolver resolver;

	private OutputCollector collector;

	public GeographyBolt(IPResolver resolver) {
		this.resolver = resolver;
	}

	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		String ip = tuple.getStringByField(storm.cookbook.Fields.IP);
		JSONObject json = resolver.resolveIP(ip);
		String city = (String) json.get(storm.cookbook.Fields.CITY);
		String country = (String) json.get(storm.cookbook.Fields.COUNTRY_NAME);
		collector.emit(new Values(country, city));
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(storm.cookbook.Fields.COUNTRY,
				storm.cookbook.Fields.CITY));
	}

}
