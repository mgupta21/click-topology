package storm.cookbook;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The GeoStatsBolt simply receives the enriched tuple from GeographicBolt
 *  and maintains an in-memory structure of the data. 
 *  
 *  GeoStatsBolt designed to split population of countries b/w many bolts.
 *   however, all cities within each country must arrive at the same bolt.
 *   The topology, therefore, splits streams into the bolt on this basis:
 *   builder.setBolt("geoStats", new GeoStatsBolt(), 10)
 *   .fieldsGrouping("geographyBolt", new Fields(Fields.COUNTRY));
 *  
 *  It also emits the updated counts to any interested party.
 * 
 */

public class GeoStatsBolt extends BaseRichBolt {

	private OutputCollector collector;
	
	// # Keeps the map of all country and countryStat, list of countries in map
	// of one instance of bolt will be different from other instance. 
	// (e.g. 160 countries divided among 10 bolt instances or 10 map objects)
	private Map<String, CountryStats> stats = new HashMap<String, CountryStats>();

	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		
		String country = tuple.getStringByField(storm.cookbook.Fields.COUNTRY);
		String city = tuple.getStringByField(Fields.CITY);
		
		if (!stats.containsKey(country)) {
			stats.put(country, new CountryStats(country));
		}
		
		stats.get(country).cityFound(city);
		collector.emit(new Values(country,
				stats.get(country).getCountryTotal(), city, stats.get(country)
						.getCityTotal(city)));

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		
		outputFieldsDeclarer.declare(new backtype.storm.tuple.Fields(
				storm.cookbook.Fields.COUNTRY,
				storm.cookbook.Fields.COUNTRY_TOTAL,
				storm.cookbook.Fields.CITY, storm.cookbook.Fields.CITY_TOTAL));
	}

}
