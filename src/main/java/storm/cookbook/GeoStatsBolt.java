package storm.cookbook;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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

	private class CountryStats {

		private int countryTotal = 0;

		private static final int COUNT_INDEX = 0;
		private static final int PERCENTAGE_INDEX = 1;
		private String countryName;

		public CountryStats(String countryName) {
			this.countryName = countryName;
		}

		private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();

		public void cityFound(String cityName) {
			countryTotal++;
			if (cityStats.containsKey(cityName)) {
				cityStats.get(cityName)
						.set(COUNT_INDEX,
								cityStats.get(cityName).get(COUNT_INDEX)
										.intValue() + 1);
			} else {
				List<Integer> list = new LinkedList<Integer>();
				list.add(1);
				list.add(0);
				cityStats.put(cityName, list);
			}

			double percent = (double) cityStats.get(cityName).get(COUNT_INDEX)
					/ (double) countryTotal;
			cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
		}

		public int getCountryTotal() {
			return countryTotal;
		}

		public int getCityTotal(String cityName) {
			return cityStats.get(cityName).get(COUNT_INDEX).intValue();
		}

		public String toString() {
			return "Total Count for " + countryName + " is "
					+ Integer.toString(countryTotal) + "\n" + "Cities:  "
					+ cityStats.toString();
		}
	}

	private OutputCollector collector;
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
