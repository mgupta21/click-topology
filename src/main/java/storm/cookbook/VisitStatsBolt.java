package storm.cookbook;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * VisitorStatsBolt method provides the final counting functionality for
 *  visitors and unique visits, based on the enriched stream
 *  from the RepeatVisitBolt
 * 
 * This bolt needs to receive all the count information in order to
 * maintain a single in-memory count,
 * which is reflected in the topology definition (one executor and globalGrouping):
 *
 */
public class VisitStatsBolt extends BaseRichBolt {

	private OutputCollector collector;

	private int total = 0;
	private int uniqueCount = 0;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		boolean unique = Boolean.parseBoolean(tuple
				.getStringByField(storm.cookbook.Fields.UNIQUE));
		total++;
		if (unique)
			uniqueCount++;
		collector.emit(new Values(total, uniqueCount));

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

		// emit stream "default" with each tuple having fields total and unique
		outputFieldsDeclarer.declare(new backtype.storm.tuple.Fields(
				Fields.TOTAL_COUNT, Fields.TOTAL_UNIQUE));
	}

}
