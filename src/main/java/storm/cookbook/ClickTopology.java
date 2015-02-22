package storm.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/*
 * This class defines the topology and provides the mechanisms to
 *  launch the topology into a cluster or in a local mode
 */

// Running topology in local 
// mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.cookbook.ClickTopology 

public class ClickTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;

	public static final String DEFAULT_JEDIS_PORT = "6379";

	public ClickTopology() {
		// spout with 10 threads/executors i.e
		// 10 instances to divide among 20 workers
		builder.setSpout("clickSpout", new ClickSpout(), 10);
		
		// By default storm runs 1 tasks per executor
		// builder.setBolt("info", null, 5).setNumTasks(4);

		// First layer of bolts, chained to spout
		builder.setBolt("repeatsBolt", new RepeatVisitBolt(), 10)
				// input chained to clickSpout,
				// Shuffle grouping distributes tuples equally in a
				// uniform, random way across the tasks
				.shuffleGrouping("clickSpout");
		builder.setBolt("geographyBolt",
				new GeographyBolt(new HttpIPResolver()), 10).shuffleGrouping(
				"clickSpout");

		// second layer of bolts, commutative in nature, chained to first level bolts
		// Global grouping single executor, this grouping does'nt partition 
		// the but sends complete to bolt's task
		builder.setBolt("totalStats", new VisitStatsBolt(), 1).globalGrouping(
				"repeatsBolt");
		
		// Fields grouping partition stream to each task by fields in the tuples.
		builder.setBolt("geoStats", new GeoStatsBolt(), 10).fieldsGrouping(
				"geographyBolt", new Fields(storm.cookbook.Fields.COUNTRY));
		
		// Info : Fields grouping is also used to join streams eg:
		// builder.setBolt("joiner", new OrderJoiner()).fieldsGrouping("1", new Fields("orderId")).fieldsGrouping("2", new Fields("orderRefId"));
		
		conf.put(Conf.REDIS_PORT_KEY, DEFAULT_JEDIS_PORT);
	}

	public void runLocal(int runTime) {
		conf.setDebug(true);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public void runCluster(String name, String redisHost)
			throws AlreadyAliveException, InvalidTopologyException {
		
		// 20 workers 
		conf.setNumWorkers(20);
		conf.put(Conf.REDIS_HOST_KEY, redisHost);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {

		ClickTopology topology = new ClickTopology();

		if (args != null && args.length > 1) {
			topology.runCluster(args[0], args[1]);
		} else {
			if (args != null && args.length == 1)
				System.out
						.println("Running in local mode, redis ip missing for cluster run");
			topology.runLocal(10000);
		}

	}

}

/* 
 * 1) A topology is an abstraction that defines the graph of the computation.
 * 
 * 2) A stream is an unbounded sequence of tuples that can be processed in 
 * parallel by Storm. Each stream can be processed by a single or multiple
 *  types of bolts
 *  
 * 3) Each stream in a Storm application is given an ID and the bolts can
 *  produce and consume tuples from these streams on the basis of their ID.
 */
