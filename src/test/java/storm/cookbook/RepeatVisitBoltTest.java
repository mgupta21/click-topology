package storm.cookbook;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.cookbook.bolt.RepeatVisitBolt;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jmock.Expectations;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;

// Start Redis server before executing Test
// /usr/local/bin/redis-server

// Parameterized is a runner inside JUnit that run same test case for all set of inputs
// - The class has single constructor that stores the test data.
// - The class has a static method that generates and returns test data.
@RunWith(value = Parameterized.class)
public class RepeatVisitBoltTest extends StormTestCase {

    // data generator method must be annotated with @Parameters
    // Each array element represents data to be used in each test execution
    // Number of elements in each array must be same as number of parameters in
    // constructor
    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        // 2-D array
        Object[][] data = new Object[][] {
            {"192.168.33.100", "Client1", "myintranet.com", "false"},
            {"192.168.33.100", "Client1", "myintranet.com", "false"},
            {"192.168.33.101", "Client2", "myintranet1.com", "true"},
            {"192.168.33.102", "Client3", "myintranet2.com", "false"}};
        // return list of arrays
        return Arrays.asList(data);
    }

    private Jedis  jedis;

    private String ip;
    private String clientKey;
    private String url;
    private String expected;

    public RepeatVisitBoltTest(String ip, String clientKey, String url,
        String expected) {
        this.clientKey = clientKey;
        this.ip = ip;
        this.url = url;
        this.expected = expected;
    }

    @BeforeClass
    public static void setupJedis() {

        Jedis jedis = new Jedis("localhost", 6379);
        jedis.flushDB();
        Iterator<Object[]> it = data().iterator();
        // Before the tests execute, the static data is populated into the Redis
        // DB
        while (it.hasNext()) {
            Object[] values = it.next();
            // In each object, check third element of array,
            if (values[3].equals("false")) {
                String key = values[2] + ":" + values[1];
                jedis.set(key, "visited");// unique, meaning it must exist
            }
        }
    }

    @Test
    public void testExecute() {

        jedis = new Jedis("localhost", 6379);

        // test class
        RepeatVisitBolt bolt = new RepeatVisitBolt();
        Map<String, String> config = new HashMap<String, String>();
        config.put("redisHost", "localhost");
        config.put("redisPort", "6379");

        // mocked OutputCollector
        final OutputCollector collector = context.mock(OutputCollector.class);

        // Initialize bolt, set redis DB
        bolt.prepare(config, null, collector);

        assertEquals(true, bolt.isConnected());

        final Tuple tuple = getTuple();

        // define behavior to mock
        context.checking(new Expectations() {

            {
                oneOf(tuple).getStringByField(Fields.IP);
                will(returnValue(ip));
                oneOf(tuple).getStringByField(Fields.CLIENT_KEY);
                will(returnValue(clientKey));
                oneOf(tuple).getStringByField(Fields.URL);
                will(returnValue(url));
                oneOf(collector).emit(new Values(clientKey, url, expected));
            }
        });

        // test the bolt
        bolt.execute(tuple);
        context.assertIsSatisfied();

        if (jedis != null)
            jedis.disconnect();
    }

}