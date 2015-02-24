package storm.cookbook;

import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

import backtype.storm.tuple.Tuple;

// simple abstraction of some of the initialization code
public class StormTestCase {

	// usually mockeries are called as mockery, or context.
	// Initialize JMock context and use it to mock any class
	protected Mockery context = new Mockery() {
		{
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};

	protected Tuple getTuple() {
		// tuple class mocked
		final Tuple tuple = context.mock(Tuple.class);
		return tuple;
	}

}