package es.cancamusa.kafka;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import kafka.producer.KeyedMessage;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public boolean startConsumer() {
    	KafkaConsumerController consumer = new KafkaConsumerController();
    	return consumer.startConsumer("my-topic");
    }
    
    public boolean startProducer() {
    	KafkaProducerController producer = new KafkaProducerController();
    	try {
    		producer.getProducer().send(new KeyedMessage<String, String>("my-topic", "my-custom-awesome-message"));
    		return true;
    	} catch (Exception e) {
    		fail(e.getMessage());
    		return false;
    	}
    }
    
    public void testApp() {
    	// start a consumer
    	assertEquals(true, startConsumer());
    	// when consumer is ready, start a producer to send a message
    	assertEquals(true, startProducer());
    }
}
