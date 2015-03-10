package es.cancamusa.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.Decoder;
import es.cancamusa.kafka.codecs.GzipDecoder;
 
/**
 * Stream reader for a Kafka consumer. This class receives log messages from a pre-configured topic
 * @author dgutierrez
 *
 */
@SuppressWarnings("rawtypes")
public class ConsumerStream implements Runnable {
	private KafkaStream m_stream;
    private int m_threadNumber;
    private boolean gzip = false;
    
    public ConsumerStream(KafkaStream a_stream, int a_threadNumber, boolean gzip) {
        this.m_threadNumber = a_threadNumber;
        this.m_stream = a_stream;
        this.gzip = gzip;
    }
 
    @SuppressWarnings("unchecked")
    public void run() {
    	Decoder decoder = null;
    	
    	if (gzip) {
    		decoder = new GzipDecoder(null);
    	}
    	
    	System.out.println("Consumer thread ["+m_threadNumber+"] is running");
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		
        while (it.hasNext()) {
        	
        	// try to parse response from Sicarius2
        	try {
        		// get content bytes
        		byte[] message = it.next().message();
        		
        		// encoded traffic
        		if (gzip) {
        			System.out.println(decoder.fromBytes(message));
        		// clear traffic
        		} else {
        			System.out.println(new String(message));
        		}
        		
        	} catch (Exception e) {
        		System.err.println("Error processing message stream");
        	}
        }
    }
}