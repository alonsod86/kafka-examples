package es.cancamusa.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * Simple kafka producer controller. It supports GZIP encoding and decoding through serializer classes
 * @author dgutierrez - alonsod86@gmail.com
 *
 */
public class KafkaProducerController {
	private boolean initialized = false;
	
	private ProducerConfig config = null;
	private Producer<String, String> producer = null;
	
	private Properties props = null;
	
	public KafkaProducerController() {
		props = new Properties();
	}
	
	/**
	 * Initializes the kafka producer
	 */
	private void initialize() {
		try {
			props.load(KafkaConsumerController.class.getResourceAsStream("/application.properties"));
		} catch (Exception e) {
			System.err.println("Failed to read properties file");
		}
		config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		initialized = true;
	}
	
	public Producer<String, String> getProducer() {
		if (!initialized) initialize();
		return producer;
	}

}
