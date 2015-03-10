package es.cancamusa.kafka;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Simple kafka consumer controller. It supports GZIP encoding and decoding through serializer classes
 * @author dgutierrez - alonsod86@gmail.com
 *
 */
public class KafkaConsumerController {
	private int a_numThreads = 1;

	private boolean initialized = false;

	private ConsumerConnector consumer;
	private ConsumerConfig config;
	private ExecutorService executor;

	private Properties props;
	
	public KafkaConsumerController() {
		props = new Properties();
	}
	
	/**
	 * Initializes the kafka consumer
	 */
	private void initialize() {
		try {
			props.load(KafkaConsumerController.class.getResourceAsStream("/application.properties"));
			
			this.config = new ConsumerConfig(props);
			this.consumer = Consumer.createJavaConsumerConnector(config);
			initialized = true;
		} catch (Exception e) {
			initialized = false;
			System.err.println("Unable to start kafka consumer");
		}
	}

	/**
	 * Returns an instance to the consumer ready for execution
	 * @param topic
	 * @return
	 */
	public boolean startConsumer(String topic) {
		List<KafkaStream<byte[], byte[]>> streams = null;
		
		// consumer and executor shutdown
		shutdown();

		// initialization if required
		if (!initialized) {
			initialize();
		}

		// create the stream with the given topic 
		try {
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, new Integer(a_numThreads));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			streams = consumerMap.get(topic);
		} catch (Exception e) {
			System.err.println("FATAL ERROR STARTING KAFKA STREAM. The system will not receive any message from the stream. Please, check kafka cluster.");
			return false;
		}

		// now launch all the threads
		try {
			executor = Executors.newFixedThreadPool(a_numThreads);

			// now create an object to consume the messages
			int threadNumber = 0;
			for (@SuppressWarnings("rawtypes") final KafkaStream stream : streams) {
				System.out.println("Consumer thread ["+threadNumber+"] is ready");
				executor.submit(new ConsumerStream(stream, threadNumber, Boolean.valueOf(props.getProperty("kafka.gzip"))));
				threadNumber++;
			}

			return true;
		} catch (Exception e) {
			System.err.println("FATAL ERROR STARTING KAFKA CONSUMER. The system will not receive any message from the stream. Please, check kafka cluster.");
			return false;
		}
	}

	/**
	 * Kafka consumer shutdown
	 */
	public void shutdown() {
		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
	}
}
