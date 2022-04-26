package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

	public static void main(String[] args) {
		logger.info("Kafka producer");

		// set Kafka Producer properties
		Properties kafkaProducerConfigProperties = new Properties();

		kafkaProducerConfigProperties.setProperty(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
			"127.0.0.1:9092"
		);

		kafkaProducerConfigProperties.setProperty(
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			StringSerializer.class.getName()
		);

		kafkaProducerConfigProperties.setProperty(
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			StringSerializer.class.getName()
		);

		// create the Producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfigProperties);

		for(int i = 0; i < 10; i++) {
			String topic = "topic1";
			String value = "hello kafka" + i;
			String key = "id_" + i;

			// create a Producer record
			ProducerRecord<String, String> kafkaProducerRecord = new ProducerRecord<>(topic, key, value);

			// send data to a kafka cluster asynchronously
			kafkaProducer.send(kafkaProducerRecord, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info(
						"metadata -> Topic: " + metadata.topic() +
						" | key: " + kafkaProducerRecord.key() +
						" | Partition: " + metadata.partition() +
						" | Offset: " + metadata.offset() +
						" | Timestamp: " + metadata.timestamp()
						);
					}else {
						logger.error("Producer error: " + exception);
					}
				}
			});
		}

		// flush sent data synchronously & close Producer
		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
