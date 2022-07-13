package io.conduktor.demos.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	public static void main(String[] args) {
		logger.info("ProducerDemo");

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

		// create a Producer record
		ProducerRecord<String, String> kafkaProducerRecord = new ProducerRecord<>("topic1", "value");

		// send data to a kafka cluster asynchronously
		kafkaProducer.send(kafkaProducerRecord);

		// flush sent data synchronously & close Producer
		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
