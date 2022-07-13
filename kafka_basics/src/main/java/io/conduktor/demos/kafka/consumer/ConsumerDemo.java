package io.conduktor.demos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
// import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

	public static void main(String[] args) {
		logger.info("ConsumerDemo");

		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "consumer_demo";
		String topic = "topic1";

		// create Kafka consumer configs
		Properties kafkaConsumerConfigProperties = new Properties();

		kafkaConsumerConfigProperties.setProperty(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
			bootstrapServer
		);

		kafkaConsumerConfigProperties.setProperty(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			StringDeserializer.class.getName()
		);

		kafkaConsumerConfigProperties.setProperty(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			StringDeserializer.class.getName()
		);

		kafkaConsumerConfigProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		/*
			- For ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, there can either be "none" or "earliest" or "latest".
			- "none" -> if no previous offsets are found, don't set this property for consumer configs.
			- "earliest" -> read from the beginning of the topic (the 1st/earliest message).
			- "latest" -> read only the current message within a topic.
		*/
		kafkaConsumerConfigProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (
			// create Kafka consumer
			KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfigProperties)
		) {
			// consumer subscribed to 1 topic
			// kafkaConsumer.subscribe(Collections.singleton(topic));

			// consumer subscribed to multiple topics
			kafkaConsumer.subscribe(Arrays.asList(topic));

			// poll for new data
			while(true) {
				logger.info("polling for new data...");

				// poll() either returns some records immediately or waits for 1 second to return them (if there's any).
				// If after 1 sec no records are received, consumerRecords will be an empty collection.
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

				for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					logger.info(
						"Key: " + consumerRecord.key() +
						" | Value: " + consumerRecord.value() +
						" | Partition: " + consumerRecord.partition() +
						" | Offset: " + consumerRecord.offset()
					);
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
