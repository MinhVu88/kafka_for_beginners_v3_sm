package io.conduktor.demos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

	public static void main(String[] args) {
		logger.info("ConsumerDemo");

		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "consumer_demo_with_shutdown";
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

		kafkaConsumerConfigProperties.setProperty(
			ConsumerConfig.GROUP_ID_CONFIG,
			groupId
		);

		kafkaConsumerConfigProperties.setProperty(
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
			"earliest"
		);

		try (
			// create Kafka consumer
			KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfigProperties)
		) {
			// reference the current/main thread, on which the shutdown hook for consumer runs
			final Thread currentThread = Thread.currentThread();

			// create a shutdown hook in the current thread
			Runtime.getRuntime().addShutdownHook(
				new Thread(() -> {
					logger.info("a shutdown hook detected. Exit program by calling kafkaConsumer.wakup()");

					kafkaConsumer.wakeup();

					// join the current thread to enable the code execution in there
					try {
						currentThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				})
			);

			kafkaConsumer.subscribe(List.of(topic));

			// poll for new data
			while(true) {
				// logger.info("polling for new data...");

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
		} catch (WakeupException e) {
			logger.info("WakeupException (ignored as it is an expected exception when closing a consumer)");
		} catch (Exception e) {
			logger.info("unexpected exception: " + e.getMessage());
		} finally {
			logger.info("consumer is now gracefully closed");
		}
	}
}
