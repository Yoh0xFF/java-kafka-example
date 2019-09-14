package io.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers = "https://localhost:9092";
        String topic = "demo-topic";

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Assign and seek are mostly used to replay data or fetch a specific message

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        // Seek
        consumer.seek(topicPartition, 15L);

        int total = 5, count = 0;
        boolean read = true;

        // Poll for the new data
        while (read) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("-----> Partition: {}, Offset: {}, Key: {}, Value: {}",
                        record.partition(), record.offset(), record.key(), record.value());
                if (++count >= total) {
                    read = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application!");
    }
}
