package io.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "http://localhost:9092";
        String topic = "demo-topic";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            // Create a producer record
            final String key = "id_" + i, message = "Hello World " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

            // Send data
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("-----> Message send failed", e);
                    return;
                }

                logger.info("-----> Message sent successfully, " +
                                "key: {}, topic: {}, partition: {}, offset: {}, timestamp: {}",
                        key,
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp());
            });
        }

        // Flush and close producer
        producer.close();
    }
}
