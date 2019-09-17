package io.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Start the consumer thread
        logger.info("Creating the consumer thread!");
        ConsumerThread consumerThread = new ConsumerThread(latch);
        new Thread(consumerThread).start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!");
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException ex) {
                logger.info("Application got interrupted!", ex);
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException ex) {
            logger.info("Application got interrupted!", ex);
        } finally {
            logger.info("Application has exited!");
        }
    }
}

class ConsumerThread implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private final String bootstrapServers = "https://localhost:9092";
    private final String group = "demo-group";
    private final String topic = "demo-topic";
    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch latch;

    public ConsumerThread(CountDownLatch latch) {
        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        this.consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to the topic
        consumer.subscribe(Arrays.asList(topic));

        // Create latch
        this.latch = latch;
    }

    @Override
    public void run() {
        // Poll for the new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                records.forEach(record -> {
                    logger.info("-----> Partition: {}, Offset: {}, Key: {}, Value: {}",
                            record.partition(), record.offset(), record.key(), record.value());
                });

                logger.info("\n");
            }
        } catch (WakeupException ex) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        // Interrupt consumer, throws WakeUpException
        consumer.wakeup();
    }
}