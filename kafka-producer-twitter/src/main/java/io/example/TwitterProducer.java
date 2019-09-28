package io.example;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        String topicName = "twitter-tweets";

        // Create twitter client
        BlockingQueue<String> tweetQueue = new LinkedBlockingQueue<>(100000);
        Client twitterClient = createTwitterClient(tweetQueue);
        twitterClient.connect();

        // Create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            twitterClient.stop();
            producer.close();
        }));

        // Read tweets and send by the producer
        try {
            while (!twitterClient.isDone()) {
                String tweet = tweetQueue.poll(5, TimeUnit.SECONDS);

                if (tweet != null) {
                    logger.info(tweet);

                    producer.send(new ProducerRecord<>(topicName, tweet), (recordMetadata, ex) -> {
                        if (ex != null) {
                            logger.error("Kafka producer send failed", ex);
                        }
                    });
                }
            }
        } catch (Exception ex) {
            logger.error("Twitter client failed", ex);
            twitterClient.stop();
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        String consumerKey = System.getenv("CONSUMER_KEY");
        String consumerSecret = System.getenv("CONSUMER_SECRET");
        String accessToken = System.getenv("ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("ACCESS_TOKEN_SECRET");

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<Long> followings = newArrayList(1234L, 566788L);
        List<String> terms = newArrayList("kafka", "java", "node");
        endpoint.followings(followings);
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Yoh0xFF-Demo-Client-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "http://localhost:9092";

        Properties properties = new Properties();

        // Basic properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer (at the expense of a bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        return new KafkaProducer<>(properties);
    }
}
