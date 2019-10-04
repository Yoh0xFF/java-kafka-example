package io.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {
        new ElasticSearchConsumer().run();
    }

    private void run() {
        String topicName = "twitter-tweets";

        // Create elastic search client
        RestHighLevelClient client = createClient();

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        // Subscribe consumer to the topic
        consumer.subscribe(Arrays.asList(topicName));

        // Poll for the new data
        try {
            while (true) {
                logger.info("Reading records...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                logger.info("Processing records...");

                if (records.isEmpty()) {
                    continue;
                }

                BulkRequest bulkRequest = new BulkRequest();

                records.forEach(record -> {
                    String id = String.format("%s_%s_%s", record.topic(), record.partition(), record.offset());

                    IndexRequest indexRequest = new IndexRequest("twitter");
                    indexRequest.id(id).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                });

                try {
                    BulkResponse indexResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Document index ids {}",
                            Arrays.asList(indexResponse.getItems())
                                    .stream()
                                    .map(bulkItemResponse -> bulkItemResponse.getId())
                                    .reduce((id1, id2) -> String.format("%s, %s", id1, id2))
                                    .get());
                } catch (IOException ex) {
                    logger.error("Document index failed", ex);
                }

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed\n");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    logger.error("Thread interrupted", ex);
                }
            }
        } finally {
            try {
                client.close();
                consumer.close();
            } catch (IOException ex) {
                logger.error("Client close operation failed", ex);
            }
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers = "https://localhost:9092";
        String clientGroup = "kafka-consumer-elasticsearch-group";

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clientGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        // Create consumer
        return new KafkaConsumer<>(properties);
    }

    private RestHighLevelClient createClient() {
        String host = System.getenv("ES_HOST");
        String user = System.getenv("ES_USER");
        String pass = System.getenv("ES_PASS");

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pass));

        RestClientBuilder restClientBuilder = RestClient
                .builder(new HttpHost(host, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }
}
