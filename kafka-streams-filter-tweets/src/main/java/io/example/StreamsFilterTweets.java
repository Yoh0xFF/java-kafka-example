package io.example;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    private JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        new StreamsFilterTweets().run();
    }

    public void run() {
        String bootstrapServers = "https://localhost:9092";
        String applicationId = "demo-kafka-streams";
        String inputTopicName = "twitter-tweets";
        String outputTopicName = "twitter-important-tweets";

        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> input = streamsBuilder.stream(inputTopicName);
        KStream<String, String> filtered = input.filter((k, tweet) -> extractFollowersCount(tweet) > 10000);
        filtered.to(outputTopicName);

        // Build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start our streams application
        kafkaStreams.start();
    }

    private int extractFollowersCount(String tweet) {
        try {
            return jsonParser
                    .parse(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception ex) {
            return 0;
        }
    }
}
