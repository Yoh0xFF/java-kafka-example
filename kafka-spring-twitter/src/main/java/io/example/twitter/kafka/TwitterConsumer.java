package io.example.twitter.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

@Component
public class TwitterConsumer {

    private final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

    @Autowired
    private RestHighLevelClient elasticSearchClient;

    @KafkaListener(topics = "${kafka.topic}",
            groupId = "${kafka.groupId}")
    public void listen(Consumer<String, String> consumer, ConsumerRecords<String, String> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();

        records.forEach(record -> {
            String id = String.format("%s_%s_%s", record.topic(), record.partition(), record.offset());

            IndexRequest indexRequest = new IndexRequest("twitter");
            indexRequest.id(id).source(record.value(), XContentType.JSON);
            bulkRequest.add(indexRequest);
        });

        BulkResponse indexResponse = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        logger.info("[x] Document index ids {}",
                Arrays.asList(indexResponse.getItems())
                        .stream()
                        .map(bulkItemResponse -> bulkItemResponse.getId())
                        .reduce((id1, id2) -> String.format("%s, %s", id1, id2))
                        .get());

        consumer.commitSync();
    }
}
