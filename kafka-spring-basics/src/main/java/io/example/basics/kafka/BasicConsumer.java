package io.example.basics.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class BasicConsumer {

    private final Logger logger = LoggerFactory.getLogger(BasicConsumer.class);

    @KafkaListener(topics = "${kafka.topic}",
            groupId = "${kafka.groupId}")
    public void listen1(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        logger.info("[x] Listener 1, Partition {}, Received {}", partition, message);
    }

    @KafkaListener(topics = "${kafka.topic}",
            groupId = "${kafka.groupId}")
    public void listen2(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        logger.info("[x] Listener 2, Partition {}, Received {}", partition, message);
    }
}
