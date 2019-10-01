package io.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.groupId}")
    public void listen1(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("[x] Listener 1, Partition " + partition + ", Received '" + message + "'");
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.groupId}")
    public void listen2(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("[x] Listener 2, Partition " + partition + ", Received '" + message + "'");
    }
}
