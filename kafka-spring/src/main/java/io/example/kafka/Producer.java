package io.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.apache.commons.lang3.RandomUtils.nextInt;

@Component
public class Producer {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 10000, initialDelay = 5000)
    public void send() {
        for (int i = 0; i < 10; ++i) {
            String message = random(nextInt(1, 25), true, true);

            kafkaTemplate.send(topic, message);

            System.out.println("[x] Sent '" + message + "'");
        }
    }
}
