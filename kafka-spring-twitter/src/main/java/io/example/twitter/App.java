package io.example.twitter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ComponentScan(basePackages = "io.example.twitter.*")
@EnableKafka
@EnableScheduling
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
