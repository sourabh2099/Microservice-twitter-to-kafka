package com.microservice.demo.twitterToKafkaService;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("TwitterToKafkaServiceApplication starts at " + LocalDateTime.now());
    }
}
