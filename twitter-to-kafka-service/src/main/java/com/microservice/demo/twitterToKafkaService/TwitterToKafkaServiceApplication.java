package com.microservice.demo.twitterToKafkaService;

import com.microservice.demo.twitterToKafkaService.config.TwitterToKafkaServiceConfigData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;

@SpringBootApplication
@Slf4j
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData) {
        this.twitterToKafkaServiceConfigData = configData;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("TwitterToKafkaServiceApplication starts at {}" , LocalDateTime.now());
        log.info("{}",twitterToKafkaServiceConfigData);
    }


}
