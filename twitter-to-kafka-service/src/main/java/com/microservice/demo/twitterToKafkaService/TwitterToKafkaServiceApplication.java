package com.microservice.demo.twitterToKafkaService;

import com.microservice.demo.config.TwitterToKafkaServiceConfigData;
import com.microservice.demo.twitterToKafkaService.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.time.LocalDateTime;

@SpringBootApplication
@Slf4j
@ComponentScan("com.microservice.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData,StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = configData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("TwitterToKafkaServiceApplication starts at {}" , LocalDateTime.now());
        log.info("{}",twitterToKafkaServiceConfigData);
        streamRunner.start();
    }


}
