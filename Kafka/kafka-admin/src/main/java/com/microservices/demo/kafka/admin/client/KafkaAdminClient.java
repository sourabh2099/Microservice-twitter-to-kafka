package com.microservices.demo.kafka.admin.client;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.config.RetryConfigData;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaAdminClient {
    private final RetryTemplate retryTemplate;
    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;

    public KafkaAdminClient(RetryTemplate retryTemplate,
                            KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,
                            AdminClient adminClient) {
        this.retryTemplate = retryTemplate;
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
    }

//    public void createTopics(){
//        CreateTopicsResult createTopicsResult;
//        createTopicsResult = retryTemplate.execute(this::doCreateTopics);
//    }

//    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
//
//    }
}
