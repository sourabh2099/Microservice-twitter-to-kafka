package com.microservices.demo.kafka.admin.client;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exceptions.KafkaClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class KafkaAdminClient {
    private final RetryTemplate retryTemplate;
    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final WebClient webClient;

    public KafkaAdminClient(RetryTemplate retryTemplate,
                            KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,
                            AdminClient adminClient,
                            WebClient webClient) {
        this.retryTemplate = retryTemplate;
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.webClient = webClient;
    }

    public void checkSchemeRegistry() {
        int retryCount = 1;
        int maxRetry = retryConfigData.getMaxAttempts();
        int multi = retryConfigData.getMultiplier().intValue();
        Long sleep = retryConfigData.getSleepTimeMs();
        while (checkSchemaRegistry().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleepTime(sleep);
            sleep = sleep * multi;
        }
    }

    private HttpStatusCode checkSchemaRegistry(){
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getGetSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached Maximum number of retry for creating a kafka topic", t);
        }
        checkTopicsCreated();
    }

    private void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Long sleepMs = retryConfigData.getSleepTimeMs();
        for (TopicListing topic : topics) {
            if (!isTopicCreated(topic.name(), topics)) {
                checkMaxRetry(retryCount++, retryConfigData.getMaxAttempts());
                sleepTime(sleepMs);
                sleepMs = sleepMs * retryConfigData.getMultiplier().intValue();
                topics = getTopics();
            }
        }
    }

    private void sleepTime(Long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (Throwable t) {
            throw new KafkaClientException("Error while sleeping in sleepTime Ms");
        }
    }

    private void checkMaxRetry(int retryAttemptNo, Integer maxAttempts) {
        if (retryAttemptNo > maxAttempts) {
            throw new KafkaClientException("Retry Limit exceeded for Topic creation");
        }
    }

    private boolean isTopicCreated(String topic, Collection<TopicListing> topics) {
        if (topics == null) return false;
        return topics.stream().anyMatch(e -> e.name().equals(topic));
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating topics with name {}", topicNames);
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(topic.trim()
                , kafkaConfigData.getNumberOfPartitions()
                , kafkaConfigData.getReplicationFactor())).toList();
        return adminClient.createTopics(kafkaTopics);
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Found error while trying to get Topic names", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Reading kafka Topicss {} attempt {}"
                , kafkaConfigData.getTopicNamesToCreate().toArray()
                , retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null) {
            topics.forEach(topic -> log.debug("Created topic with name {}", topic.name()));
        }
        return topics;
    }
}
