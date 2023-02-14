package com.microservices.demo.config;

import com.microservice.demo.config.RetryConfigData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {
    private final RetryConfigData retryConfigData;

    public RetryConfig(RetryConfigData retryConfigData) {
        this.retryConfigData = retryConfigData;
    }

    @Bean
    public RetryTemplate retryTemplate(){
        RetryTemplate retryTemplate = new RetryTemplate();
        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialRandomBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(retryConfigData.getInitialInterval());
        exponentialBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());
        exponentialBackOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
        exponentialBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());

        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(retryConfigData.getMaxAttempts());

        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }
}
