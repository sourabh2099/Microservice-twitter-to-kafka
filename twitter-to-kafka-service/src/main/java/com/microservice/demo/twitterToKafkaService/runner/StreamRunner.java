package com.microservice.demo.twitterToKafkaService.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    public void start() throws TwitterException;
}
