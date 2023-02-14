package com.microservice.demo.twitterToKafkaService.runner.impl;

import com.microservice.demo.config.TwitterToKafkaServiceConfigData;
import com.microservice.demo.twitterToKafkaService.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitterToKafkaService.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(value = "twitter-to-kafka-service.enable-mock-kafka-stream-Runner", havingValue = "true")
@Slf4j
public class MockKafkaStreamRunner implements StreamRunner {
    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private TwitterKafkaStatusListener twitterKafkaStatusListener;

    private Random RANDOM = new Random();
    public static final String[] WORDS = new String[]{
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "Cras id magna facilisis",
            "dapibus ligula porttitor",
            "condimentum mi", "Etiam a sollicitudin urna",
            "In non sapien",
            "et lectus placerat porttitor Donec sagittis",
            "ex sit amet dignissim laoreet",
            "neque dolor gravida erat",
            "non tempus nibh sem vel dui",
            "Donec molestie volutpat leo eget bibendum",
            "Praesent posuere tellus ac nulla sollicitudin ultrices"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        log.info("Starting mock filtering twitter streams for keywords {}", keywords);
        simulateTwitterStream(keywords, minLength, maxLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minLength, int maxLength, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                String formatterTweetAsRawString = getFormattedTweet(keywords, minLength, maxLength);
                Status status = TwitterObjectFactory.createStatus(formatterTweetAsRawString);
                twitterKafkaStatusListener.onStatus(status);
                sleep(sleepTimeMs);
            }
        });

    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while sleeping for thread");
        }
    }

    private String getFormattedTweet(String[] keywords, int minLength, int maxLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(minLength)),
                getRandomTweetContent(keywords, minLength, maxLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(maxLength))
        };
        return formatTweetAsJsonWithParams(params);
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minLength, int maxLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
