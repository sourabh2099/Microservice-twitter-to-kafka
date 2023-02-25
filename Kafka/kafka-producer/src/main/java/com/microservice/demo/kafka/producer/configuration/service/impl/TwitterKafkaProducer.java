package com.microservice.demo.kafka.producer.configuration.service.impl;

import com.microservice.demo.kafka.producer.configuration.service.KafkaProducer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {
    private KafkaTemplate kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    /*
    Listenable future -> Register CAllback methods for handling events when response is returned
    Completeable Future is introduced in java 8 and with its introduction the Listenable future is now deprecated
    * */

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture
                = kafkaTemplate.send(topicName, key, message);
        addCallBack(kafkaResultFuture);
    }

    private static void addCallBack(CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture
                .whenComplete(((longTwitterAvroModelSendResult, throwable) -> {
                    RecordMetadata metadata = longTwitterAvroModelSendResult.getRecordMetadata();
                }))
                .exceptionally(throwable -> {
                    System.out.println(Arrays.toString(throwable.getStackTrace()));
                    return null;
                })
                .thenAcceptAsync(longTwitterAvroModelSendResult -> {
                    System.out.println("Producer data this is the final execution block of code" + longTwitterAvroModelSendResult.getProducerRecord());
                });
    }
}
