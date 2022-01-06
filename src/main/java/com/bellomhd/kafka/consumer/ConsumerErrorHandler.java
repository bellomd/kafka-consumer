package com.bellomhd.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerErrorHandler implements ErrorHandler {

    private static final String HEADER_KEY_TOP_NAME = "deserializer-topic-name";

    @Override
    public void handle(Exception e, ConsumerRecord<?, ?> data) {
        final String topic = new String(data.headers().lastHeader(HEADER_KEY_TOP_NAME).value());
        log.error("An exception occurred while sending message to listener from topic: {}\n {}", topic, e.getMessage());
    }
}
