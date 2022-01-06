package com.bellomhd.kafka.consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

public class ConsumerErrorHandlingDeserializer<T> extends ErrorHandlingDeserializer<T> {

    private static final String HEADER_KEY_TOP_NAME = "deserializer-topic-name";


    public ConsumerErrorHandlingDeserializer(Deserializer<T> deserializer) {
        super(deserializer);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        prepareHeaderBeforeDeserialize(topic, headers, data);
        return super.deserialize(topic, headers, data);
    }

    private void prepareHeaderBeforeDeserialize(String topic, Headers headers, byte[] data) {
        headers.add(HEADER_KEY_TOP_NAME, topic.getBytes());
    }
}
