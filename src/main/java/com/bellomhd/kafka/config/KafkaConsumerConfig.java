package com.bellomhd.kafka.config;

import com.bellomhd.kafka.consumer.ConsumerErrorHandler;
import com.bellomhd.kafka.consumer.ConsumerErrorHandlingDeserializer;
import com.bellomhd.kafka.consumer.MessageVo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.message.consumer.group}")
    private String consumerGroup;

    @Value("${kafka.consumer.concurrency:1}")
    private Integer consumerConcurrency;

    private final ConsumerErrorHandler consumerErrorHandler;

    public KafkaConsumerConfig(ConsumerErrorHandler consumerErrorHandler) {
        this.consumerErrorHandler = consumerErrorHandler;
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, MessageVo>> kafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, MessageVo> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(consumerErrorHandler);
        factory.setConcurrency(consumerConcurrency);
        return factory;
    }

    private ConsumerFactory<String, MessageVo> consumerFactory() {
        try (JsonDeserializer<MessageVo> jsonDeserializer = new JsonDeserializer<>(MessageVo.class, false)) {
            jsonDeserializer.addTrustedPackages("*");
            try (StringDeserializer stringDeserializer = new StringDeserializer()) {
                return new DefaultKafkaConsumerFactory<>(consumerConfig(consumerGroup, JsonDeserializer.class), stringDeserializer, new ConsumerErrorHandlingDeserializer<>(jsonDeserializer));
            }
        }
    }

    private Map<String, Object> consumerConfig(final String consumerGroup, final Class<?> valueDeserializerClass) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        return properties;
    }
}
