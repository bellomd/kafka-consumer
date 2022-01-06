package com.bellomhd.kafka.service;

import com.bellomhd.kafka.consumer.MessageVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageConsumerServiceImpl implements ConsumerService {

    @Override
    @KafkaListener(topics = {"kafka-series-1"}, containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, MessageVo> consumerRecord, @Payload MessageVo messageVo) {
        log.info("Received event with topic: {}, partition: {}, payload: {}", consumerRecord.topic(), consumerRecord.partition(), messageVo);
    }
}
