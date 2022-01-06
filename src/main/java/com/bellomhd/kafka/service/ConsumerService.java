package com.bellomhd.kafka.service;

import com.bellomhd.kafka.consumer.MessageVo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.Message;

public interface ConsumerService {

    void consume(ConsumerRecord<String, MessageVo> consumerRecord, MessageVo messageVo);
}
