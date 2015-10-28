package com.dredgeplatfrom.dredge.queuemanagement;

import com.dredgeplatfrom.dredge.queuemanagement.KafkaUtils.MsgConsumer;

public interface ConsumerService {
    MsgConsumer startConsumer() throws Exception;

    void processConsumerData(String offset, String data);
}
