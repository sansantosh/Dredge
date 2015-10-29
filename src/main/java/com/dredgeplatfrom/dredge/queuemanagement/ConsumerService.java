package com.dredgeplatfrom.dredge.queuemanagement;

public interface ConsumerService {
    void processConsumerData(String offset, String data);
}
