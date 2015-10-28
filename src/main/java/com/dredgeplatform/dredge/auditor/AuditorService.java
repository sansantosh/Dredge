package com.dredgeplatform.dredge.auditor;

import com.dredgeplatfrom.dredge.queuemanagement.KafkaUtils.MsgConsumer;

public interface AuditorService {
    void startAuditor(String loggerName, String brokerList);

    void stopAuditor(String loggerName);

    String getAuditorStatus(String loggerName);

    MsgConsumer startConsumer();

    void processConsumerData(String offset, String data);
}
