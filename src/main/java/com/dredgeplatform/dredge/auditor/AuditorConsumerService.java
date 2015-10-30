package com.dredgeplatform.dredge.auditor;

public interface AuditorConsumerService {

    void startConsumer() throws Exception;

    void stopConsumer();

    String getConsumerStatus();

}
