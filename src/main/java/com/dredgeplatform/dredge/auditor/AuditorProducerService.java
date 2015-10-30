package com.dredgeplatform.dredge.auditor;

public interface AuditorProducerService {
    void startProducer(String loggerName, String brokerList);

    void stopProducer(String loggerName);

    String getProducerStatus(String loggerName);

}
