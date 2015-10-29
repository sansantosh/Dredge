package com.dredgeplatform.dredge.auditor;

public interface AuditorService {
    void startProducer(String loggerName, String brokerList);

    void stopProducer(String loggerName);

    String getProducerStatus(String loggerName);

    void startConsumer() throws Exception;

    void stopConsumer();

    String getConsumerStatus();

}
