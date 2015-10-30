package com.dredgeplatform.dredge.auditor;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatfrom.dredge.queuemanagement.ConsumerService;
import com.dredgeplatfrom.dredge.queuemanagement.KafkaConsumerimpl;

public class AuditorConsumerServiceimpl implements Service, AuditorConsumerService, ConsumerService {
    private static final long serialVersionUID = 4757847444882296733L;

    final static Logger log = LoggerFactory.getLogger(AuditorConsumerServiceimpl.class);

    private KafkaConsumerimpl AuditorConsumer;
    private final String DredgeConsumerKey;
    private boolean startedOnce = false;

    public AuditorConsumerServiceimpl(String DredgeConsumerKey) {
        this.DredgeConsumerKey = DredgeConsumerKey;
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        log.debug("AuditorConsumerServiceimpl Service Initialized. Service Name: {}", ctx.name());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        log.debug("AuditorConsumerServiceimpl Service Execution Started. Service Name: {}", ctx.name());
        AuditorConsumer = new KafkaConsumerimpl(this, DredgeConsumerKey);
        log.debug("AuditorConsumerServiceimpl Service Execution Started. startedOnce: {}", startedOnce);
        if (startedOnce) {
            startConsumer();
        } else {
            startedOnce = true;
        }
        log.debug("AuditorConsumerServiceimpl Service Execution Started. startedOnce: {}", startedOnce);
        log.debug("AuditorConsumerServiceimpl Service Execution Completed. Service Name: {}", ctx.name());
    }

    @Override
    public void cancel(ServiceContext ctx) {
        log.debug("AuditorConsumerServiceimpl Service Cancel Started. Service Name: {}", ctx.name());
        stopConsumer();
        log.debug("AuditorConsumerServiceimpl Service Cancel Completed. Service Name: {}", ctx.name());
    }

    @Override
    public void startConsumer() throws Exception {
        AuditorConsumer.startConsumer();
    }

    @Override
    public void stopConsumer() {
        AuditorConsumer.stopConsumer();
    }

    @Override
    public void processConsumerData(String offset, String data) {
        log.info("AuditorConsumer: " + offset + ":" + data);
    }

    @Override
    public String getConsumerStatus() {
        try {
            return AuditorConsumer.getConsumerStatus();
        } catch (final Exception e) {
            log.info("ERROR : {} {} ", e.getStackTrace(), e.getMessage());
            return String.format("ERROR :  Messag: %s Trace: %s", e.getMessage(), e.getStackTrace());
        }
    }

}
