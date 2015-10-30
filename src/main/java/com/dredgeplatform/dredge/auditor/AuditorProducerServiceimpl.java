package com.dredgeplatform.dredge.auditor;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatfrom.dredge.queuemanagement.KafkaProducerLogger;

public class AuditorProducerServiceimpl implements Service, AuditorProducerService {
    private static final long serialVersionUID = -113810467528455321L;
    final static Logger log = LoggerFactory.getLogger(AuditorProducerServiceimpl.class);

    String loggerName;
    String brokerList;

    public AuditorProducerServiceimpl(String loggerName, String brokerList) {
        this.loggerName = loggerName;
        this.brokerList = brokerList;
        log.debug("AuditorProducerServiceimpl LoggerName: {} BrokerList: {}", loggerName, brokerList);
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        log.debug("AuditorProducerServiceimpl Service Initialized. Service Name: {}", ctx.name());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        log.debug("AuditorProducerServiceimpl Service Execution Started. Service Name: {}", ctx.name());
        startProducer(loggerName, brokerList);
        log.debug("AuditorProducerServiceimpl Service Execution Completed. Service Name: {}", ctx.name());
    }

    @Override
    public void cancel(ServiceContext ctx) {
        log.debug("AuditorProducerServiceimpl Service Cancel Started. Service Name: {}", ctx.name());
        stopProducer(loggerName);
        log.debug("AuditorProducerServiceimpl Service Cancel Completed. Service Name: {}", ctx.name());
    }

    @Override
    public void startProducer(String loggerName, String brokerList) {
        final KafkaProducerLogger auditorLog = new KafkaProducerLogger();
        auditorLog.setName(loggerName);
        auditorLog.setThreshold(Level.INFO);
        auditorLog.setBrokerList(brokerList);
        auditorLog.setSyncSend(true);
        auditorLog.setTopic(loggerName);
        auditorLog.activateOptions();
        org.apache.log4j.Logger.getLogger(loggerName).addAppender(auditorLog);
        org.apache.log4j.Logger.getLogger(loggerName).setAdditivity(false);
    }

    @Override
    public void stopProducer(String loggerName) {
        log.debug("Stop Producer Called. Name: {}", org.apache.log4j.Logger.getLogger(loggerName).getName());
        org.apache.log4j.Logger.getLogger(loggerName).getAppender(loggerName).close();
        org.apache.log4j.Logger.getLogger(loggerName).removeAppender(loggerName);
        log.debug("Stop Producer Complete.");
    }

    @Override
    public String getProducerStatus(String loggerName) {
        String status = null;
        if (org.apache.log4j.Logger.getLogger(loggerName).getAppender(loggerName) == null) {
            status = "Stopped";
        } else {
            status = "Started";
        }
        return status;
    }

}
