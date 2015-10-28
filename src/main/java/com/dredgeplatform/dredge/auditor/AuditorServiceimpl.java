package com.dredgeplatform.dredge.auditor;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatfrom.dredge.queuemanagement.KafkaProducerLogger;
import com.dredgeplatfrom.dredge.queuemanagement.KafkaUtils.MsgConsumer;

public class AuditorServiceimpl implements Service, AuditorService {
    private static final long serialVersionUID = 1L;
    final static Logger log = LoggerFactory.getLogger(AuditorServiceimpl.class);

    String loggerName;
    String brokerList;

    public AuditorServiceimpl(String loggerName, String brokerList) {
        this.loggerName = loggerName;
        this.brokerList = brokerList;
        log.debug("Auditor LoggerName: {} BrokerList: {}", loggerName, brokerList);
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        log.debug("Auditor Service Initialized. Service Name: {}", ctx.name());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        log.debug("Audtior Service Execution Started. Service Name: {}", ctx.name());
        startAuditor(loggerName, brokerList);
        log.debug("Audtior Service Execution Completed. Service Name: {}", ctx.name());
    }

    @Override
    public void cancel(ServiceContext ctx) {
        stopAuditor(loggerName);
        log.debug("Auditor Service Cancelled. Service Name: {}", ctx.name());
    }

    @Override
    public void startAuditor(String loggerName, String brokerList) {
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
    public void stopAuditor(String loggerName) {
        org.apache.log4j.Logger.getLogger(loggerName).getAppender(loggerName).close();
        org.apache.log4j.Logger.getLogger(loggerName).removeAppender(loggerName);
    }

    @Override
    public String getAuditorStatus(String loggerName) {
        String status = null;
        if (org.apache.log4j.Logger.getLogger(loggerName).getAppender(loggerName) == null) {
            status = "Stopped";
        } else {
            status = "Started";
        }
        return status;
    }

    @Override
    public MsgConsumer startConsumer() {
        return null;
        // return new KafkaUtils().new MsgConsumer(this, "consumerDredgeKey");
    }

    @Override
    public void processConsumerData(String offset, String data) {
        System.out.println(offset + ":" + data);
    }

}
