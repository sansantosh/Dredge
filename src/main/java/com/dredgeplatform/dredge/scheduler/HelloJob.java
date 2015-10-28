package com.dredgeplatform.dredge.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloJob implements Job {
    final static Logger log = LoggerFactory.getLogger(HelloJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            log.debug("Hello");
            final org.apache.log4j.Logger l = org.apache.log4j.Logger.getLogger("auditor");
            log.info("Name of auditor :- " + l.getAppender("auditor"));
            l.info("test");
        } catch (final Exception e) {
            log.info("Hello Error === " + e.getMessage());
        }
    }

}
