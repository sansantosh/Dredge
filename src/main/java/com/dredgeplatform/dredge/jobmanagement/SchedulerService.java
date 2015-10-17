package com.dredgeplatform.dredge.jobmanagement;

import org.quartz.SchedulerException;

public interface SchedulerService {

    void startScheduler() throws SchedulerException;

    void stopScheduler() throws SchedulerException;

    void addJob();

    void updateJob();

    void deleteJob();

    void startJob() throws SchedulerException;

    void stopJob() throws SchedulerException;

    // Get Methods
    String getSchedulerStatus();

    void getJobList();

    void getCronJobList();

    void getCronEventJobList();

    void getEventOnlyJobList();

    void getEventJobList();

    void getDeamonJobList();

    void getJobStatus();

}
