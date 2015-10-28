package com.dredgeplatform.dredge.scheduler;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Properties;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerServiceImpl implements Service, SchedulerService {
    private static final Logger log = LoggerFactory.getLogger(SchedulerServiceImpl.class);
    private static final long serialVersionUID = 1L;

    private Scheduler scheduler;
    public String clusterName;
    public String schedulerThreads;

    public SchedulerServiceImpl(String clusterName, String schedulerThreads) {
        this.clusterName = clusterName;
        this.schedulerThreads = schedulerThreads;
        log.debug("Cluster Name: {} with Threads: {}", clusterName, schedulerThreads);
    }

    @Override
    public void init(ServiceContext ctx) {
        log.debug("Scheduler Service Initialized. Service Name: {}", ctx.name());
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        log.debug("Scheduler Service Execution Started. Service Name: {}", ctx.name());
        startScheduler();
        log.debug("Scheduler Service Execution Completed. Service Name: {}", ctx.name());

    }

    @Override
    public void cancel(ServiceContext ctx) {
        try {
            stopScheduler();
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
        log.debug("Scheduler Service Cancelled. Service Name: {}", ctx.name());
    }

    @Override
    public void startScheduler() throws SchedulerException {
        log.debug("Starting Scheduler Server");

        final Properties props = new Properties();
        props.setProperty("org.quartz.scheduler.instanceName", "DredgeScheduler");
        props.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
        props.setProperty("org.quartz.threadPool.threadCount", schedulerThreads);

        final StdSchedulerFactory schedFact = new StdSchedulerFactory(props);
        scheduler = schedFact.getScheduler();
        scheduler.start();
        log.debug("Scheduler Started");
    }

    @Override
    public void stopScheduler() throws SchedulerException {
        log.warn("Stopping Schduler Server");
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
        log.warn("Scheduler Server Stopped.");
    }

    @Override
    public String getSchedulerStatus() {
        String status;
        try {
            if (scheduler.isShutdown()) {
                status = "Stopped";
            } else {
                status = "Started";
            }
        } catch (final Exception e) {
            log.warn("Scheduler Server is Stopped...");
            status = "Stopped";
        }
        return status;
    }

    @Override
    public void addJob() {

    }

    @Override
    public void updateJob() {

    }

    @Override
    public void deleteJob() {

    }

    @Override
    public void startJob() throws SchedulerException {
        log.debug("Starting job");
        // define the job and tie it to our HelloJob class
        final JobDetail job = newJob(HelloJob.class).withIdentity("job1", "group1").build();

        // Trigger the job to run now, and then repeat every 40 seconds
        final Trigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow().withSchedule(simpleSchedule().withIntervalInSeconds(5).repeatForever()).build();

        // Tell quartz to schedule the job using our trigger
        scheduler.scheduleJob(job, trigger);
    }

    @Override
    public void stopJob() throws SchedulerException {
        scheduler.deleteJob(jobKey("job1", "group1"));
    }

    @Override
    public void getJobList() {

    }

    @Override
    public void getCronJobList() {

    }

    @Override
    public void getCronEventJobList() {

    }

    @Override
    public void getEventOnlyJobList() {

    }

    @Override
    public void getEventJobList() {

    }

    @Override
    public void getDeamonJobList() {

    }

    @Override
    public void getJobStatus() {

    }

}
