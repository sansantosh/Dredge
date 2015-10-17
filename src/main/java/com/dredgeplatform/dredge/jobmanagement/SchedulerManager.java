package com.dredgeplatform.dredge.jobmanagement;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;

public class SchedulerManager {
    final static Logger log = LoggerFactory.getLogger(SchedulerManager.class);

    public static void main(String[] args) throws NumberFormatException, IgniteException, Exception {
        if (args.length != 2) {
            log.error("ERROR: Invalid Arguments. Usage SchedulerManager ClusterName SchedulerThreads");
            System.exit(1);
        }

        try {
            log.debug("Cluster Name: {} with Threads: {}", args[0], args[1]);
            final String clusterName = args[0];
            final String SchedulerThreads = args[1];

            final Ignite ignite = ClusterManager.getIgnite();
            final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
            final IgniteServices svcs = ignite.services(remoteGroup);
            svcs.deployClusterSingleton("DredgeSchdulerServer", new SchedulerServiceImpl(clusterName, SchedulerThreads));
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static String startScheduler(String clusterName) throws Exception {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final SchedulerService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeSchdulerServer", SchedulerService.class, false);

        if (!schSrvc.getSchedulerStatus().equals("Started")) {
            schSrvc.startScheduler();
        }
        status = "Scheduler Server " + schSrvc.getSchedulerStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static String stopScheduler(String clusterName) throws Exception {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final SchedulerService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeSchdulerServer", SchedulerService.class, false);

        if (schSrvc.getSchedulerStatus().equals("Started")) {
            schSrvc.stopScheduler();
        }
        while (!schSrvc.getSchedulerStatus().equals("Stopped")) {
            log.warn(schSrvc.getSchedulerStatus());
            log.warn("Waiting for Scheduler to Stop...");
        }
        status = "Scheduler " + schSrvc.getSchedulerStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static String getSchedulerServerStatus(String clusterName) {
        String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final SchedulerService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeSchdulerServer", SchedulerService.class, false);

        status = "Scheduler " + schSrvc.getSchedulerStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static void startJob(String clusterName) throws SchedulerException {
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final SchedulerService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeSchdulerServer", SchedulerService.class, false);

        schSrvc.startJob();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
    }

    public static void stopJob(String clusterName) throws SchedulerException {
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final SchedulerService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeSchdulerServer", SchedulerService.class, false);

        schSrvc.stopJob();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
    }

}
