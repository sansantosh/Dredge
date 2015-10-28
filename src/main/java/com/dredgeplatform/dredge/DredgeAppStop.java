package com.dredgeplatform.dredge;

import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.auditor.AuditorServiceManager;
import com.dredgeplatform.dredge.clustermanagement.ClusterManager;
import com.dredgeplatform.dredge.lib.DredgeUtils;
import com.dredgeplatform.dredge.scheduler.SchedulerServiceManager;
import com.dredgeplatform.dredge.webserver.WebserverServiceManager;

public class DredgeAppStop {
    final static Logger log = LoggerFactory.getLogger(DredgeAppStop.class);
    static String propertiesPath;

    public static void main(final String[] args) throws Exception {
        log.info("______   ______  ______ ______  ______   ______");
        log.info("|     \\ |_____/ |______ |     \\ |  ____ |______");
        log.info("|_____/ |    \\_ |______ |_____/ |_____| |______");
        log.info("Stopping Dredge Server...");

        if (args.length != 1) {
            propertiesPath = "./src/main/resources/dredge.properties";
            log.info("dredge.properties file not provided, using default properties...");
        } else {
            propertiesPath = args[0];
        }

        final Properties props = DredgeUtils.readDredgeProperties(propertiesPath);
        for (final Entry<Object, Object> e : props.entrySet()) {
            log.debug("Dredge Property Key : {} - Value {}", e.getKey(), e.getValue());
        }

        ClusterManager.clusterAddresses = props.get("clusterAddresses").toString();

        try {
            System.out.println(WebserverServiceManager.stopWebserver(props.get("webserverClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Webserver:{}. Message: {} Trace: {}", props.get("webserverClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            System.out.println(SchedulerServiceManager.stopScheduler(props.get("schedulerClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Scheduler:{}. Message: {} Trace: {}", props.get("schedulerClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            AuditorServiceManager.stopAuditor(props.get("auditorTopicName").toString());
        } catch (final Exception e) {
            log.error("ERROR: Stopping Auditor:{}. Message: {} Trace: {}", props.get("auditorTopicName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            ClusterManager.stopCluster(props.get("computeClusterName").toString());
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("computeClusterName").toString(), e.getMessage(), e.getStackTrace());
        }
        try {
            ClusterManager.stopCluster(props.get("schedulerClusterName").toString());
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("schedulerClusterName").toString(), e.getMessage(), e.getStackTrace());
        }
        try {
            ClusterManager.stopCluster(props.get("webserverClusterName").toString());
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("webserverClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

    }

}
