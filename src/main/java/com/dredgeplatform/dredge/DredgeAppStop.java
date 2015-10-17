package com.dredgeplatform.dredge;

import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;
import com.dredgeplatform.dredge.clustermanagement.WebserverManager;
import com.dredgeplatform.dredge.jobmanagement.SchedulerManager;
import com.dredgeplatform.dredge.lib.DredgeUtils;

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
        } else {
            propertiesPath = args[0];
        }

        final Properties props = DredgeUtils.readDredgeProperties(propertiesPath);
        for (final Entry<Object, Object> e : props.entrySet()) {
            log.debug("Dredge Property Key : {} - Value {}", e.getKey(), e.getValue());
        }

        try {
            System.out.println(WebserverManager.getWebServerStatus(props.get("webserverClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("webserverClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            System.out.println(WebserverManager.stopWebserver(props.get("webserverClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("webserverClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            System.out.println(SchedulerManager.getSchedulerServerStatus(props.get("schedulerClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("schedulerClusterName").toString(), e.getMessage(), e.getStackTrace());
        }

        try {
            System.out.println(SchedulerManager.stopScheduler(props.get("schedulerClusterName").toString()));
        } catch (final Exception e) {
            log.error("ERROR: Stopping Cluster:{}. Message: {} Trace: {}", props.get("schedulerClusterName").toString(), e.getMessage(), e.getStackTrace());
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
