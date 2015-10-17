package com.dredgeplatform.dredge;

import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;
import com.dredgeplatform.dredge.lib.DredgeUtils;

public class DredgeAppStart {
    final static Logger log = LoggerFactory.getLogger(DredgeAppStart.class);
    static String propertiesPath;

    public static void main(final String[] args) throws Exception {
        if (args.length != 1) {
            propertiesPath = "./src/main/resources/dredge.properties";
        } else {
            propertiesPath = args[0];
        }

        log.info("______   ______  ______ ______  ______   ______");
        log.info("|     \\ |_____/ |______ |     \\ |  ____ |______");
        log.info("|_____/ |    \\_ |______ |_____/ |_____| |______");

        log.info("Starting Dredge Server...");

        // Read Dredge Properties
        final Properties props = DredgeUtils.readDredgeProperties(propertiesPath);
        for (final Entry<Object, Object> e : props.entrySet()) {
            log.debug("Dredge Property Key : {} - Value {}", e.getKey(), e.getValue());
        }

        // Start Dredge as a Cluster App.
        // Compute Cluster for Tasks Processing
        // Scheduler Cluster for Distributed Quatz Instance
        // WebServer Cluster for Dustributed Jetty Instance
        log.info("Starting Compute Cluster: {} Nodes: {}", props.get("computeClusterName").toString(), props.get("computeClusterNodes").toString());
        ClusterManager.startCluster(Integer.parseInt(props.get("computeClusterNodes").toString()), props.get("computeClusterName").toString());

        log.info("Starting Scheduler Cluster: {} Nodes: {}", props.get("schedulerClusterName").toString(), props.get("schedulerClusterNodes").toString());
        ClusterManager.startCluster(Integer.parseInt(props.get("schedulerClusterNodes").toString()), props.get("schedulerClusterName").toString());

        log.info("Starting Webserver Cluster: {} Nodes: {}", props.get("webserverClusterName").toString(), props.get("webserverClusterNodes").toString());
        ClusterManager.startCluster(Integer.parseInt(props.get("webserverClusterNodes").toString()), props.get("webserverClusterName").toString());

        // status = ClusterManager.startCluster(1, "test");
        // log.debug(status + "=================================");

        // Start Jetty Server and add instance to Webserver Cluster.
        log.info("Starting Web Server...");
        try {
            ClusterManager.startWebserver(props.get("webserverClusterName").toString(), props.get("jettyPort").toString());
        } catch (final Exception e) {
            log.error("ERROR: Starting WebServer. Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
        log.info("Web Server Started");

        log.info("Dredge Startup Completed...");
        log.info("Dredge Available @ http://localhost:" + props.get("jettyPort").toString() + "/dredge");

    }
}
