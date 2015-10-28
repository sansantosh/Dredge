package com.dredgeplatform.dredge.webserver;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;

public class WebserverServiceManager {
    final static Logger log = LoggerFactory.getLogger(WebserverServiceManager.class);

    public static void main(String[] args) throws NumberFormatException, IgniteException, Exception {
        if (args.length != 3) {
            log.error("ERROR: Invalid Arguments. Usage WebserverManager ClusterName port clusterAddresses");
            System.exit(1);
        }

        try {
            log.debug("Cluster Name: {} Port: {} clusterAddresses: {}", args[0], args[1], args[2]);
            final String clusterName = args[0];
            final int port = Integer.parseInt(args[1]);
            ClusterManager.clusterAddresses = args[2];

            final Ignite ignite = ClusterManager.getIgnite();
            final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
            final IgniteServices svcs = ignite.services(remoteGroup);
            svcs.deployClusterSingleton("DredgeWebserver", new WebserverServiceimpl(clusterName, port));
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static String startWebserver(String clusterName) throws Exception {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final WebserverService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeWebserver", WebserverService.class, false);

        if (!schSrvc.getWebserverStatus().equals("STARTED")) {
            schSrvc.startWebserver();
        }
        status = schSrvc.getWebserverStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static String stopWebserver(String clusterName) throws Exception {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final WebserverService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeWebserver", WebserverService.class, false);

        if (schSrvc.getWebserverStatus().equals("STARTED")) {
            schSrvc.stopWebserver();
        }
        while (!schSrvc.getWebserverStatus().equals("STOPPED")) {
            log.warn("Waiting for Webserver to Stop...");
        }
        status = schSrvc.getWebserverStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static String getWebServerStatus(String clusterName) {
        String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
        final WebserverService schSrvc = ignite.services(remoteGroup).serviceProxy("DredgeWebserver", WebserverService.class, false);

        status = schSrvc.getWebserverStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

}
