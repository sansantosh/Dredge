package com.dredgeplatform.dredge.clustermanagement;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebserverManager {
    final static Logger log = LoggerFactory.getLogger(WebserverManager.class);

    public static void main(String[] args) throws NumberFormatException, IgniteException, Exception {
        if (args.length != 2) {
            log.error("ERROR: Invalid Arguments. Usage WebserverManager ClusterName port");
            System.exit(1);
        }

        try {
            log.debug("Cluster Name: {} Port: {}", args[0], args[1]);
            final String clusterName = args[0];
            final int port = Integer.parseInt(args[1]);

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
        status = "Webserver " + schSrvc.getWebserverStatus();

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
        status = "Webserver " + schSrvc.getWebserverStatus();

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

        status = "Webserver " + schSrvc.getWebserverStatus() + " " + ignite.configuration().isClientMode();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

}
