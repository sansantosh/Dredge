package com.dredgeplatform.dredge.auditor;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;

public class AuditorServiceManager {
    final static Logger log = LoggerFactory.getLogger(AuditorServiceManager.class);

    public static void main(String[] args) throws NumberFormatException, IgniteException, Exception {
        if (args.length != 3) {
            log.error("ERROR: Invalid Arguments. Usage AuditorManager LoggerName BrokerList clusterAddresses");
            System.exit(1);
        }

        try {
            log.debug("Auditor LoggerName: {} BrokerList: {} clusterAddresses: {}", args[0], args[1], args[2]);
            final String loggerName = args[0];
            final String brokerList = args[1];
            ClusterManager.clusterAddresses = args[2];

            final Ignite ignite = ClusterManager.getIgnite();
            final IgniteServices svcs = ignite.services();
            svcs.deployClusterSingleton("DredgeAuditor", new AuditorServiceimpl(loggerName, brokerList));
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static String startAuditor(String loggerName, String brokerList) throws Exception {
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorService schSrvc = ignite.services().serviceProxy("DredgeAuditor", AuditorService.class, false);

        schSrvc.startProducer(loggerName, brokerList);
        schSrvc.startConsumer();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return "Started";
    }

    public static String stopAuditor(String loggerName) throws Exception {
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorService schSrvc = ignite.services().serviceProxy("DredgeAuditor", AuditorService.class, false);

        if (schSrvc.getProducerStatus(loggerName).equals("Started")) {
            schSrvc.stopProducer(loggerName);
        }

        if (schSrvc.getConsumerStatus().equals("Started")) {
            schSrvc.stopConsumer();
        }
        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return "Stopped";
    }

    public static String getProducerStatus(String loggerName) {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorService schSrvc = ignite.services().serviceProxy("DredgeAuditor", AuditorService.class, false);

        status = schSrvc.getProducerStatus(loggerName);

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

    public static String geConsumerStatus(String loggerName) {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorService schSrvc = ignite.services().serviceProxy("DredgeAuditor", AuditorService.class, false);

        status = schSrvc.getConsumerStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

}
