package com.dredgeplatform.dredge.auditor;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.clustermanagement.ClusterManager;

public class AuditorConsumerServiceManager {
    final static Logger log = LoggerFactory.getLogger(AuditorConsumerServiceManager.class);

    public static void main(String[] args) {
        if (args.length != 2) {
            log.error("ERROR: Invalid Arguments. Usage AuditorConsumerServiceManager  clusterAddresses consumerDredgeKey");
            System.exit(1);
        }

        try {
            log.debug("Auditor Consumer clusterAddresses: {} consumerDredgeKey {}", args[0], args[1]);
            ClusterManager.clusterAddresses = args[0];
            final String consumerDredgeKey = args[1];

            final Ignite ignite = ClusterManager.getIgnite();
            final IgniteServices svcs = ignite.services();
            svcs.deployClusterSingleton("DredgeAuditorConsumer", new AuditorConsumerServiceimpl(consumerDredgeKey));
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
            startAuditorConsumer();
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static String startAuditorConsumer() throws Exception {
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorConsumerService schSrvc = ignite.services().serviceProxy("DredgeAuditorConsumer", AuditorConsumerService.class, false);

        schSrvc.startConsumer();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return "Started";
    }

    public static String stopAuditorConsumer() throws Exception {
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorConsumerService schSrvc = ignite.services().serviceProxy("DredgeAuditorConsumer", AuditorConsumerService.class, false);

        if (schSrvc.getConsumerStatus().equals("Started")) {
            schSrvc.stopConsumer();
        }
        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return "Stopped";
    }

    public static String geConsumerStatus() {
        final String status;
        final Ignite ignite = ClusterManager.getIgnite();
        final AuditorConsumerService schSrvc = ignite.services().serviceProxy("DredgeAuditorConsumer", AuditorConsumerService.class, false);

        status = schSrvc.getConsumerStatus();

        if (ignite.configuration().isClientMode()) {
            ignite.close();
        }
        return status;
    }

}
