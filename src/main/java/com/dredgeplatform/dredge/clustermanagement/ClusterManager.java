package com.dredgeplatform.dredge.clustermanagement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManager {
    final static Logger log = LoggerFactory.getLogger(ClusterManager.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            log.error("ERROR: Invalid Arguments. Usage StartCluster ClusterName");
            System.exit(1);
        }
        try {
            log.debug("Cluster Name: {}", args[0]);
            final String clusterName = args[0];
            final IgniteConfiguration cfg = new IgniteConfiguration();
            final Map<String, String> attrs = Collections.singletonMap("ROLE", clusterName);
            cfg.setUserAttributes(attrs);
            startNode(cfg);
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static void startCluster(int cntNodes, String clusterName) {
        log.debug("Cluster Initialize: {}", clusterName);
        for (int i = 0; i < cntNodes; i++) {
            try {
                log.debug("Starting Cluster: {} Node: {}", clusterName, i);
                final String separator = System.getProperty("file.separator");
                final String classpath = System.getProperty("java.class.path");
                final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

                final List<String> command = new ArrayList<String>();
                command.add(path);
                command.add("-cp");
                command.add(classpath);
                command.add(ClusterManager.class.getName());
                command.add(clusterName);
                log.debug("Starting Cluster: {} Node: {} Command: {}", clusterName, i, command.toString());
                new ProcessBuilder(command).start();
                log.debug("Starting Cluster: {} Node: {} Process Started.", clusterName, i);
            } catch (final Exception e) {
                log.error("ERROR: Cluster: {} Node: {} Trace: {}", clusterName, i, e.getStackTrace());
            }
        }

    }

    public static void stopCluster(String clusterName) {
        try {
            log.debug("Stoping Cluster: {} ", clusterName);
            final Ignite ignite = getIgnite();
            final IgniteCompute compute = ignite.compute(ignite.cluster().forAttribute("ROLE", clusterName));
            compute.broadcast(new ClusterManager.SendStopClusterMessage());
            log.debug("Stoping Cluster: {} Message Brodcasted.", clusterName);
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
        } catch (final Exception e) {
            log.error("ERROR: Cluster {}  Trace: {}", clusterName, e.getStackTrace());
        }
    }

    private static Ignite startNode(IgniteConfiguration cfg) {
        final TcpDiscoverySpi spi = new TcpDiscoverySpi();
        final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(spi);
        return Ignition.start(cfg);
    }

    public static Ignite getIgnite() {
        if (Ignition.state() == IgniteState.STOPPED) {
            final IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setClientMode(true);
            return startNode(cfg);
        } else {
            return Ignition.ignite();
        }
    }

    public static void startWebserver(String clusterName, String port) throws Exception {
        log.debug("Starting WebServer on Cluster: {} at Port: {}", clusterName, port);
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

        final List<String> command = new ArrayList<String>();
        command.add(path);
        command.add("-cp");
        command.add(classpath);
        command.add(WebserverManager.class.getName());
        command.add(clusterName);
        command.add(port);
        log.debug("Starting WebServer on Cluster: {} at Port: {} Command: {}", clusterName, port, command.toString());
        new ProcessBuilder(command).start();
        log.debug("Starting WebServer on Cluster: {} at Port: {} Process Started.", clusterName, port);
    }

    public static class SendStopClusterMessage implements IgniteRunnable {
        private static final long serialVersionUID = 1L;
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public void run() {
            new Thread() {
                @Override
                public void run() {
                    ignite.close();
                }
            }.start();
        }
    }

}
