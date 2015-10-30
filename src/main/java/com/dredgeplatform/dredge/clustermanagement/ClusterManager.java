package com.dredgeplatform.dredge.clustermanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dredgeplatform.dredge.auditor.AuditorConsumerServiceManager;
import com.dredgeplatform.dredge.auditor.AuditorProducerServiceManager;
import com.dredgeplatform.dredge.scheduler.SchedulerServiceManager;
import com.dredgeplatform.dredge.webserver.WebserverServiceManager;

public class ClusterManager {
    final static Logger log = LoggerFactory.getLogger(ClusterManager.class);
    public static String clusterAddresses;

    public static void main(String[] args) {
        if (args.length != 2) {
            log.error("ERROR: Invalid Arguments. Usage ClusterManager ClusterName clusterAddresses");
            System.exit(1);
        }
        try {
            log.debug("Cluster Name: {} ", args[0]);
            final String clusterName = args[0];
            clusterAddresses = args[1];
            final IgniteConfiguration cfg = new IgniteConfiguration();
            final Map<String, String> attrs = Collections.singletonMap("ROLE", clusterName);
            cfg.setUserAttributes(attrs);
            startNode(cfg);
        } catch (final Exception e) {
            log.error("ERROR: Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    public static String startCluster(int cntNodes, String clusterName) {
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
                command.add(clusterAddresses);
                log.debug("Starting Cluster: {} Node: {} Command: {}", clusterName, i, command.toString());
                new ProcessBuilder(command).start();
                log.debug("Starting Cluster: {} Node: {} Process Started.", clusterName, i);
            } catch (final Exception e) {
                log.error("ERROR: Cluster: {} Node: {} Trace: {}", clusterName, i, e.getStackTrace());
                return String.format("ERROR: Cluster: %s Node: %s Trace: %s", clusterName, i, e.getStackTrace());
            }
        }
        return "Request Executed";
    }

    public static String stopCluster(String clusterName) {
        try {
            log.debug("Stoping Cluster: {} ", clusterName);
            final Ignite ignite = getIgnite();
            // For some reason it is canceling all the services on all the
            // nodes
            // ignite.services(ignite.cluster().forAttribute("ROLE",
            // clusterName)).cancelAll();
            final IgniteCompute compute = ignite.compute(ignite.cluster().forAttribute("ROLE", clusterName));
            compute.broadcast(new ClusterManager.SendStopClusterMessage());
            log.debug("Stoping Cluster: {} Message Brodcasted.", clusterName);
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
        } catch (final Exception e) {
            log.error("ERROR: Cluster {}  Trace: {}", clusterName, e.getMessage());
            return String.format("ERROR: Cluster: %s  Trace: %s", clusterName, e.getMessage());
        }
        return "Request Executed";
    }

    public static Ignite startNode(IgniteConfiguration cfg) {
        log.debug("Start Node clusterAddresses " + clusterAddresses);
        final TcpDiscoverySpi spi = new TcpDiscoverySpi();
        final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(clusterAddresses));
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

    public static String getNodeCnt(String clusterName) {
        try {
            String nodeCnt;
            final Ignite ignite = ClusterManager.getIgnite();
            final ClusterGroup remoteGroup = ignite.cluster().forAttribute("ROLE", clusterName);
            nodeCnt = String.valueOf(remoteGroup.metrics().getTotalNodes());
            if (ignite.configuration().isClientMode()) {
                ignite.close();
            }
            return nodeCnt;
        } catch (final Exception e) {
            log.error("ERROR: Cluster {}  Trace: {}", clusterName, e.getMessage());
            return String.format("ERROR: Cluster: %s  Trace: %s", clusterName, e.getMessage());
        }
    }

    public static void startWebserverService(String clusterName, String port) throws Exception {
        log.debug("Starting WebServer on Cluster: {} at Port: {}", clusterName, port);
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

        final List<String> command = new ArrayList<String>();
        command.add(path);
        command.add("-cp");
        command.add(classpath);
        command.add(WebserverServiceManager.class.getName());
        command.add(clusterName);
        command.add(port);
        command.add(clusterAddresses);
        log.debug("Starting WebServer on Cluster: {} at Port: {} Command: {}", clusterName, port, command.toString());
        new ProcessBuilder(command).start();
        log.debug("Starting WebServer on Cluster: {} at Port: {} Process Started.", clusterName, port);
    }

    public static void startSchedulerService(String clusterName, String SchedulerThreads) throws Exception {
        log.debug("Starting Schduler on Cluster: {} with Threads: {}", clusterName, SchedulerThreads);
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

        final List<String> command = new ArrayList<String>();
        command.add(path);
        command.add("-cp");
        command.add(classpath);
        command.add(SchedulerServiceManager.class.getName());
        command.add(clusterName);
        command.add(SchedulerThreads);
        command.add(clusterAddresses);
        log.debug("Starting Schduler on Cluster: {} with Threads: {} Command: {}", clusterName, SchedulerThreads, command.toString());
        new ProcessBuilder(command).start();
        log.debug("Starting Schduler on Cluster: {} with Threads: {} Process Started.", clusterName, SchedulerThreads);
    }

    public static void startAuditorProducerSerivce(String loggerName, String brokerList) throws IOException {
        log.debug("Auditor Producer LoggerName: {} BrokerList: {}", loggerName, brokerList);
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

        final List<String> command = new ArrayList<String>();
        command.add(path);
        command.add("-cp");
        command.add(classpath);
        command.add(AuditorProducerServiceManager.class.getName());
        command.add(loggerName);
        command.add(brokerList);
        command.add(clusterAddresses);
        log.debug("Starting Auditor Producer loggerName: {} with brokerList: {} Command: {}", loggerName, brokerList, command.toString());
        new ProcessBuilder(command).start();
        log.debug("Starting Auditor Producer loggerName: {} with brokerList: {} Process Started.", loggerName, brokerList);
    }

    public static void startAuditorConsumerSerivce(String consumerDredgeKey) throws IOException {
        log.debug("Auditor Consumer");
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = String.format("%s%sbin%sjava", System.getProperty("java.home"), separator, separator);

        final List<String> command = new ArrayList<String>();
        command.add(path);
        command.add("-cp");
        command.add(classpath);
        command.add(AuditorConsumerServiceManager.class.getName());
        command.add(clusterAddresses);
        command.add(consumerDredgeKey);
        log.debug("Starting Auditor Consumer  Command: {}", command.toString());
        new ProcessBuilder(command).start();
        log.debug("Starting Auditor Consumer Process Started.");

    }

}
