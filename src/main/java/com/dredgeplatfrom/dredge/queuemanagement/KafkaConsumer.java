package com.dredgeplatfrom.dredge.queuemanagement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class KafkaConsumer {
    final static Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    boolean runConsumer;
    final String topic;
    final int partition;
    final List<String> seeds = new ArrayList<String>();
    final int port;
    private List<String> m_replicaBrokers = new ArrayList<String>();
    ConsumerService cs;

    public KafkaConsumer(ConsumerService cs, String consumerDredgeKey) throws Exception {
        this.topic = "auditor";
        this.partition = 0;
        this.seeds.add("192.168.0.106");
        this.port = 6667;
        this.m_replicaBrokers = new ArrayList<String>();
        this.runConsumer = true;
        this.cs = cs;
        startConsumer();
    }

    public void startConsumer() throws Exception {
        final PartitionMetadata metadata = findLeader(seeds, topic, partition, port);
        if (metadata == null) {
            log.error("Error communicating with Broker [ {} ] to find Leader for [ {}, {} ]", seeds, topic, partition);
            return;
        }
        if (metadata.leader() == null) {
            log.error("Can't Find Leader for [ {}, {} ]", topic, partition);
            return;
        }

        String leadBroker = metadata.leader().host();
        final String clientName = String.format("Client_%s_%s", topic, partition);

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long readOffset = getOffset(topic, partition);

        while (runConsumer) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }
            final FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build();
            final FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                final short code = fetchResponse.errorCode(topic, partition);
                log.error("Error fetching data from the Broker: {} Reason: {}", leadBroker, code);
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }

            long numRead = 0;
            for (final MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                readOffset = messageAndOffset.nextOffset();
                final ByteBuffer payload = messageAndOffset.message().payload();
                final byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                cs.processConsumerData(String.valueOf(messageAndOffset.offset()), new String(bytes, "UTF-8"));
                setOffset(String.valueOf(messageAndOffset.offset()));
                numRead++;
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ie) {
                }
            }
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public void stopConsumer() {
        runConsumer = false;
    }

    public String getConsumerStatus() {
        if (runConsumer) {
            return "Started";
        } else {
            return "Stopped";
        }
    }

    private long getOffset(String topic, int partition) {
        return 0;
    }

    private void setOffset(String offset) {

    }

    private PartitionMetadata findLeader(List<String> seeds, String topic, int partition, int port) {
        PartitionMetadata returnMetaData = null;
        for (final String seed : seeds) {
            SimpleConsumer consumer = null;
            try {
                final List<String> topics = new ArrayList<String>();
                topics.add(topic);
                final TopicMetadataRequest req = new TopicMetadataRequest(topics);

                // broker_host, broker_port, timeout, buffer_size, client_id
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                final kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                final List<TopicMetadata> metaData = resp.topicsMetadata();
                for (final TopicMetadata item : metaData) {
                    for (final PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (final Exception e) {
                log.error("Error communicating with Broker [ {} ] to find Leader for [ {}, {} ] Reason: {}", seed, topic, partition, e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (final kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    private String findNewLeader(String oldLeadBroker, String topic, int partition, int port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            final PartitionMetadata metadata = findLeader(m_replicaBrokers, topic, partition, port);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeadBroker.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ie) {
                }
            }
        }
        log.error("Error communicating with Broker [ {} ] to find new Leader for [ {}, {} ]", m_replicaBrokers, topic, partition);
        throw new Exception(String.format("Error communicating with Broker [ %s ] to find new Leader for [ %s, %s ]", m_replicaBrokers, topic, partition));
    }
}
