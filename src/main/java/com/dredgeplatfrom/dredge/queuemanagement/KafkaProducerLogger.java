package com.dredgeplatfrom.dredge.queuemanagement;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerLogger extends AppenderSkeleton {
    final static Logger log = LoggerFactory.getLogger(KafkaProducerLogger.class);

    private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    private static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String ACKS_CONFIG = "acks";
    private static final String RETRIES_CONFIG = "retries";
    private static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    private static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";

    private String brokerList = null;
    private String topic = null;
    private String compressionType = null;

    private int retries = 0;
    private int requiredNumAcks = Integer.MAX_VALUE;
    private boolean syncSend = false;
    private Producer<byte[], byte[]> producer = null;

    public Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public int getRequiredNumAcks() {
        return requiredNumAcks;
    }

    public void setRequiredNumAcks(int requiredNumAcks) {
        this.requiredNumAcks = requiredNumAcks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean getSyncSend() {
        return syncSend;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }

    @Override
    public void activateOptions() {
        System.out.println("activateOptions" + "-" + System.currentTimeMillis());
        log.info("Kafka producer connected to " + brokerList);
        final Properties props = new Properties();
        if (brokerList != null) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        }
        if (props.isEmpty()) {
            throw new ConfigException("The bootstrap servers property should be specified");
        }
        if (topic == null) {
            throw new ConfigException("Topic must be specified by the Kafka log4j appender");
        }
        if (compressionType != null) {
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);
        }
        if (requiredNumAcks != Integer.MAX_VALUE) {
            props.put(ACKS_CONFIG, Integer.toString(requiredNumAcks));
        }
        if (retries > 0) {
            props.put(RETRIES_CONFIG, retries);
        }

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("batch.size", "1");

        this.producer = getKafkaProducer(props);
    }

    protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
        return new KafkaProducer<byte[], byte[]>(props);
    }

    @Override
    protected void append(LoggingEvent event) {
        final String message = subAppend(event);
        final Future<RecordMetadata> response = producer.send(new ProducerRecord<byte[], byte[]>(topic, message.getBytes()));
        if (syncSend) {
            try {
                response.get();
            } catch (final InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (final ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }
        System.out.println("append" + "=" + message.getBytes() + "-" + System.currentTimeMillis());
    }

    private String subAppend(LoggingEvent event) {
        return this.layout == null ? event.getRenderedMessage() : this.layout.format(event);
    }

    @Override
    public void close() {
        System.out.println("closed" + "-" + System.currentTimeMillis());
        if (!this.closed) {
            this.closed = true;
            log.info("Closed Kafka Producer");
            producer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        System.out.println("requiresLayout" + System.currentTimeMillis());
        return true;
    }

}
