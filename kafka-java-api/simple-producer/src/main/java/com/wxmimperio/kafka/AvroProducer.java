package com.wxmimperio.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wxmimperio on 2017/10/6.
 */
public class AvroProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.112:9092";
    private final static String ACKS = "all";
    private final static String topic = "stream-test1";

    static Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }


    public static void main(String[] args) {
        AvroProducer avroProducer = new AvroProducer();
        AvroProducer.start();
    }
}
