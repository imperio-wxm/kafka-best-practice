package com.wxmimperio.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wxmimperio on 2017/6/4.
 */
public class SimpleConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.112:9092";
    private final static String topic = "simple-topic-001";
    private final static String groupId = "simple-group";
    private final static String AUTO_OFFSET_RESET = "earliest";
    private final static int SESSION_TIMEOUT_MS = 10000;
    private static KafkaConsumer<String, String> consumer;

    private Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        return props;
    }

    private KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<String, String>(props());
    }

    private void process(ConsumerRecord<String, String> record) {
        if (record.offset() % 2 == 0) {
            //consumer.commitAsync();
        }
        LOG.info("topic = {}, message = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    private void start() {
        consumer = getConsumer();
        try {
            consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        System.out.println("onPartitionsRevoked===============");
                        for (TopicPartition topicPartition : partitions) {
                            System.out.println(topicPartition.topic() + "-" + topicPartition.partition());
                        }
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        System.out.println("onPartitionsAssigned===========");
                        for (TopicPartition topicPartition : partitions) {
                            System.out.println(topicPartition.topic() + "-" + topicPartition.partition());
                        }
                    }
                }
            });

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private void shutdown() {
        closed.set(true);
    }

    public static void main(String[] args) {
        SimpleConsumer simpleConsumer = new SimpleConsumer();
        simpleConsumer.start();
        //simpleConsumer.shutdown();
    }
}
