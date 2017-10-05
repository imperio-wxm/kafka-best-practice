package com.wxmimperio.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wxmimperio on 2017/7/23.
 */
public class ConsumerTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerTask.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.112:9092";
    private final static String groupId = "simple-group";
    private final static String AUTO_OFFSET_RESET = "earliest";
    //earliest
    private final static int SESSION_TIMEOUT_MS = 10000;
    private KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerTask(String topic) {
        this.topic = topic;
    }

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
       // System.out.println(("topic = " + record.topic() + "message = " + record.value()));
    }

    private void start() {
        LOG.info("topic" + topic + "start!");
        consumer = getConsumer();
        try {
            consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    //consumer.commitSync();
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
                long index = 0;
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord record : records) {
                    process(record);
                }

                if (index % 5000 == 0) {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {
                                consumer.commitSync();
                                LOG.error("Commit failed for " + topic + " offsets {}", offsets, exception);
                            }
                        }
                    });
                }
                //System.out.println(Thread.currentThread().getName() + " " + consumer.subscription());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("topic " + topic + "closed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }


    public void shutdown() {
        closed.set(true);
    }

    @Override
    public void run() {
        start();
    }
}
