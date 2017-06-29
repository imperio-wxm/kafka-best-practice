package com.wxmimperio.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.wxmimperio.kafka.Main.readFileByLines;

/**
 * Created by wxmimperio on 2017/6/29.
 */
public class ConsumerRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerRunner.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.112:9092";
    private final static String groupId = "simple-group";
    private final static String AUTO_OFFSET_RESET = "earliest";
    private final static int SESSION_TIMEOUT_MS = 10000;
    private static ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();
    private static long lastModified = 0;

    public ConsumerRunner(ConcurrentLinkedQueue<String> subscribedTopics, String topics) {
        //this.subscribedTopics = subscribedTopics;
        //this.topics = topics;
    }

    private Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        return props;
    }

    private KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<String, String>(props());
    }

    private void process(ConsumerRecord<String, String> record) {
        LOG.info("topic = {}, message = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    private void start() {
        String topic = readFileByLines("E:\\coding\\github\\kafka-best-practice\\kafka-java-api\\kafka-dynamic-subscribe\\src\\main\\resources\\test.properties");
        final Map<String, String> topicMap = new HashMap<>();
        topicMap.put("topics", topic);
        new Timer().schedule(new TimerTask() {
            public void run() {
                System.out.println("启动timer" + Thread.currentThread().getName());
                //ResourceBundle resource = ResourceBundle.getBundle("test");
                //String topics = resource.getString("topics");
                String topic = readFileByLines("E:\\coding\\github\\kafka-best-practice\\kafka-java-api\\kafka-dynamic-subscribe\\src\\main\\resources\\test.properties");

                System.out.println("=============");
                System.out.println(topic);
                System.out.println(topicMap);
                System.out.printf("===============");

                if (!topicMap.get("topics").equalsIgnoreCase(topic)) {
                    System.out.println(topic);
                    subscribedTopics.addAll(Arrays.asList(topic.split(",", -1)));
                    System.out.println(Thread.currentThread().getName() + " 刷新");
                    topicMap.put("topics", topic);
                }
            }
        }, 0, 2000);

        KafkaConsumer<String, String> consumer = getConsumer();
        try {
            consumer.subscribe(Arrays.asList(topicMap.get("topics").split(",", -1)), new ConsumerRebalanceListener() {
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
                System.out.println(Thread.currentThread().getName() + " " + consumer.subscription());

                if (!subscribedTopics.isEmpty()) {
                    Iterator<String> iter = subscribedTopics.iterator();
                    List<String> topics = new ArrayList<>();
                    while (iter.hasNext()) {
                        topics.add(iter.next());
                    }
                    consumer.unsubscribe();
                    consumer.subscribe(topics); // 重新订阅topic
                    System.out.println("重新订阅");
                    subscribedTopics.clear();
                }
                /*for (ConsumerRecord<String, String> record : records) {
                    process(record);
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public void run() {
        start();
    }

    public static boolean isFileUpdated(String filename) {
        File file = new File(filename);
        if (file.isFile()) {
            long lastUpdateTime = file.lastModified();
            if (lastUpdateTime > lastModified) {
                System.out.println("The properties file was modified.");
                lastModified = lastUpdateTime;
                return true;
            } else {
                System.out.println("The properties file was not modified.");
                return false;
            }
        } else {
            System.out.println("The path does not point to a file.");
            return false;
        }
    }
}
