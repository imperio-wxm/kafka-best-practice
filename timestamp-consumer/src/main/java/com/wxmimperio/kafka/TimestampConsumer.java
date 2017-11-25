package com.wxmimperio.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by wxmimperio on 2017/11/25.
 */
public class TimestampConsumer {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static long MAX_LATENCY = 30 * 60 * 1000;// 30mins
    private static Map<TopicPartition, Long> endOffsets;

    public static void main(String[] args) throws Exception {
        String topics = args[0];
        String start = args[1] + " " + args[2];
        String end = args[3] + " " + args[4];
        // String start = "2017-10-18 12:00:00";
        // String end = "2017-10-18 13:00:00";
        for (String topic : topics.split(",")) {
            System.out.println("Start processing " + topic);
            recover(Sets.newHashSet(topic), sdf.parse(start).getTime(), sdf.parse(end).getTime());
        }
        System.out.println("Finish...");
    }

    private static KafkaConsumer<String, byte[]> getConsumer(Set<String> topics, long startTS) throws ParseException {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.112:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> tpList = getTopicPartList(consumer, topics);
        consumer.assign(tpList);

        Map<TopicPartition, Long> map = Maps.newHashMap();
        for (TopicPartition tp : tpList) {
            map.put(tp, startTS - MAX_LATENCY);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(map);
        for (TopicPartition tp : offsets.keySet()) {
            if (offsets.get(tp) != null) {
                consumer.seek(tp, offsets.get(tp).offset());
            } else {
                consumer.seekToBeginning(Lists.newArrayList(tp));
            }
        }
        endOffsets = getEndOffsets(consumer, offsets);
        return consumer;
    }

    private static Map<TopicPartition, Long> getEndOffsets(KafkaConsumer<String, byte[]> consumer,
                                                           Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
        Map<TopicPartition, Long> notZeroEndOffsets = Maps.newHashMap();
        for (TopicPartition tp : endOffsets.keySet()) {
            if (endOffsets.get(tp) != 0 && offsetsForTimes.get(tp) != null
                    && endOffsets.get(tp) != offsetsForTimes.get(tp).offset()) {
                notZeroEndOffsets.put(tp, endOffsets.get(tp));
            }
        }
        return notZeroEndOffsets;
    }

    private static void recover(Set<String> topics, long startTS, long endTS) throws Exception {
        long ts = System.currentTimeMillis();
        KafkaConsumer<String, byte[]> consumer = getConsumer(topics, startTS);
        long count = 0;

        try {
            while (true) {
                if (endOffsets.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, byte[]> record : consumer.poll(1000)) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (!endOffsets.containsKey(tp)) {
                        continue;
                    }
                    if (record.timestamp() > endTS + MAX_LATENCY) {
                        System.out.println(tp + " is finished.");
                        endOffsets.remove(tp);
                        continue;
                    }
                    if (endOffsets.get(tp) - 1 == record.offset()) {
                        System.out.println(tp + " is finished.");
                        endOffsets.remove(tp);
                    }
                    try {
                        byte[] buffer = record.value();


                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
        }
        System.out.println("Finish " + topics + " .... Count = " + count + ", Cost = "
                + (System.currentTimeMillis() - ts) + "ms");
    }

    private static List<TopicPartition> getTopicPartList(KafkaConsumer<String, byte[]> consumer, Set<String> topics) {
        List<TopicPartition> tpList = Lists.newArrayList();
        for (String topic : topics) {
            List<PartitionInfo> partInfoList = consumer.partitionsFor(topic);
            for (PartitionInfo pi : partInfoList) {
                tpList.add(new TopicPartition(pi.topic(), pi.partition()));
            }
        }
        return tpList;
    }
}
