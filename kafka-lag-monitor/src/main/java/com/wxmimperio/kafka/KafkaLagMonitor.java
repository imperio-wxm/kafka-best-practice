package com.wxmimperio.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import joptsimple.internal.Strings;
import kafka.admin.AdminClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.math.BigDecimal;
import java.util.*;

public class KafkaLagMonitor {
    private static Logger LOGGER = LoggerFactory.getLogger(KafkaLagMonitor.class);

    private static Set<String> includeGroups = Sets.newHashSet("test_group");
    private static final int INTERVAL_MS = 5 * 60 * 1000;
    private static final String KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static AdminClient adminClient;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminClient = AdminClient.create(props);

        for (String groupId : includeGroups) {
            getOffsets(groupId);
        }
    }

    private static KafkaConsumer<byte[], byte[]> getKafkaConsumer(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<byte[], byte[]>(properties);
    }

    private static void getOffsets(String group) {
        List<Tuple2<Long, Double>> lagList = Lists.newArrayList();
        Collection<AdminClient.ConsumerSummary> consumerSummaries = JavaConversions.asJavaCollection(adminClient.describeConsumerGroup(group, 60000).consumers().get());
        if (consumerSummaries.isEmpty()) {
            LOGGER.warn("Consumer group " + group + " does not exist or is rebalancing.");
        } else {
            KafkaConsumer<?, ?> consumer = getKafkaConsumer(KAFKA_BOOTSTRAP_SERVERS, group);
            List<String> data = Lists.newArrayList();
            try {
                for (AdminClient.ConsumerSummary cs : consumerSummaries) {
                    Collection<TopicPartition> tpList = JavaConversions.asJavaCollection(cs.assignment());
                    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tpList);
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tpList);
                    Map<TopicPartition, Long> currentOffsets = Maps.newHashMap();
                    for (TopicPartition tp : tpList) {
                        OffsetAndMetadata oam = consumer.committed(tp);
                        if (oam == null) {
                            LOGGER.warn(group + " has no offset on " + tp + ", consumer = " + cs.consumerId());
                            continue;
                        }
                        LOGGER.debug(String.format("%s, Group = %s, TopicPartition = %s, Offset = %s",
                                new Object[]{new Date(), group, tp, oam.offset()}));
                        currentOffsets.put(tp, oam.offset());
                    }

                    for (TopicPartition tp : currentOffsets.keySet()) {
                        try {
                            long begin = beginningOffsets.get(tp);
                            long end = endOffsets.get(tp);
                            long offset = currentOffsets.get(tp);
                            long lag = end - offset;

                            System.out.println(tp.topic() + " , first = " + begin + ", end = " + end + ",partition = " + tp.partition());
                            System.out.println(tp.topic() + " = " + lag);

                            double lagPercent = (end - begin) == 0 ? 0
                                    : new BigDecimal((end - offset) * 1.0 / (end - begin) / 100)
                                    .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            lagList.add(new Tuple2<Long, Double>(lag, (end - begin) == 0 ? 0.0
                                    : new BigDecimal((end - offset) * 1.0 / (end - begin)).doubleValue()));
                        } catch (Exception e) {
                            LOGGER.error("encounter exception.", e);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("encounter exception.", e);
            } finally {
                consumer.close();
            }
        }
    }
}
