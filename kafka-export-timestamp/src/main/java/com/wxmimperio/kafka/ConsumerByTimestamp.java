package com.wxmimperio.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;
import com.wxmimperio.kafka.common.WriteType;
import com.wxmimperio.kafka.export.BaseTo;
import com.wxmimperio.kafka.export.ToSequenceFile;
import com.wxmimperio.kafka.util.HttpClientUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

class ConsumerByTimestamp {

    /**
     * 最大延迟：从指定start、end time 向前、后推的时间
     */
    private static long MAX_LATENCY = 30 * 60 * 1000;

    private static Map<TopicPartition, Long> endOffsets;
    private static ResourceBundle rb;

    ConsumerByTimestamp() {
        init();
    }

    private void init() {
        rb = ResourceBundle.getBundle("application");
    }

    private KafkaConsumer<String, byte[]> getConsumer(String topic, long startTS) throws ParseException {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, rb.getString("bootstrap.servers.config"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> tpList = getTopicPartList(consumer, topic);
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

    private Map<TopicPartition, Long> getEndOffsets(KafkaConsumer<String, byte[]> consumer, Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes) {
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

    private List<TopicPartition> getTopicPartList(KafkaConsumer<String, byte[]> consumer, String topic) {
        List<TopicPartition> tpList = Lists.newArrayList();
        List<PartitionInfo> partInfoList = consumer.partitionsFor(topic);
        for (PartitionInfo pi : partInfoList) {
            tpList.add(new TopicPartition(pi.topic(), pi.partition()));
        }
        return tpList;
    }

    private Schema getSchemaFromRegistry(String name) throws IOException {
        try {
            String subject = HttpClientUtil.doGet(rb.getString("schema.reg") + "/subjects/" + name + "/versions/latest");
            String schema = new JsonParser().parse(subject).getAsJsonObject().get("schema").getAsString();
            return new Schema.Parser().parse(schema);
        } finally {
            System.out.println("name = " + name);
        }
    }

    private Map<String, DatumReader<GenericRecord>> getReaders(String topic) throws IOException {
        Map<String, DatumReader<GenericRecord>> map = Maps.newHashMap();
        map.put(topic, new SpecificDatumReader<>(getSchemaFromRegistry(topic)));
        return map;
    }

    void export(String topic, long startTS, long endTS, WriteType writeType, String path) throws Exception {
        long ts = System.currentTimeMillis();
        Map<String, DatumReader<GenericRecord>> readers = getReaders(topic);
        long count = 0;
        long lastPrint = System.currentTimeMillis();

        BaseTo baseTo = null;

        switch (writeType) {
            case SEQUENCE:
                baseTo = new ToSequenceFile();
                baseTo.initWriter(topic, path);
                break;
            case TXT:
                break;
            case JSON:
                break;
            case CASSANDRA:
                break;
            default:
                throw new RuntimeException("Type not exists!");
        }

        try (KafkaConsumer<String, byte[]> consumer = getConsumer(topic, startTS)) {
            while (!endOffsets.isEmpty()) {
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
                        BinaryDecoder binaryEncoder = DecoderFactory.get().binaryDecoder(buffer, null);
                        GenericRecord gr = readers.get(record.topic()).read(null, binaryEncoder);

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        long eventTime = Timestamp.valueOf(LocalDateTime.parse(gr.get("event_time").toString(), formatter)).getTime();
                        if (eventTime >= startTS && eventTime < endTS) {
                            StringBuilder message = new StringBuilder();
                            for (Schema.Field field : gr.getSchema().getFields()) {
                                message.append(gr.get(field.name()) == null ? "" : gr.get(field.name()).toString()).append("\t");
                            }

                            //TODO
                            Map<String, String> messageMap = Maps.newHashMap();
                            messageMap.put(record.key(), message.toString());

                            assert baseTo != null;
                            baseTo.writeTo(messageMap);

                            count++;
                            if (count % 10000 == 0 || (System.currentTimeMillis() - lastPrint) > 10000) {
                                System.out.println("Count = " + count + ", " + record.topic() + " ----- " + gr.get("event_time"));
                                lastPrint = System.currentTimeMillis();
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        } finally {
            assert baseTo != null;
            baseTo.close();
        }
        System.out.println("Finish recovering " + topic + " .... Count = " + count + ", Cost = " + (System.currentTimeMillis() - ts) + "ms");
    }
}
