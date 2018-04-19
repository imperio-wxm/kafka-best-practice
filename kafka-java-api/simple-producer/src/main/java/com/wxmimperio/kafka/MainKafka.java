package com.wxmimperio.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.StringBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class MainKafka {
    public static void main(String[] args) throws Exception {
        SendData();
    }

    private final static String BOOTSTRAP_SERVERS = "10.128.74.83:9092";
    private final static String ACKS = "all";


    private static final ThreadLocal<SimpleDateFormat> descFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };


    private static Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    private static Producer<String, String> getProducer() {
        return new KafkaProducer<String, String>(props());
    }

    private static void process(Producer producer, byte[] key, byte[] value, String topic) throws Exception {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        future.get().partition();
    }

    public static void SendData() throws Exception {
        int index = 0;
        Producer producer = getProducer();
        String path = "D:\\d_backup\\github\\kafka-best-practice\\kafka-java-api\\simple-producer\\src\\main\\resources\\wxm_test_avro.avsc";
        Schema schema = getSchema(path);
        try {
            while (index < 1000000) {
                JsonObject jsonMessage = new JsonObject();

                UUID uuid = UUID.randomUUID();
                String uuidStr = uuid.toString();
                String nanoTime = new StringBuilder(String.valueOf(System.nanoTime())).reverse().substring(0, 4) + "-";
                String key = uuidStr.substring(0, 4) + nanoTime + uuidStr.substring(9, uuidStr.length());

                int game_id = (int) (Math.random() * 100 + 1);
                jsonMessage.addProperty("message_key", index);
                jsonMessage.addProperty("message", "message=" + index);
                jsonMessage.addProperty("event_time", descFormat.get().format(new Date()));
                jsonMessage.addProperty("game_id", game_id);

                GenericRecord records = SchemaHelper.format(jsonMessage, schema, true);
                SchemaHelper.serialize(records, schema);
                process(producer, uuid.toString().getBytes(), SchemaHelper.serialize(records, schema), schema.getName());

                System.out.println(jsonMessage.toString());
                //Thread.sleep(1);
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Schema getSchema(String path) throws Exception {
        Schema schema = new Schema.Parser().parse(new File(path));
        return schema;
    }
}
