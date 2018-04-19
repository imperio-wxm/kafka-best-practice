package com.wxmimperio.kafka;

import avro.shaded.com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;

import java.util.Properties;

public class AvroConsumer {
    private Consumer<String, byte[]> consumer;

    public static void main(String[] args) throws Exception {
        new AvroConsumer().start();
    }

    private void initKafkaConsumer() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "10.128.74.83:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group2");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Lists.newArrayList("phoenix_test1"));
    }

    public void start() throws Exception {
        initKafkaConsumer();
        String path = "D:\\d_backup\\github\\kafka-best-practice\\kafka-java-api\\simple-producer\\src\\main\\resources\\phoenix_test1.avsc";
        Schema schema = getSchema(path);
        //Schema schema = SchemaHelper.getSchema("pt_active_glog");
        //schema = SchemaHelper.getSchema("pt_active_glog");

        System.out.println(schema.toString());
        try {
            while (true) {
                processRecord(schema, consumer.poll(1000));
            }
        } finally {
            consumer.close();
        }
    }

    private static Schema getSchema(String path) throws Exception {
        Schema schema = new Schema.Parser().parse(new FileInputStream(new File(path)));
        return schema;
    }

    private void processRecord(Schema schema, ConsumerRecords<String, byte[]> records) {
        for (ConsumerRecord<String, byte[]> record : records) {
            String topic = record.topic();
            String key = record.key();
            String source = null;
            try {
                source = processAvro(schema, record);
                JsonObject message = new JsonParser().parse(source).getAsJsonObject();
                message.addProperty("message_key", key);
                System.out.println(message.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String processAvro(Schema schema, ConsumerRecord<String, byte[]> record) throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        byteArrayInputStream = new ByteArrayInputStream(record.value());
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
        GenericRecord gr = null;
        gr = datumReader.read(gr, decoder);
        return gr.toString();
    }
}
