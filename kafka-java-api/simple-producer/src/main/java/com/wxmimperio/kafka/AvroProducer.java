package com.wxmimperio.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wxmimperio on 2017/10/6.
 */
public class AvroProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "10.128.74.83:9092";
    private final static String ACKS = "all";
    private final static String topic = "wxm_test_avro";

    private static final ThreadLocal<SimpleDateFormat> descFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    static Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    private Producer<String, byte[]> getProducer() {
        return new KafkaProducer<String, byte[]>(props());
    }

    private void start(Schema schema) {
        long index = 0L;
        Producer producer = getProducer();
        try {
            while (!closed.get()) {
                GenericRecord genericRecord = new GenericData.Record(schema);
                UUID uuid = UUID.randomUUID();
                int game_id = (int) (Math.random() * 100 + 1);
                genericRecord.put("message_key", uuid.toString());
                genericRecord.put("message", "message=" + index);
                genericRecord.put("event_time", descFormat.get().format(new Date()));
                genericRecord.put("game_id", game_id);

                process(producer, getSerializedValue(schema, genericRecord));

                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private byte[] getSerializedValue(Schema schema, GenericRecord genericRecord) throws Exception {
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(genericRecord, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    private void process(Producer producer, byte[] value) {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, null, value);
        Future<RecordMetadata> future = producer.send(record);
        try {
            LOG.info("Offset = {}, Partition = {}", future.get().offset(), future.get().partition());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        AvroProducer avroProducer = new AvroProducer();
        String path = "D:\\d_backup\\github\\kafka-best-practice\\kafka-java-api\\simple-producer\\src\\main\\resources\\wxm_test_avro.avsc";
        Schema schema = getSchema(path);
        avroProducer.start(schema);
    }

    private static Schema getSchema(String path) throws Exception {
        Schema schema = new Schema.Parser().parse(new File(path));
        return schema;
    }
}
