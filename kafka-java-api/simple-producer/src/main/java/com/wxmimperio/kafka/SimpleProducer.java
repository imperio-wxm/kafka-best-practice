package com.wxmimperio.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wxmimperio on 2017/6/4.
 */
public class SimpleProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = "192.168.1.112:9092";
    private final static String ACKS = "1";
    private final static String topic = "test_producer_size";

    private static final ThreadLocal<SimpleDateFormat> descFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 50);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private Producer<String, String> getProducer() {
        return new KafkaProducer<String, String>(props());
    }

    private void process(Producer producer, String key, String value) {
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        try {
            LOG.info("Topic = {}, Offset = {}, Partition = {}", future.get().toString(), future.get().offset(), future.get().partition());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void processBatch(Producer producer, List<ProducerRecord<String, String>> records) {
        List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());
        final ExtendedSerializer<String> keySerializer = ExtendedSerializer.Wrapper.ensureExtended(new StringSerializer());
        final ExtendedSerializer<String> valueSerializer = ExtendedSerializer.Wrapper.ensureExtended(new StringSerializer());
        records.forEach(record -> {
            byte[] serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            byte[] serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(
                    RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, serializedKey, serializedValue, record.headers().toArray()
            );
            if (serializedSize > (Integer) props().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)) {
                LOG.info(String.format("SerializedSize = %s bytes, Topic = %s, msg = %s, header = %s, key = %s",
                        serializedSize, record.topic(), record.value(), record.headers(), record.key()
                ));
            }
            futures.add(producer.send(record));
        });
        // Prevent linger.ms from holding the batch
        producer.flush();
        futures.forEach(future -> {
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = future.get();
                LOG.info("Topic = {}, Offset = {}, Partition = {}", recordMetadata.toString(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException | ExecutionException e) {
                if (e instanceof ExecutionException && e.getCause() instanceof RecordTooLargeException) {
                    RecordTooLargeException recordTooLargeException = (RecordTooLargeException) e.getCause();
                    // recordTooLargeException == null
                    System.out.println(recordTooLargeException.recordTooLargePartitions());
                    // The message is 185 bytes when serialized which is larger than the maximum request size you have configured with the max.request.size configuration.
                    System.out.println(recordTooLargeException.getLocalizedMessage());
                }
            }
        });
    }

    private void start() {
        long index = 0L;
        Producer producer = getProducer();
        try {
            while (!closed.get()) {
                Message message = new Message();
                message.setMessage("message=" + index);
                message.setTopic(topic);
                message.setEventTime(descFormat.get().format(new Date()));

                //process(producer, Long.toString(System.currentTimeMillis()), JSON.toJSON(message).toString());
                processBatch(producer, getBatchMsg(10));
                Thread.sleep(2000);
                System.out.println(String.format("size = %s, msg = %s", message.toString().getBytes().length, message));

              /*  if (index > 10) {
                    closed.set(true);
                }*/
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private List<ProducerRecord<String, String>> getBatchMsg(int size) {
        List<ProducerRecord<String, String>> records = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            Message message = new Message();
            message.setMessage("message=" + i);
            message.setTopic(topic);
            message.setEventTime(descFormat.get().format(new Date()));
            records.add(new ProducerRecord<String, String>(topic, Long.toString(System.currentTimeMillis()), JSON.toJSON(message).toString()));
        }
        return records;
    }

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }
}
