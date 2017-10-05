package com.wxmimperio.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
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
    private final static String ACKS = "all";
    private final static String topic = "stream-test1";

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
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
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

    private void start() {
        long index = 0L;
        Producer producer = getProducer();
        try {
            while (!closed.get()) {
                Message message = new Message();
                message.setMessage("message=" + index);
                message.setTopic(topic);
                message.setEventTime(descFormat.get().format(new Date()));

                process(producer, Long.toString(System.currentTimeMillis()), JSON.toJSON(message).toString());
                Thread.sleep(2000);
                System.out.println(message);

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

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }
}
