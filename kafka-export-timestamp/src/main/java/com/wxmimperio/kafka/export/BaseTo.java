package com.wxmimperio.kafka.export;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;

public interface BaseTo {
    void close() throws IOException;

    void initWriter(String topicName, String path) throws IOException;

    void writeTo(ConsumerRecord<String, byte[]> record,GenericRecord gr);
}
