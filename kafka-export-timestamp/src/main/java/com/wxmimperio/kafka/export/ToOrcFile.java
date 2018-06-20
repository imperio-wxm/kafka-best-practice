package com.wxmimperio.kafka.export;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class ToOrcFile implements BaseTo {

    @Override
    public void close() throws IOException {

    }

    @Override
    public void initWriter(String topicName, String path) throws IOException {

    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {

    }
}
