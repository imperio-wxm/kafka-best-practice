package com.wxmimperio.kafka.export;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.sdo.dw.rtc.hbaseclient.HBaseClient;
import com.sdo.dw.rtc.hbaseclient.manager.HBaseClientManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ToHbase implements BaseTo {
    private static final Logger LOG = LoggerFactory.getLogger(ToHbase.class);

    private HBaseClientManager manager;
    private HBaseClient hBaseClient;
    private List<JSONObject> result = Lists.newArrayList();

    @Override
    public void close() throws IOException {
        if (result.size() > 0) {
            putData();
        }
        this.manager.dispose();
    }

    @Override
    public void initWriter(String topicName, String path) throws IOException {
        this.manager = new HBaseClientManager();
        this.hBaseClient = manager.createHBaseClient(topicName);
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        JSONObject jsonObject = new JSONObject();
        for (Schema.Field field : gr.getSchema().getFields()) {
            jsonObject.put(field.name(), gr.get(field.name()) == null ? "" : gr.get(field.name()).toString());
        }
        result.add(jsonObject);
        if (result.size() % 1000 == 0) {
            putData();
        }
    }

    private void putData() {
        try {
            hBaseClient.put(result);
            LOG.info("Put data size = " + result.size());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            result.clear();
        }
    }
}
