package com.wxmimperio.kafka.export;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ResourceBundle;

public class ToJsonFile implements BaseTo {

    private Configuration conf;
    private FileSystem fs;
    private FSDataOutputStream outputStream;

    public ToJsonFile() {
        initConfig();
    }

    private void initConfig() {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        Configuration configuration = new Configuration(false);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.addResource(new Path(rb.getString("hdfs.resource.hdfs-site")));
        configuration.addResource(new Path(rb.getString("hdfs.resource.core-site")));
        conf = configuration;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
        fs.close();
    }

    @Override
    public void initWriter(String topicName, String tempPath) throws IOException {
        String fileName = topicName + "_json_" + System.currentTimeMillis();
        String finalPath = tempPath.replaceAll("\\$\\{topic\\}", fileName) + "/" + fileName;
        this.fs = FileSystem.get(conf);
        this.outputStream = fs.create(new Path(finalPath));
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        JsonObject jsonObject = new JsonObject();
        for (Schema.Field field : gr.getSchema().getFields()) {
            jsonObject.addProperty(field.name(), gr.get(field.name()) == null ? "" : gr.get(field.name()).toString());
        }
        try {
            outputStream.write((jsonObject.toString() + "\n").getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
