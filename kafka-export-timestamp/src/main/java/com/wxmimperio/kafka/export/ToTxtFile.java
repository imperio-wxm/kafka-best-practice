package com.wxmimperio.kafka.export;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ResourceBundle;

public class ToTxtFile implements BaseTo {

    private Configuration conf;
    private FileSystem fs;
    private FSDataOutputStream outputStream;

    public ToTxtFile() {
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
        String fileName = topicName + "_txt_" + System.currentTimeMillis();
        String finalPath = tempPath.replaceAll("\\$\\{topic\\}", fileName) + "/" + fileName;
        this.fs = FileSystem.get(conf);
        this.outputStream = fs.create(new Path(finalPath));
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        StringBuilder message = new StringBuilder();
        for (Schema.Field field : gr.getSchema().getFields()) {
            message.append(gr.get(field.name()) == null ? "" : gr.get(field.name()).toString()).append("\t");
        }
        try {
            outputStream.write((message.substring(0, message.length() - 1) + "\n").getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
