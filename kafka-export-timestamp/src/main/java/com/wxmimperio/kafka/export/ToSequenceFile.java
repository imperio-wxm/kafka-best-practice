package com.wxmimperio.kafka.export;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;
import java.util.ResourceBundle;

public class ToSequenceFile implements BaseTo {

    private SequenceFile.Writer writer;
    private Configuration conf;

    public ToSequenceFile() {
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
    public void initWriter(String topicName, String tempPath) throws Exception {
        String fileName = topicName + "_seq_" + System.currentTimeMillis();
        String finalPath = tempPath.replaceAll("\\$\\{topic\\}", fileName) + "/" + fileName;
        Path path = new Path(finalPath);
        Text value = new Text();
        Text key = new Text();
        writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()),
                //In hadoop-2.6.0-cdh5, it can use hadoop-common-2.6.5 with appendIfExists()
                //SequenceFile.Writer.appendIfExists(true),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
        );
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) throws Exception {
        StringBuilder message = new StringBuilder();
        for (Schema.Field field : gr.getSchema().getFields()) {
            message.append(gr.get(field.name()) == null ? "" : gr.get(field.name()).toString()).append("\t");
        }
        if (!StringUtils.isEmpty(new String(record.value(), "UTF-8"))) {
            writer.append(new Text(record.key()), new Text(message.substring(0, message.length() - 1)));
            writer.sync();
        }
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
