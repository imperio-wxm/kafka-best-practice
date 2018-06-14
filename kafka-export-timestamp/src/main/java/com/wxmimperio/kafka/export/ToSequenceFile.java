package com.wxmimperio.kafka.export;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

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
    public void initWriter(String topicName, String tempPath) throws IOException {
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
    public void writeTo(Map<String, String> message) {
        message.forEach((k, v) -> {
            try {
                if (!StringUtils.isEmpty(v)) {
                    writer.append(new Text(k), new Text(v.substring(0, v.length() - 1)));
                    writer.sync();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
