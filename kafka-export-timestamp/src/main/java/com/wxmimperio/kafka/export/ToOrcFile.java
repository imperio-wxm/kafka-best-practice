package com.wxmimperio.kafka.export;

import com.wxmimperio.kafka.common.DisplayColumnType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ResourceBundle;

public class ToOrcFile implements BaseTo {

    private Configuration conf;
    private Writer writer;
    private Schema avroSchema;
    private TypeDescription orcSchema;
    private VectorizedRowBatch batch;

    public ToOrcFile(Schema avroSchema) {
        this.avroSchema = avroSchema;
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
        if (batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }

    @Override
    public void initWriter(String topicName, String tempPath) throws IOException {
        String fileName = topicName + "_orc_" + System.currentTimeMillis();
        String finalPath = tempPath.replaceAll("\\$\\{topic\\}", fileName) + "/" + fileName;
        this.orcSchema = getOrcSchema();
        this.batch = orcSchema.createRowBatch();
        this.writer = OrcFile.createWriter(
                new Path(finalPath),
                OrcFile.writerOptions(conf).setSchema(orcSchema).compress(CompressionKind.SNAPPY)
        );
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        Object[] temp = new Object[gr.getSchema().getFields().size()];
        for (int i = 0; i < gr.getSchema().getFields().size(); i++) {
            temp[i] = gr.get(gr.getSchema().getFields().get(i).name()) == null ? "" : gr.get(gr.getSchema().getFields().get(i).name()).toString();
        }
        for (int i = 0; i < orcSchema.getFieldNames().size(); i++) {
            try {
                setColumnVectorVal(orcSchema.getChildren().get(i), batch.cols[i], batch.size, String.valueOf(temp[i]));
            } catch (ArrayIndexOutOfBoundsException e) {
                // 当数据中字段个数少于表结构字段个数，用空字符串补齐
                temp = new ArrayList<Object>(Arrays.asList(temp)) {{
                    add("");
                }}.toArray();
                setColumnVectorVal(orcSchema.getChildren().get(i), batch.cols[i], batch.size, String.valueOf(temp[i]));
            }
        }
        // 行数，最大为createRowBatch—> maxSize
        batch.size++;
        if (batch.size == batch.getMaxSize()) {
            try {
                writer.addRowBatch(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            batch.reset();
        }
    }

    private TypeDescription getOrcSchema() {
        TypeDescription schema = TypeDescription.createStruct();
        for (Schema.Field field : avroSchema.getFields()) {
            schema.addField(field.name(), TypeDescription.fromString(parse(field.schema()).name()));
        }
        return schema;
    }

    private DisplayColumnType parse(Schema schema) {
        switch (schema.getType()) {
            case UNION:
                for (Schema type : schema.getTypes()) {
                    try {
                        return parse(type);
                    } catch (Exception e) {
                        continue;
                    }
                }
                throw new RuntimeException("The valid types are: " + schema.getTypes());
            case STRING:
                return DisplayColumnType.STRING;
            case BYTES:
                return DisplayColumnType.SMALLINT;
            case INT:
                return DisplayColumnType.INT;
            case LONG:
                return DisplayColumnType.BIGINT;
            case FLOAT:
                return DisplayColumnType.FLOAT;
            case DOUBLE:
                return DisplayColumnType.DOUBLE;
            case BOOLEAN:
                return DisplayColumnType.BOOLEAN;
            default:
                throw new RuntimeException("Invalid type, only STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL are permitted.");
        }
    }

    private static void setColumnVectorVal(TypeDescription td, ColumnVector cv, int rc, String val) {
        if (null == val || "".equals(val) || "\\N".equals(val)) {
            cv.noNulls = false;
            cv.isNull[rc] = true;
        } else {
            try {
                switch (td.getCategory()) {
                    case BOOLEAN:
                        long bval = Long.parseLong(val) > 0 ? 1 : 0;
                        ((LongColumnVector) cv).vector[rc] = bval;
                        break;
                    case INT:
                    case LONG:
                    case BYTE:
                        ((LongColumnVector) cv).vector[rc] = Long.parseLong(val);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        ((DoubleColumnVector) cv).vector[rc] = Double.parseDouble(val);
                        break;
                    case STRING:
                    case VARCHAR:
                    case BINARY:
                        ((BytesColumnVector) cv).setVal(rc, val.getBytes(Charset.forName("UTF-8")));
                        break;
                    default:
                        throw new RuntimeException(td.getCategory() + ":" + val);
                }
            } catch (NumberFormatException e) {
                cv.noNulls = false;
                cv.isNull[rc] = true;
            }
        }
    }
}
