package com.wxmimperio.kafka.export;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.client.*;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ResourceBundle;
import java.util.UUID;

public class ToKudu implements BaseTo {

    private KuduClient client;
    private KuduSession session;
    private KuduTable table;
    private String masterIps;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public ToKudu() {
        initConfig();
        initClientConnecter();
    }

    private void initConfig() {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        masterIps = rb.getString("kudu.master");
    }

    private void initClientConnecter() {
        if (StringUtils.isEmpty(masterIps)) {
            throw new RuntimeException("Master ips can not be empty.");
        }
        client = new KuduClient.KuduClientBuilder(masterIps).build();
        session = client.newSession();
    }

    @Override
    public void close() throws Exception {
        session.close();
        client.close();
    }

    @Override
    public void initWriter(String topicName, String path) throws Exception {
        table = client.openTable(topicName);
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) throws Exception {
        session.apply(getPreparedType(record, gr, table.newInsert()));
    }

    private static Insert getPreparedType(ConsumerRecord<String, byte[]> record, GenericRecord gr, Insert insert) {
        PartialRow row = insert.getRow();
        for (Schema.Field field : gr.getSchema().getFields()) {
            String value = gr.get(field.name()) == null ? "" : gr.get(field.name()).toString();
            for (org.apache.avro.Schema typeSchema : field.schema().getTypes()) {
                if (!typeSchema.isNullable()) {
                    switch (typeSchema.getType()) {
                        case STRING:
                            if ("event_time".equals(field.name())) {
                                row.addTimestamp(field.name(), Timestamp.valueOf(formatter.format(formatter.parse(StringUtils.isEmpty(value) ? "" : value))));
                                row.addString("_key", StringUtils.isEmpty(record.key()) ? UUID.randomUUID().toString() : record.key());
                            } else {
                                row.addString(field.name(), StringUtils.isEmpty(value) ? "" : value);
                            }
                            break;
                        case BYTES:
                            row.addString(field.name(), StringUtils.isEmpty(value) ? "" : value);
                            break;
                        case INT:
                            row.addInt(field.name(), StringUtils.isEmpty(value) ? 0 : Integer.parseInt(value));
                            break;
                        case LONG:
                            row.addLong(field.name(), StringUtils.isEmpty(value) ? 0L : Long.parseLong(value));
                            break;
                        case FLOAT:
                            row.addFloat(field.name(), StringUtils.isEmpty(value) ? 0.0F : Float.parseFloat(value));
                            break;
                        case DOUBLE:
                            row.addDouble(field.name(), StringUtils.isEmpty(value) ? 0.0D : Double.parseDouble(value));
                            break;
                        case BOOLEAN:
                            row.addBoolean(field.name(), Boolean.parseBoolean(value));
                            break;
                        default:
                            throw new RuntimeException(String.format("Not support this type = %s", typeSchema.getType()));
                    }
                }
            }
        }
        return insert;
    }
}
