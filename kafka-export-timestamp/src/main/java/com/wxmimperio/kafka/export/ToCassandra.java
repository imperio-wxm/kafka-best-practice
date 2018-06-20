package com.wxmimperio.kafka.export;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import com.wxmimperio.kafka.util.CassandraUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ToCassandra implements BaseTo {
    private static final Logger LOG = LoggerFactory.getLogger(ToCassandra.class);
    private String cassandraUrl;
    private Cluster cluster;
    private Session session;
    private Schema schema;
    private BatchStatement batchStatement;
    private PreparedStatement preparedStatement;

    private static final ThreadLocal<SimpleDateFormat> ASC_FORMAT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    );

    public ToCassandra(Schema schema) {
        this.schema = schema;
        this.batchStatement = new BatchStatement();
        initConfig();
        initCluster();
        initSession();
    }

    private void initConfig() {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        cassandraUrl = rb.getString("cassandra.url");
    }

    private void initCluster() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 20);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);

        Cluster.Builder builder = Cluster.builder();
        for (String connect : cassandraUrl.split(",", -1)) {
            builder.addContactPoints(connect);
        }
        cluster = builder.withPoolingOptions(poolingOptions).withQueryOptions(queryOptions).build();
        LOG.info("Cassandra Cluster build!");
    }

    @Override
    public void close() throws IOException {
        if (batchStatement.size() > 0) {
            executeStatement();
        }
        this.session.close();
        this.cluster.close();
    }

    @Override
    public void initWriter(String topicName, String path) throws IOException {
        preparedStatement = prepareBatch(schema);
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        List<Object> message = prepareData(record, gr, 1);
        batchStatement.add(preparedStatement.bind(message.toArray()));
        if (batchStatement.size() % 1000 == 0) {
            executeStatement();
        }
    }

    private void initSession() {
        this.session = cluster.connect();
    }

    private List<Object> prepareData(ConsumerRecord<String, byte[]> record, GenericRecord gr, int splitMinute) {
        List<Object> objects = Lists.newArrayList();
        try {
            StringBuilder messageIdBuilder = new StringBuilder();
            messageIdBuilder.append(String.valueOf(record.partition())).append("-").append(String.valueOf(record.offset()));

            if (gr != null) {
                objects = CassandraUtil.changeStrToObject(gr);
                //event_time equals cassandra_time
                Calendar cal = Calendar.getInstance();
                Date cassandraDate;
                try {
                    String eventTime = "event_time";
                    cassandraDate = ASC_FORMAT.get().parse(gr.get(eventTime).toString());
                } catch (Exception e) {
                    String time = ASC_FORMAT.get().format(record.timestamp());
                    cassandraDate = ASC_FORMAT.get().parse(time);
                    LOG.warn("event_time parse error. Topic = " + record.topic() + " error=" + e + " gr=" + gr);
                }
                cal.setTime(cassandraDate);
                String time = CassandraUtil.minuteHourChange(cal, splitMinute);

                // Insert cassandra_time and message_id
                objects.add(0, time.substring(0, 16));
                objects.add(1, record.key() == null ? messageIdBuilder.toString() : record.key());
            }
        } catch (Exception e) {
            LOG.error("Topic " + record.topic() + ", prepare data error. Message=" + new String(record.value()), e);
            return Lists.newArrayList();
        }
        return objects;
    }

    private PreparedStatement prepareBatch(Schema schema) {
        StringBuilder sqlBuilder = new StringBuilder();
        StringBuilder fieldsBuilder = new StringBuilder();

        sqlBuilder.append("INSERT INTO ").append("rtc").append(".").append(schema.getName()).append(" (cassandra_time,message_id,");
        fieldsBuilder.append(") VALUES(?,?,");

        for (Schema.Field field : schema.getFields()) {
            sqlBuilder.append(CassandraUtil.keyWordChange(field.name())).append(",");
            fieldsBuilder.append("?,");
        }
        String insertSQL = sqlBuilder.substring(0, sqlBuilder.toString().length() - 1)
                + fieldsBuilder.substring(0, fieldsBuilder.toString().length() - 1) + ");";

        LOG.info(insertSQL);
        return prepareBatch(insertSQL);
    }

    private PreparedStatement prepareBatch(String sql) {
        PreparedStatement prepareBatch = null;
        try {
            if (session != null) {
                prepareBatch = session.prepare(sql);
            }
        } catch (Exception e) {
            LOG.error("Prepare Statement error:" + e + " sql = " + sql);
        }
        return prepareBatch;
    }

    private void executeStatement() {
        if (session != null) {
            session.execute(batchStatement);
            LOG.info("Insert size = " + batchStatement.size());
            batchStatement.clear();
        }
    }
}
