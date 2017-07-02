package com.wxmimperio.kafka.main;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.Lists;
import com.wxmimperio.kafka.common.CassandraManager;
import com.wxmimperio.kafka.util.CassandraUtil;
import com.wxmimperio.kafka.util.CommonUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wxmimperio on 2017/7/1.
 */
public class KafkaCassandraSink {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCassandraSink.class);

    private CassandraManager cassandraManager = CassandraManager.getInstance();
    private Map<String, BatchStatement> statementCache;
    private Map<String, DatumReader<GenericRecord>> datumReaderCache;
    private Map<String, PreparedStatement> prepareCache;
    private Map<String, Schema> schemaCache;
    private KafkaConsumer<String, byte[]> consumer;
    private String topics;
    private String KEY_SPACE = "rt";


    private void initConsumer() {
        LOG.info("start initKafkaConsumer.");
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        consumer = new KafkaConsumer<>(props);
        reSubscribe();
        LOG.info("end initKafkaConsumer.");
    }

    private void reSubscribe() {
        consumer.unsubscribe();
        consumer.subscribe(Arrays.asList(topics.split(",", -1)));
    }

    /**
     * Init schema and datumReader
     */
    private void initSchema() {
        LOG.info("start initSchema.");
        Map<String, DatumReader<GenericRecord>> tempReaders = new ConcurrentHashMap<>();
        Map<String, Schema> tempSchemas = new ConcurrentHashMap<>();
        try {
            for (String topic : topics.split(",", -1)) {
                String fileName = "src/main/resources/" + topic + ".avsc";
                Schema schema = new Schema.Parser().parse(new File(fileName));
                tempReaders.put(topic, new SpecificDatumReader<GenericRecord>(schema));
                tempSchemas.put(topic, schema);
            }
            datumReaderCache = tempReaders;
            schemaCache = tempSchemas;
        } catch (Exception e) {
            LOG.error("Failed to load schema, ", e);
        }
        LOG.info("end initSchema.");
    }


    private void initStatements() {
        LOG.info("start initCassandraTables.");
        Map<String, BatchStatement> tempStatements = new ConcurrentHashMap<>();
        Map<String, PreparedStatement> tempPrepares = new ConcurrentHashMap<>();
        KeyspaceMetadata keyspaceMetadata = cassandraManager.getCluster().getMetadata().getKeyspace(KEY_SPACE);
        for (String topic : topics.split(",", -1)) {
            try {
                // table not exist,prepare a new statement
                if (!tableExist(keyspaceMetadata, topic)) {
                    tempPrepares.put(topic, CassandraUtil.prepareBatch(""));
                    tempStatements.put(topic, new BatchStatement());
                }
                List<String> columns = getColumns(keyspaceMetadata, topic);
                List<String> fields = getFields(topic);
                // table has been changed,prepare a new statement
                if (!CommonUtils.compareList(columns, fields)) {
                    tempPrepares.put(topic, CassandraUtil.prepareBatch(""));
                    tempStatements.put(topic, new BatchStatement());
                } else { // table has been prepared
                    if (prepareCache.containsKey(topic) && statementCache.containsKey(topic)) {
                        tempPrepares.put(topic, prepareCache.get(topic));
                        tempStatements.put(topic, statementCache.get(topic));
                    } else {
                        tempPrepares.put(topic, CassandraUtil.prepareBatch(""));
                        tempStatements.put(topic, new BatchStatement());
                        LOG.warn(topic + " has been prepared and do not change schema.");
                    }
                }
            } catch (Exception e) {
                LOG.error(topic + " prepare error.", e);
            }
        }
        statementCache = tempStatements;
        prepareCache = tempPrepares;
        LOG.info("end initCassandraTables.");
    }

    /**
     * @param keyspaceMetadata
     * @param topic
     * @return
     */
    private List<String> getColumns(KeyspaceMetadata keyspaceMetadata, String topic) throws Exception {
        List<String> columns = new ArrayList<>();
        for (ColumnMetadata columnMetadata : keyspaceMetadata.getTable(topic).getColumns()) {
            columns.add(columnMetadata.getName());
        }
        return columns;
    }

    /**
     * @param keyspaceMetadata
     * @param topic
     * @return
     */
    private boolean tableExist(KeyspaceMetadata keyspaceMetadata, String topic) {
        try {
            keyspaceMetadata.getTable(topic).exportAsString();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * @param topic
     * @return
     */
    private List<String> getFields(String topic) throws Exception {
        List<String> fields = new ArrayList<>();
        for (Schema.Field field : schemaCache.get(topic).getFields()) {
            fields.add(field.name());
        }
        return fields;
    }
}
