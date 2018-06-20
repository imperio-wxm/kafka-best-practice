package com.wxmimperio.kafka.export;

import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

public class ToPhoenix implements BaseTo {
    private static final Logger LOG = LoggerFactory.getLogger(ToPhoenix.class);

    private Connection connection;
    private PreparedStatement pst;
    private String sql;
    private Schema schema;

    public ToPhoenix(Schema schema) {
        initConfig();
        this.schema = schema;
    }

    private void initConfig() {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        String url = rb.getString("phoenix.query.url");
        String driver = rb.getString("phoenix.query.driver");
        Properties props = new Properties();
        props.setProperty("phoenix.query.dateFormatTimeZone", rb.getString("phoenix.query.dateFormatTimeZone"));
        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, props);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            pst.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initWriter(String topicName, String path) throws IOException {
        try {
            this.sql = getPreparedSql(schema);
            pst = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeTo(ConsumerRecord<String, byte[]> record, GenericRecord gr) {
        try {
            pst = formatData(sql, pst, schema, gr, record);
            pst.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String getPreparedSql(Schema schema) {
        String dbName = "";
        if (!StringUtils.isEmpty(schema.getProp("db"))) {
            JsonObject dbInfoJson = new JsonParser().parse(schema.getProp("db")).getAsJsonObject();
            for (JsonElement dbInfo : dbInfoJson.get("identifiers").getAsJsonArray()) {
                JsonObject db = dbInfo.getAsJsonObject();
                if (db.get("storageType").getAsString().equalsIgnoreCase("PHOENIX")) {
                    dbName = db.get("name").getAsString();
                }
            }
        }
        StringBuffer sql = new StringBuffer("UPSERT INTO " + dbName + ".");
        //判断是否有messagekey
        boolean hasMessageKey = getMessageKey(schema.getName(), dbName);
        sql.append(schema.getName().toUpperCase());
        // field name
        StringBuffer fields = new StringBuffer("(");
        if (hasMessageKey) {
            fields.append("MESSAGEKEY").append(",");
        }
        Map<Integer, String> posMap = Maps.newHashMap();
        schema.getFields().forEach(field -> {
            int pos = field.pos();
            if (hasMessageKey) {
                pos++;
            }
            // 特殊处理schema中的logicalType类型
            if (!StringUtils.isEmpty(field.getProp("logicalType"))) {
                switch (field.getProp("logicalType").toLowerCase()) {
                    case "datetime":
                        posMap.put(pos, "TO_TIMESTAMP(?,'yyyy-MM-dd HH:mm:ss','GMT+8'),");
                        break;
                    case "timestamp":
                        posMap.put(pos, "?,");
                        break;
                    case "date":
                        posMap.put(pos, "TO_DATE(?,'yyyy-MM-dd','GMT+8'),");
                        break;
                    case "time":
                        posMap.put(pos, "TO_TIME(?,'HH:mm:ss','GMT+8'),");
                        break;
                    default:
                        posMap.put(pos, "?,");
                        break;
                }
            } else {
                posMap.put(pos, "?,");
            }
            fields.append(field.name()).append(",");
        });
        sql.append(fields.deleteCharAt(fields.length() - 1).append(")").toString()).append(" VALUES(");
        String tempSql = sql.toString().toUpperCase();
        sql.setLength(0);
        sql.append(tempSql);
        int fieldSize = schema.getFields().size();
        if (hasMessageKey) {
            fieldSize++;
        }
        for (int i = 0; i < fieldSize; i++) {
            sql.append(posMap.get(i) == null ? "?," : posMap.get(i));
        }
        sql.deleteCharAt(sql.length() - 1).append(")");
        LOG.info("Insert sql = " + sql.toString());
        return sql.toString();
    }

    private boolean getMessageKey(String tableName, String dbName) {
        String pkSql = "select COLUMN_NAME,KEY_SEQ,IS_ROW_TIMESTAMP from SYSTEM.CATALOG where TABLE_NAME='" +
                tableName.toUpperCase() + "' and TABLE_SCHEM = '" + dbName.toUpperCase() + "'  and KEY_SEQ is not null order by KEY_SEQ";

        try (PreparedStatement pds = connection.prepareStatement(pkSql)) {
            ResultSet resultSet = pds.executeQuery();
            while (resultSet.next()) {
                if (!StringUtils.isEmpty(resultSet.getString("COLUMN_NAME")) &&
                        resultSet.getString("COLUMN_NAME").equalsIgnoreCase("MESSAGEKEY")) {
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private PreparedStatement formatData(String sql, PreparedStatement pst, Schema schema, GenericRecord gr, ConsumerRecord<String, byte[]> record)
            throws SQLException {
        List<Schema.Field> fields = schema.getFields();
        int index = 1;
        int columnSize = appearNumber(sql, "?");
        if (gr != null && pst != null) {
            if (columnSize - schema.getFields().size() > 1) {
                LOG.warn("Schema fields size not match column size, Schema = " + schema.toString() + ", sql = " + sql);
                return pst;
            } else if (columnSize != schema.getFields().size()) {
                // schema中列个数与实际Phoenix表中列个数不同，表示使用了message_key作为primary key，则添加到数据最前面
                columnSize--;
                index = 2;
                pst.setString(1, record.key());
            }

            for (int i = 0; i < columnSize; i++) {
                Schema.Field field = fields.get(i);
                String datum = gr.get(field.name()) == null ? "" : gr.get(field.name()).toString();
                pst = getPreparedType(
                        pst,
                        schema.getName(),
                        field.schema(),
                        index,
                        datum,
                        field
                );
                index++;
            }
            pst.addBatch();
        }
        return pst;
    }

    private static int appearNumber(String srcText, String findText) {
        int count = 0;
        int index = 0;
        while ((index = srcText.indexOf(findText, index)) != -1) {
            index = index + findText.length();
            count++;
        }
        return count;
    }

    private PreparedStatement getPreparedType(PreparedStatement pst, String schemaName, Schema fieldSchema, int index, String datum, Schema.Field field) throws SQLException {
        switch (fieldSchema.getType()) {
            case UNION:
                for (Schema type : fieldSchema.getTypes()) {
                    try {
                        return getPreparedType(pst, schemaName, type, index, datum, field);
                    } catch (Exception e) {
                        continue;
                    }
                }
                throw new RuntimeException("No match type for the datum: " + datum + ", the valid types are: " + schema.getTypes());
            case STRING:
                try {
                    if (!StringUtils.isEmpty(field.getProp("logicalType"))
                            && field.getProp("logicalType").toLowerCase().equalsIgnoreCase("timestamp")) {
                        if (datum.isEmpty()) {
                            pst.setNull(index, Types.TIMESTAMP);
                        } else {
                            pst.setTimestamp(index, new Timestamp(Long.parseLong(datum)));
                        }
                    } else {
                        if (datum.isEmpty()) {
                            pst.setNull(index, Types.VARCHAR);
                        } else {
                            pst.setString(index, datum);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case BYTES:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.BINARY);
                } else {
                    pst.setString(index, datum);
                }
                break;
            case INT:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.INTEGER);
                } else {
                    pst.setInt(index, Integer.parseInt(datum));
                }
                break;
            case LONG:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.BIGINT);
                } else {
                    pst.setLong(index, Long.parseLong(datum));
                }
                break;
            case FLOAT:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.FLOAT);
                } else {
                    pst.setFloat(index, Float.parseFloat(datum));
                }
                break;
            case DOUBLE:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.DOUBLE);
                } else {
                    pst.setDouble(index, Double.parseDouble(datum));
                }
                break;
            case BOOLEAN:
                if (datum.isEmpty()) {
                    pst.setNull(index, Types.BOOLEAN);
                } else {
                    pst.setBoolean(index, Boolean.parseBoolean(datum));
                }
                break;
            case NULL:
                if (datum == null || datum.isEmpty()) {
                    pst.setString(index, null);
                    break;
                } else {
                    throw new RuntimeException("datum is not NULL.");
                }
            default:
                throw new RuntimeException("Invalid type, only STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL are permitted.");
        }
        return pst;
    }
}
