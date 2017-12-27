package com.wxmimperio.kafka;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author xiejing.kane
 */
public class SchemaHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaHelper.class);
    private static Map<String, Schema> schemaCache = Maps.newHashMap();
    private static Map<String, String> aliasCache = Maps.newHashMap();


    private static void printDifference(Map<String, Schema> oldSchemas, Map<String, Schema> newSchemas) {
        for (Entry<String, Schema> entry : newSchemas.entrySet()) {
            if (!oldSchemas.containsKey(entry.getKey())) {
                //LOG.info("Add " + entry.getKey() + ", Schema = " + entry.getValue().toString());
            } else {
                String newSchema = entry.getValue().toString();
                String oldSchema = oldSchemas.get(entry.getKey()).toString();
                if (!newSchema.equals(oldSchema)) {
                    LOG.info("Update " + entry.getKey() + ", Schema = " + newSchema);
                }
            }
        }
    }

    public static Schema getSchema(String schemaName) throws Exception {
        if (schemaCache.containsKey(schemaName)) {
            return schemaCache.get(schemaName);
        } else {
            throw new Exception("Schema : " + schemaName + " doesn't exist!");
        }
    }

    public static String getSchemaNameByAlias(String alias) {
        return aliasCache.containsKey(alias) ? aliasCache.get(alias) : alias;
    }

    public static GenericRecord format(JsonObject json, Schema schema) throws Exception {
        return format(json, schema, false);
    }

    public static GenericRecord format(JsonObject json,  Schema schema, boolean autoFillDefault) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        for (Field field : schema.getFields()) {
            String jsonKey = field.name();
            String datum = null;
            if (json.has(jsonKey)) {
                JsonElement ele = json.get(jsonKey);
                datum = ele.isJsonNull() ? null : ele.getAsString();
            } else {
                JsonNode defaultValue = field.defaultValue();
                if (defaultValue != null) {
                    datum = defaultValue.getValueAsText();
                } else if (autoFillDefault && field.schema().getType().equals(Schema.Type.STRING)) {
                    datum = "";
                } else {
                    LOG.debug(jsonKey + " has no value and no default value in Schema: " + schema.getName());
                }
            }
            record.put(field.name(), parse(field.schema(), schema.getName(), field.name(), datum));
        }
        return record;
    }

    /**
     * @param lines
     * @param schemaName
     * @return
     * @throws Exception
     */
    public static GenericRecord format(String[] lines, String schemaName) throws Exception {
        return format(lines, 0, schemaName);
    }

    /**
     * @param lines
     * @param schemaName
     * @return
     * @throws Exception
     */
    public static GenericRecord format(String[] lines, int startIndex, String schemaName) throws Exception {
        Schema schema = getSchema(schemaName);
        GenericRecord record = new GenericData.Record(schema);
        for (Field field : schema.getFields()) {
            if (startIndex < lines.length) {
                record.put(field.pos(), parse(field.schema(), schemaName, field.name(), lines[startIndex]));
                startIndex++;
            } else {
                LOG.debug("Log doesn't match the schema exactly, lines = " + lines.length + ", fields = "
                        + schema.getFields().size());
                break;
            }
        }
        return record;
    }

    public static byte[] serialize(GenericRecord record, Schema schema) throws Exception {
        DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        userDatumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();
        return outputStream.toByteArray();
    }

    /**
     * @param schema
     * @param datum
     * @return
     * @throws Exception
     */
    public static Object parse(Schema schema, String schemaName, String fieldName, String datum) throws Exception {
        switch (schema.getType()) {
            case UNION:
                for (Schema type : schema.getTypes()) {
                    try {
                        return parse(type, schemaName, fieldName, datum);
                    } catch (Exception e) {
                        continue;
                    }
                }
            case STRING:
                return datum;
            case BYTES:
                return datum.getBytes(Charsets.UTF_8);
            case INT:
                return datum.isEmpty() ? 0 : Integer.parseInt(datum);
            case LONG:
                return datum.isEmpty() ? 0L : Long.parseLong(datum);
            case FLOAT:
                return datum.isEmpty() ? 0.0F : Float.parseFloat(datum);
            case DOUBLE:
                return datum.isEmpty() ? 0D : Double.parseDouble(datum);
            case BOOLEAN:
                return Boolean.parseBoolean(datum);
            case NULL:
                if (datum == null || datum.isEmpty()) {
                    return null;
                } else {
                    throw new Exception("datum is not NULL.");
                }
            default:
                throw new Exception(
                        "Invalid type, only STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL are permitted.");
        }
    }
}
