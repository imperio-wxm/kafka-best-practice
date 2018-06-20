package com.wxmimperio.kafka.util;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by weiximing.imperio on 2017/3/3.
 */
public class CassandraUtil {

    /**
     * cassandrabuilder key words
     */
    private static final String[] cassandraKeyword = {
            "ADD", "ALL", "ALLOW", "ALTER", "AND", "ANY", "APPLY", "ASC", "AS",
            "ASCII", "AUTHORIZE", "BATCH", "BEGIN", "BIGINT", "BLOB", "BOOLEAN", "BY", "CLUSTERING",
            "COLUMNFAMILY", "COMPACT", "CONSISTENCY", "COUNT", "COUNTER", "CREATE", "CUSTOM",
            "DECIMAL", "DELETE", "DESC", "DISTINCT", "DOUBLE", "DROP", "EACH_QUORUM", "EXISTS",
            "FILTERING", "FLOAT", "FROM", "FROZEN", "FULL", "GRANT", "IF", "IN", "INDEX", "INET",
            "INFINITY", "INSERT", "INT", "INTO", "KEY", "KEYSPACE", "KEYSPACES", "LEVEL", "LIMIT",
            "LIST", "LOCAL_ONE", "LOCAL_QUORUM", "MAP", "MODIFY", "NAN", "NORECURSIVE", "NOSUPERUSER",
            "NOT", "OF", "ON", "ONE", "ORDER", "PASSWORD", "PERMISSION", "PERMISSIONS", "PRIMARY",
            "QUORUM", "RENAME", "REVOKE", "SCHEMA", "SELECT", "SET", "STATIC", "STORAGE", "SUPERUSER",
            "TABLE", "TEXT", "TIMESTAMP", "TIMEUUID", "THREE", "TO", "TOKEN", "TRUNCATE", "TTL", "TUPLE",
            "TWO", "TYPE", "UNLOGGED", "UPDATE", "USE", "USER", "USERS", "USING", "UUID", "VALUES",
            "VARCHAR", "VARINT", "WHERE", "WITH", "WRITETIME"
    };

    /**
     * Change string to obj by schema
     *
     * @param gr
     * @return
     */
    public static List<Object> changeStrToObject(GenericRecord gr) throws Exception {
        List<Object> objList = Lists.newArrayList();

        int count = 0;
        for (Schema.Field field : gr.getSchema().getFields()) {
            if (count == gr.getSchema().getFields().size()) {
                break;
            }
            objList.add(parse(field.schema(), gr.get(field.name()) == null ? "" : gr.get(field.name()).toString()));
            count++;
        }
        return objList;
    }

    /**
     * cassandra keyword add quotation
     *
     * @param filed
     * @return
     */
    public static String keyWordChange(String filed) {
        int isExit = Arrays.binarySearch(cassandraKeyword, filed.toUpperCase());
        if (isExit >= 0) {
            filed = addQuotation(filed);
        }
        return filed;
    }

    private static String addQuotation(String filed) {
        return "\"" + filed + "\"";
    }

    /**
     * @param schema
     * @param datum
     * @return
     * @throws Exception
     */
    private static Object parse(Schema schema, String datum) throws Exception {
        switch (schema.getType()) {
            case UNION:
                for (Schema type : schema.getTypes()) {
                    try {
                        return parse(type, datum);
                    } catch (Exception e) {
                        continue;
                    }
                }
                throw new Exception(
                        "No match type for the datum: " + datum + ", the valid types are: " + schema.getTypes());
            case STRING:
                return datum;
            case BYTES:
                return Arrays.toString(datum.getBytes(Charsets.UTF_8));
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

    /**
     * split time
     *
     * @param cal
     * @param splitMinute
     * @return
     * @throws Exception
     */
    public static String minuteHourChange(Calendar cal, int splitMinute) throws Exception {
        //所有时间统一为cal的second、millisecond（为了计算差值，精确到millisecond）
        int second = cal.get(Calendar.SECOND);
        int millisecond = cal.get(Calendar.MILLISECOND);

        //baseTime，即以当前小时，零分为基准，根据splitMinute计算时间起始点
        long nowTimeStamp = cal.getTimeInMillis();
        int baseMinute = (cal.get(Calendar.MINUTE) / splitMinute) * splitMinute;
        int baseHour = cal.get(Calendar.HOUR_OF_DAY);
        int baseDay = cal.get(Calendar.DAY_OF_MONTH);
        int baseMonth = cal.get(Calendar.MONTH) + 1;
        int baseYear = cal.get(Calendar.YEAR);

        if (baseMinute > 60) {
            baseHour += 1;
        }

        String baseTime = baseYear + "-" + CassandraUtil.addZero(baseMonth, 2) + "-" + CassandraUtil.addZero(baseDay, 2) +
                " " + CassandraUtil.addZero(baseHour, 2) + ":" + CassandraUtil.addZero((cal.get(Calendar.MINUTE) / splitMinute) * splitMinute, 2)
                + ":" + CassandraUtil.addZero(second, 2) + ":" + millisecond;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
        //System.out.println("base time = " + baseTime);
        Date baseDate = sdf.parse(baseTime);
        Calendar baseCal = new GregorianCalendar();
        baseCal.setTime(baseDate);

        //delayTime为以baseTime为基准，计算splitMinute为间隔的下一次时间点
        int hour = (baseCal.get(Calendar.HOUR_OF_DAY) + ((baseCal.get(Calendar.MINUTE) + splitMinute) / 60));
        int minute = (baseCal.get(Calendar.MINUTE) + splitMinute) % 60;
        int day = baseCal.get(Calendar.DAY_OF_MONTH) + hour / 24;
        int month = baseCal.get(Calendar.MONTH) + 1;
        int year = baseCal.get(Calendar.YEAR);

        if (day > baseCal.getActualMaximum(Calendar.DAY_OF_MONTH)) {
            day = day % baseCal.getActualMaximum(Calendar.DAY_OF_MONTH);
            month = (baseCal.get(Calendar.MONTH) + 1) + (day % baseCal.getActualMaximum(Calendar.DAY_OF_MONTH));
            if (month > (baseCal.getActualMaximum(Calendar.MONTH) + 1)) {
                month = 1;
                year = baseCal.get(Calendar.YEAR) + (month % baseCal.getActualMaximum(Calendar.MONTH));
            }
        }

        if (hour >= 24) {
            hour = 0;
        }

        String delayTime = CassandraUtil.addZero(year, 2) + "-" + CassandraUtil.addZero(month, 2) + "-" + CassandraUtil.addZero(day, 2) +
                " " + CassandraUtil.addZero(hour, 2) + ":" + CassandraUtil.addZero(minute, 2) + ":" + CassandraUtil.addZero(second, 2) + ":" + millisecond;
        //System.out.println("delay time = " + delayTime);
        Date delayDate = sdf.parse(delayTime);
        Calendar delayCal = new GregorianCalendar();
        delayCal.setTime(delayDate);

        long delayTimeStamp = delayCal.getTimeInMillis();

        //最终归类时间：下一次间隔时间 - 当前时间，如果大于splitMinute则进入下一次时间间隔，否则留在当前时间
        String doneTime;
        if ((delayTimeStamp - nowTimeStamp) >= splitMinute * 60 * 1000) {
            doneTime = baseTime;
        } else {
            doneTime = delayTime;
        }
        return doneTime;
    }

    private static String addZero(int num, int len) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(num);
        while (stringBuilder.length() < len) {
            stringBuilder.insert(0, "0");
        }
        return stringBuilder.toString();
    }
}
