package com.wxmimperio.kafka;

import com.wxmimperio.kafka.common.WriteType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {

        String topics = args[0];
        String start = args[1];
        String end = args[2];
        WriteType writeType = WriteType.valueOf(args[3].toUpperCase(Locale.ENGLISH));
        String path = args[4];
        // String start = "2017-10-18 12:00:00";
        // String end = "2017-10-18 13:00:00";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long startTS = Timestamp.valueOf(LocalDateTime.parse(start, formatter)).getTime();
        long endTS = Timestamp.valueOf(LocalDateTime.parse(end, formatter)).getTime();

        ConsumerByTimestamp consumerByTimestamp = new ConsumerByTimestamp();

        for (String topic : topics.split(",")) {
            LOG.info("Start processing " + topic);
            consumerByTimestamp.export(topic, startTS, endTS, writeType, path);
        }
    }
}
