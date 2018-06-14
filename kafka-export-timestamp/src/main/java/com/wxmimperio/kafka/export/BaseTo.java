package com.wxmimperio.kafka.export;

import java.io.IOException;
import java.util.Map;

public interface BaseTo {
    void close() throws IOException;

    void initWriter(String topicName, String path) throws IOException;

    void writeTo(Map<String, String> messages);
}
