package com.wxmimperio.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by weiximing.imperio on 2017/4/25.
 */
public class GlobalConsumer {
    // Map<WriteType,Map<topic,consume>>
    private Map<String, Map<String, ConsumerTask>> consumerMap;
    // Map<WriteType,TopicStr>
    private Map<String, String[]> topics;

    private ExecutorService executor;


    private static class SingletonHolder {
        private static final GlobalConsumer INSTANCE = new GlobalConsumer();
    }

    public static GlobalConsumer getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private GlobalConsumer() {
        this.consumerMap = new ConcurrentHashMap<String, Map<String, ConsumerTask>>();
        this.topics = new ConcurrentHashMap<String, String[]>();
        this.executor = Executors.newCachedThreadPool();
    }

    public Map<String, Map<String, ConsumerTask>> getConsumerMap() {
        return consumerMap;
    }

    public Map<String, String[]> getTopics() {
        return topics;
    }

    public ExecutorService getExecutor() {
        return executor;
    }
}
