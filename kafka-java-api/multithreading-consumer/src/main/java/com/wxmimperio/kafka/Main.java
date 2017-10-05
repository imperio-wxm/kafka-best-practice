package com.wxmimperio.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wxmimperio on 2017/7/23.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        initQuartz();
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        String[] topicStr = "flink_demo1".split(",", -1);

        GlobalConsumer.getInstance().getTopics().put("schema", topicStr);
        Map<String, ConsumerTask> consumerTaskMap = new HashMap<>();

        for (String topic : topicStr) {
            ConsumerTask consumerTask = new ConsumerTask(topic);
            consumerTaskMap.put(topic, consumerTask);

            cachedThreadPool.submit(consumerTask);
        }
        GlobalConsumer.getInstance().getConsumerMap().put("schema", consumerTaskMap);
    }

    private static void initQuartz() {
        QuartzManager quartzUtil = QuartzManager.getInstance("test", "test");
        try {
            //add quartz job for change txt to sequenceFile
            quartzUtil.addJob(
                    String.valueOf(quartzUtil.hashCode()) + "_tran_file",
                    quartzUtil.toString() + "_tran_file",
                    RefreshConfig.class,
                    "*/3 * * * * ?",
                    null
            );
        } catch (Exception e) {
            LOG.error("Create quartz job error, ", e);
            quartzUtil.shutdown();
        }
    }
}
