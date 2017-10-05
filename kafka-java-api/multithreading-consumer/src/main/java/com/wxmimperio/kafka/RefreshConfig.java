package com.wxmimperio.kafka;

import com.sun.xml.internal.bind.v2.runtime.output.SAXOutput;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by weiximing.imperio on 2017/4/25.
 */
public class RefreshConfig implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(RefreshConfig.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        maybeRefreshConfig();
    }

    private void maybeRefreshConfig() {
        String topic = readFileByLines(
                "E:\\coding\\github\\kafka-best-practice\\kafka-java-api\\multithreading-consumer\\src\\main\\resources\\test.properties");

        List<String> newSchemaTopics = getTopicList(topic.split(",", -1));

        LOG.info("Old list = " + Arrays.asList(GlobalConsumer.getInstance().getTopics().get("schema")));
        LOG.info("New list = " + newSchemaTopics);

        List<String> oldSchemaTopics = getTopicList(GlobalConsumer.getInstance().getTopics().get("schema"));
        List<String> oldSchemaTemp = getTopicList(GlobalConsumer.getInstance().getTopics().get("schema"));

        // schema
        refresh(oldSchemaTemp, oldSchemaTopics, newSchemaTopics, "schema");
    }

    private void refresh(List<String> tempTopics, List<String> oldTopics, List<String> newTopics, String type) {
        if (!compare(tempTopics, newTopics)) {
            tempTopics.retainAll(newTopics);
            for (String topic : newTopics) {
                if (!tempTopics.contains(topic) && !topic.isEmpty()) {
                    startConsumer(type, topic);
                    LOG.info("add topic = {}", topic);
                }
            }
            for (String topic : oldTopics) {
                if (!tempTopics.contains(topic) && !topic.isEmpty()) {
                    closeConsumer(type, topic);
                    LOG.info("remove topic = {}", topic);
                }
            }
            // update cache topics
            GlobalConsumer.getInstance().getTopics().put(type, newTopics.toArray(new String[newTopics.size()]));
        }
    }

    /**
     * close consumer by type
     *
     * @param type
     * @param topic
     */
    private void closeConsumer(String type, String topic) {
        if (type.equalsIgnoreCase("schema")) {
            close(topic, type);
        }
    }

    /**
     * close consumer and update topic list
     *
     * @param topic
     * @param type
     */
    private void close(String topic, String type) {
        System.out.println("关闭了" + topic);
        Map<String, ConsumerTask> consumerTaskMap = GlobalConsumer.getInstance().getConsumerMap().get(type);
        System.out.println(consumerTaskMap);
        consumerTaskMap.get(topic).shutdown();
        consumerTaskMap.remove(topic);
    }

    /**
     * start consumer by type
     *
     * @param type
     * @param topic
     */
    private void startConsumer(String type, String topic) {
        ExecutorService executor = Executors.newCachedThreadPool();

        Map<String, ConsumerTask> consumerTaskMap = GlobalConsumer.getInstance().getConsumerMap().get(type);

        if (type.equalsIgnoreCase("schema")) {
            System.out.println("启动了" + topic);
            final ConsumerTask consumerTask = new ConsumerTask(topic);
            consumerTaskMap.put(topic, consumerTask);
            executor.submit(consumerTask);
        }
    }


    public static <T extends Comparable<T>> boolean compare(List<T> a, List<T> b) {
        if (a.size() != b.size())
            return false;
        Collections.sort(a);
        Collections.sort(b);
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equals(b.get(i)))
                return false;
        }
        return true;
    }

    private List<String> getTopicList(String[] topics) {
        List<String> list = new ArrayList<>();
        for (String topic : topics) {
            list.add(topic);
        }
        return list;
    }

    public static String readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String topics = "";
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                topics = tempString.split("=", -1)[1];
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return topics;
    }
}
