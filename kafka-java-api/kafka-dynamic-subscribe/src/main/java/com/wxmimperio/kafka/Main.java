package com.wxmimperio.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wxmimperio on 2017/6/29.
 */
public class Main {

    private static String cacheTopic;
    private static long lastModified = 0;

    public static void main(String[] args) {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        final ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();

       /* new Timer().schedule(new TimerTask() {
            public void run() {
                //ResourceBundle resource = ResourceBundle.getBundle("test");
                //String topics = resource.getString("topics");
                String topics = readFileByLines("E:\\coding\\github\\kafka-best-practice\\kafka-java-api\\kafka-dynamic-subscribe\\src\\main\\resources\\test.properties");
                if (isFileUpdated("E:\\coding\\github\\kafka-best-practice\\kafka-java-api\\kafka-dynamic-subscribe\\src\\main\\resources\\test.properties")) {
                    System.out.println(topics);
                    subscribedTopics.addAll(Arrays.asList(topics.split(",", -1)));
                    cacheTopic = topics;
                    System.out.println("刷新");
                }
            }
        }, 0, 5000);*/

        for (int i = 0; i < 2; i++) {
            cachedThreadPool.submit(new ConsumerRunner(subscribedTopics, cacheTopic));
        }
    }

    public static boolean isFileUpdated(String filename) {
        File file = new File(filename);
        if (file.isFile()) {
            long lastUpdateTime = file.lastModified();
            if (lastUpdateTime > lastModified) {
                System.out.println("The properties file was modified.");
                lastModified = lastUpdateTime;
                return true;
            } else {
                System.out.println("The properties file was not modified.");
                return false;
            }
        } else {
            System.out.println("The path does not point to a file.");
            return false;
        }
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
