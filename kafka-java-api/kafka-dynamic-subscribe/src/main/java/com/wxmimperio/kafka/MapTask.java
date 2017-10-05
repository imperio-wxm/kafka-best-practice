package com.wxmimperio.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by wxmimperio on 2017/6/29.
 */
public class MapTask implements Runnable{

    private Map map;

    public MapTask(Map map) {
        this.map = map;
    }

    @Override
    public void run() {
        map.put(Thread.currentThread().getName(),Thread.currentThread().getId());
        final String id = Thread.currentThread().getName();
        try {
            Thread.sleep(1000);
            map.remove(id);
            new Timer().schedule(new TimerTask() {
                public void run() {
                    map.put(Thread.currentThread().getName(),Thread.currentThread().getId());
                    System.out.println(map);
                }
            }, 0, 2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
