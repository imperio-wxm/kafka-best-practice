package com.wxmimperio.kafka;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by wxmimperio on 2017/7/1.
 */
public class DemoTest {

    @Test
    public void demo1() {
        List<String> list = new ArrayList<String>() {
            {
                add("1");
                add("2");
                add("1");
                add("2");
                add("1");
                add("2");
                add("1");
                add("2");
            }
        };
        System.out.println(list);
        cleanList(list);
        System.out.println(list);

    }

    @Test
    public void demo2() {
        List<String> list1 = Arrays.asList("table1", "table2", "table3");
        List<String> list2 = Arrays.asList("table2", "table1", "table3");
        System.out.println(compareList(list1, list2));
    }

    private void cleanList(List list) {
        list.clear();
    }

    public static <T extends Comparable<T>> boolean compareList(List<T> firstList, List<T> secondList) {
        if (firstList.size() != secondList.size())
            return false;
        Collections.sort(firstList);
        Collections.sort(secondList);
        for (int i = 0; i < firstList.size(); i++) {
            if (!firstList.get(i).equals(secondList.get(i)))
                return false;
        }
        return true;
    }
}

