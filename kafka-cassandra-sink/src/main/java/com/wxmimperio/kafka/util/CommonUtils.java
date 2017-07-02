package com.wxmimperio.kafka.util;

import java.util.Collections;
import java.util.List;

/**
 * Created by wxmimperio on 2017/7/2.
 */
public class CommonUtils {

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
