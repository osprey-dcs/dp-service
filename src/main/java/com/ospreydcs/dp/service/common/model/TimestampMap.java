package com.ospreydcs.dp.service.common.model;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TimestampMap<T> {

    final private Map<Long, Map<Long, T>> timestampMap = new TreeMap<>();

    public void put(long seconds, long nanos, T value) {
        Map<Long, T> secondMap = timestampMap.get(seconds);
        if (secondMap ==  null) {
            secondMap = new TreeMap<>();
            timestampMap.put(seconds, secondMap);
        }
        secondMap.put(nanos, value);
    }

    public T get(long seconds, long nanos) {
        Map<Long, T> secondMap = timestampMap.get(seconds);
        if (secondMap == null) {
            return null;
        }
        return secondMap.get(nanos);
    }

    public Set<Map.Entry<Long, Map<Long, T>>> entrySet() {
        return timestampMap.entrySet();
    }

}
