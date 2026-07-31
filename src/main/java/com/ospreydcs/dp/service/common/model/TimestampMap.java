package com.ospreydcs.dp.service.common.model;

import java.util.*;

public class TimestampMap<T> {

    // instance variables
    final protected Map<Long, Map<Long, T>> timestampMap = new TreeMap<>();

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

    /**
     * Removes the value at the given timestamp, dropping the enclosing per-second map once it is
     * empty.
     *
     * <p>Exists so a consumer that materializes this map into another representation can release
     * each entry as it is consumed, rather than holding the whole map live until the second copy is
     * complete (issue #199). Callers that need the map afterward must not use this.
     *
     * @return the removed value, or null if nothing was stored at that timestamp
     */
    public T remove(long seconds, long nanos) {
        final Map<Long, T> secondMap = timestampMap.get(seconds);
        if (secondMap == null) {
            return null;
        }
        final T removed = secondMap.remove(nanos);
        if (secondMap.isEmpty()) {
            timestampMap.remove(seconds);
        }
        return removed;
    }

    /** True when no values remain. */
    public boolean isEmpty() {
        return timestampMap.isEmpty();
    }

    public int size() {
        int entryCount = 0;
        for (Map.Entry<Long, Map<Long, T>> entry : timestampMap.entrySet()) {
            entryCount = entryCount + entry.getValue().size();
        }
        return entryCount;
    }

}
