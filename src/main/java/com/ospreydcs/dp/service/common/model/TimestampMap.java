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

    /**
     * Removes every value at or after {@code (seconds, nanos)}, returning how many were removed.
     *
     * <p>Exists for the querySamples slice retry (issue #274, plan D3): a slice whose drain
     * tripped the byte budget is discarded whole, because under the PV-major cursor order none of
     * its timestamps is known to be complete across PVs. The seconds map is a {@code TreeMap}, so
     * everything strictly after the boundary second goes in one {@code tailMap().clear()}; the
     * boundary second itself is trimmed by nanos and dropped if it empties.
     */
    public int removeFrom(long seconds, long nanos) {
        int removed = 0;
        final TreeMap<Long, Map<Long, T>> secondsMap = (TreeMap<Long, Map<Long, T>>) timestampMap;
        final Map<Long, T> boundarySecond = secondsMap.get(seconds);
        if (boundarySecond != null) {
            final TreeMap<Long, T> nanosMap = (TreeMap<Long, T>) boundarySecond;
            final Map<Long, T> tail = nanosMap.tailMap(nanos, true);
            removed += tail.size();
            tail.clear();
            if (nanosMap.isEmpty()) {
                secondsMap.remove(seconds);
            }
        }
        final Map<Long, Map<Long, T>> laterSeconds = secondsMap.tailMap(seconds, false);
        for (Map<Long, T> secondMap : laterSeconds.values()) {
            removed += secondMap.size();
        }
        laterSeconds.clear();
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
