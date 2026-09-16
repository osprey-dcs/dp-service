package com.ospreydcs.dp.service.query.handler.mongo.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A group of one request's PVs whose {@code pvStats} spans fall in the same power-of-two class,
 * carrying the largest span actually present in the group (issue #274, plan D11).
 *
 * <p>The #232 {@code firstTime.seconds >= begin - span} bound was a single maximum over every PV a
 * request named, and the overlap residual is evaluated after the document fetch (measured: keys
 * examined equals documents examined), so one long-span PV in a request made every other PV's scan
 * fetch {@code longSpan} worth of history it then discarded. Grouping PVs by span class and issuing
 * one find per class, each bounded by its own maximum, keeps every PV's scan within a factor of two
 * of its own ideal while bounding the number of finds by the number of non-empty classes -- one to
 * three in practice, at most a few dozen for spans up to years.
 *
 * <p>Class assignment: class 0 holds spans of at most one second (which includes every PV with no
 * {@code pvStats} document, whose bound is {@code begin} itself, #232 plan D6); class {@code k}
 * holds {@code 2^(k-1) < span <= 2^k}. The bound a class carries is {@link #maxSpanSeconds}, the
 * largest span among its members, never the class ceiling.
 *
 * @param pvNames        the PVs in this class, in the order they were first named by the request
 * @param maxSpanSeconds the largest {@code pvStats} span among {@code pvNames}; non-negative
 */
public record SpanClass(List<String> pvNames, long maxSpanSeconds) {

    public SpanClass {
        if (pvNames == null || pvNames.isEmpty()) {
            throw new IllegalArgumentException("a SpanClass must name at least one PV");
        }
        if (maxSpanSeconds < 0) {
            throw new IllegalArgumentException("maxSpanSeconds must be non-negative: " + maxSpanSeconds);
        }
        pvNames = List.copyOf(pvNames);
    }

    /**
     * The power-of-two class index for a span: 0 for spans of at most one second, otherwise the
     * {@code k} with {@code 2^(k-1) < spanSeconds <= 2^k}.
     */
    public static int classIndex(long spanSeconds) {
        if (spanSeconds <= 1) {
            return 0;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(spanSeconds - 1);
    }

    /**
     * Partitions {@code pvNames} into span classes by each PV's own span from {@code spanByPv}
     * (a PV absent from the map has span 0), ordered by class index. Duplicate names are folded;
     * an empty name list yields an empty partition.
     */
    public static List<SpanClass> partition(Collection<String> pvNames, Map<String, Long> spanByPv) {
        final Map<Integer, List<String>> namesByClass = new TreeMap<>();
        final Map<Integer, Long> maxByClass = new TreeMap<>();
        for (String pvName : new LinkedHashSet<>(pvNames)) {
            final long span = Math.max(0L, spanByPv.getOrDefault(pvName, 0L));
            final int classIndex = classIndex(span);
            namesByClass.computeIfAbsent(classIndex, k -> new ArrayList<>()).add(pvName);
            maxByClass.merge(classIndex, span, Math::max);
        }
        final List<SpanClass> classes = new ArrayList<>(namesByClass.size());
        for (Map.Entry<Integer, List<String>> entry : namesByClass.entrySet()) {
            classes.add(new SpanClass(entry.getValue(), maxByClass.get(entry.getKey())));
        }
        return classes;
    }
}
