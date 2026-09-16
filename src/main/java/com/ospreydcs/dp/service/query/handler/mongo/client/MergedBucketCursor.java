package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Merges the cursors of a span-class-partitioned bucket query back into one stream in the
 * {@code (pvName, firstTime.seconds, firstTime.nanos)} order every bucket query sorts by (issue
 * #274, plan D11).
 *
 * <p>Each input cursor is already in that order (it is the hinted index's order), and the classes
 * hold disjoint PV sets, so the merge is a head comparison across the inputs: the smallest head is
 * emitted and that cursor is advanced. Inputs are read lazily -- nothing is fetched until the first
 * {@link #hasNext()} -- so a consumer that stops early (the unary buckets page) leaves the other
 * cursors' batches unread, and {@link #close()} releases every input.
 *
 * <p>A document with no {@code dataTimestamps} sorts first within its PV rather than throwing here:
 * the dispatchers already classify such a document as a {@code DpException} when they deserialize
 * it, which is where a malformed bucket must surface (see CLAUDE.md, "Bucket Deserialization Must
 * Fail as DpException"); a throw from the cursor would escape that classification.
 *
 * <p>Not thread-safe, like the cursors it wraps. Wrapped in {@code TimedMongoCursor} by the client
 * so the merge and the inputs' time are charged to the request's {@code db} stage together.
 */
public class MergedBucketCursor implements MongoCursor<BucketDocument> {

    private final List<MongoCursor<BucketDocument>> cursors;
    private final BucketDocument[] heads;
    private boolean primed = false;
    private boolean closed = false;

    public MergedBucketCursor(List<MongoCursor<BucketDocument>> cursors) {
        if (cursors == null || cursors.isEmpty()) {
            throw new IllegalArgumentException("MergedBucketCursor requires at least one cursor");
        }
        this.cursors = new ArrayList<>(cursors);
        this.heads = new BucketDocument[this.cursors.size()];
    }

    private void prime() {
        if (primed) {
            return;
        }
        primed = true;
        for (int i = 0; i < cursors.size(); i++) {
            advance(i);
        }
    }

    private void advance(int index) {
        final MongoCursor<BucketDocument> cursor = cursors.get(index);
        heads[index] = cursor.hasNext() ? cursor.next() : null;
    }

    /** Index of the smallest head, or -1 when every input is exhausted. */
    private int smallestHead() {
        int best = -1;
        for (int i = 0; i < heads.length; i++) {
            if (heads[i] == null) {
                continue;
            }
            if (best < 0 || compare(heads[i], heads[best]) < 0) {
                best = i;
            }
        }
        return best;
    }

    /**
     * Orders two buckets the way the server's {@code (pvName, firstTime)} sort does.
     *
     * <p>The name comparison assumes PV names are ASCII. MongoDB sorts strings by raw UTF-8 bytes
     * (no collation is configured on this collection or its sort), which agrees with Java's
     * UTF-16 code-unit order for ASCII and the whole BMP, and diverges only for supplementary-plane
     * characters. That matters here because the keyset page resume filters {@code pvName > last}
     * server-side: if this merge ever disagreed with the server's order, a continuation page could
     * skip or repeat a PV -- a silent wrong answer rather than an error.
     */
    static int compare(BucketDocument a, BucketDocument b) {
        final int byName = nullFirst(a.getPvName()).compareTo(nullFirst(b.getPvName()));
        if (byName != 0) {
            return byName;
        }
        final TimestampDocument firstA = a.getDataTimestamps() == null ? null : a.getDataTimestamps().getFirstTime();
        final TimestampDocument firstB = b.getDataTimestamps() == null ? null : b.getDataTimestamps().getFirstTime();
        final long secsA = firstA == null ? Long.MIN_VALUE : firstA.getSeconds();
        final long secsB = firstB == null ? Long.MIN_VALUE : firstB.getSeconds();
        if (secsA != secsB) {
            return Long.compare(secsA, secsB);
        }
        final long nanosA = firstA == null ? Long.MIN_VALUE : firstA.getNanos();
        final long nanosB = firstB == null ? Long.MIN_VALUE : firstB.getNanos();
        return Long.compare(nanosA, nanosB);
    }

    private static String nullFirst(String s) {
        return s == null ? "" : s;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        prime();
        return smallestHead() >= 0;
    }

    @Override
    public BucketDocument next() {
        if (closed) {
            throw new NoSuchElementException("cursor is closed");
        }
        prime();
        final int index = smallestHead();
        if (index < 0) {
            throw new NoSuchElementException();
        }
        final BucketDocument result = heads[index];
        advance(index);
        return result;
    }

    @Override
    public BucketDocument tryNext() {
        return hasNext() ? next() : null;
    }

    /** Closes every input, continuing past a failure so no cursor is left open behind another's error. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException first = null;
        for (MongoCursor<BucketDocument> cursor : cursors) {
            try {
                cursor.close();
            } catch (RuntimeException ex) {
                if (first == null) {
                    first = ex;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /** Documents already fetched from the inputs: the sum of their batches plus the held heads. */
    @Override
    public int available() {
        int available = 0;
        for (int i = 0; i < cursors.size(); i++) {
            available += cursors.get(i).available() + (heads[i] == null ? 0 : 1);
        }
        return available;
    }

    /**
     * Unsupported, inheriting {@link java.util.Iterator}'s throwing default. Deliberately different
     * from the sibling {@code TimedMongoCursor}, which delegates {@code remove()} because it is a
     * transparent decorator over one cursor; a merge has no single underlying cursor to remove
     * from. No bucket-query caller invokes it.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported on a merged bucket cursor");
    }

    /** No single server cursor stands for a merge; null, as an exhausted driver cursor reports. */
    @Override
    public ServerCursor getServerCursor() {
        return null;
    }

    @Override
    public ServerAddress getServerAddress() {
        return cursors.get(0).getServerAddress();
    }
}
