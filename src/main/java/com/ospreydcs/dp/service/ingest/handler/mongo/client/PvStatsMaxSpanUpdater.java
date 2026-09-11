package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains {@code pvStats.maxBucketSpanSeconds} for the PVs an ingestion batch writes (#232).
 *
 * <p>The statistic is the maximum over a PV's stored buckets of
 * {@code lastTime.seconds - firstTime.seconds}, and the query side uses the maximum over a
 * request's PVs as the {@code firstTime} lower bound of its bucket overlap filter. It is only ever
 * raised, via {@code $max}, so a stored value can never be too small for the buckets it covers.
 *
 * <p><b>Stats are written before the buckets, and a failed stats write fails the request.</b>
 * Inside {@code insertBatch}, the {@code pvStats} update precedes {@code insertMany}. At every
 * instant, the stored maximum for a PV is at least the span of every stored bucket for that PV: a
 * stats write that succeeds without the bucket insert over-states, which errs safe (a bound at
 * least as wide as needed). A stats write that fails surfaces as a {@link DpException}, the caller
 * returns an error result and no buckets are inserted; the request is reported as an error like any
 * other database failure. Buckets first would let a query between the two writes miss the new
 * bucket; fire-and-forget would leave an uncovered bucket as a silent wrong answer on every later
 * query.
 *
 * <p><b>A per-process high-watermark cache gates the write.</b> A PV is written only if the
 * request's span exceeds its cached value or it has no cached value. The cache is advanced only
 * after the bulk write succeeds, so a cached value is always one this process has stored. Skipping
 * is safe across processes because {@code $max} is monotone: a peer can only have raised the stored
 * value. Steady-state cost for uniform per-PV spans is zero extra writes; first sight of a PV costs
 * one upsert in an unordered bulk. If the bulk throws, no entry is advanced (a partially applied
 * bulk is re-written harmlessly next time).
 *
 * <p>The cache is unbounded and not pre-warmed. PV counts of 10^4 to 10^5 make it megabytes at
 * most; eviction is safe (a miss just writes) and can be added later without changing semantics.
 *
 * <p>Safe for concurrent use by the ingestion handler's worker threads: the cache is a
 * {@link ConcurrentHashMap}, and two threads writing the same PV at once both issue a monotone
 * {@code $max}, so neither can lower what the other stored.
 *
 * <p>Operates on raw {@link Document}s rather than {@code PvStatsDocument}: the update touches one
 * field by name and never encodes or decodes a whole document.
 */
public class PvStatsMaxSpanUpdater {

    private static final Logger logger = LogManager.getLogger();

    private static final BulkWriteOptions UNORDERED = new BulkWriteOptions().ordered(false);
    private static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);

    private final MongoCollection<Document> pvStatsCollection;

    // high-watermark cache: pvName -> largest span this process has successfully stored for it
    private final ConcurrentHashMap<String, Long> maxSpanSecondsByPvName = new ConcurrentHashMap<>();

    public PvStatsMaxSpanUpdater(MongoCollection<Document> pvStatsCollection) {
        this.pvStatsCollection = pvStatsCollection;
    }

    /**
     * Raises {@code maxBucketSpanSeconds} to at least {@code spanSeconds} for each named PV whose
     * cached watermark does not already cover it. Issues at most one unordered bulk write.
     *
     * @param pvNames     PV names of the buckets about to be inserted; duplicates are collapsed
     * @param spanSeconds {@code lastTime.seconds - firstTime.seconds} of those buckets, non-negative
     * @throws DpException if the bulk write throws or is not acknowledged; nothing is cached
     */
    public void recordSpan(Collection<String> pvNames, long spanSeconds) throws DpException {

        if (spanSeconds < 0) {
            throw new IllegalArgumentException("spanSeconds must be non-negative: " + spanSeconds);
        }

        // Filter by the watermark first: in the steady state every PV is covered and this returns
        // without touching the database. A LinkedHashSet collapses duplicates so one bulk never
        // carries two upserts on the same _id.
        final Set<String> pvNamesToWrite = new LinkedHashSet<>();
        for (String pvName : pvNames) {
            final Long cachedSpanSeconds = maxSpanSecondsByPvName.get(pvName);
            if (cachedSpanSeconds == null || cachedSpanSeconds < spanSeconds) {
                pvNamesToWrite.add(pvName);
            }
        }
        if (pvNamesToWrite.isEmpty()) {
            return;
        }

        final List<WriteModel<Document>> writes = new ArrayList<>(pvNamesToWrite.size());
        for (String pvName : pvNamesToWrite) {
            writes.add(new UpdateOneModel<>(
                    Filters.eq(BsonConstants.BSON_KEY_PV_STATS_PV_NAME, pvName),
                    Updates.max(BsonConstants.BSON_KEY_PV_STATS_MAX_BUCKET_SPAN_SECONDS, spanSeconds),
                    UPSERT));
        }

        logger.debug("pvStats recordSpan writing spanSeconds: {} for {} PVs", spanSeconds, writes.size());

        final BulkWriteResult result;
        try {
            result = pvStatsCollection.bulkWrite(writes, UNORDERED);
        } catch (MongoException ex) {
            throw new DpException("pvStats bulkWrite failed: " + ex.getMessage(), ex);
        }
        if (!result.wasAcknowledged()) {
            // an unacknowledged write is not known to be stored, so it must not advance the cache
            throw new DpException("pvStats bulkWrite not acknowledged");
        }

        // Advance the watermark only now that the bulk has succeeded (D5): every cached value is
        // one this process has stored. merge() with max keeps a concurrent larger value in place.
        for (String pvName : pvNamesToWrite) {
            maxSpanSecondsByPvName.merge(pvName, spanSeconds, Math::max);
        }
    }

}
