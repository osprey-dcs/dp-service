package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import io.opentelemetry.api.common.Attributes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Per-request timing context for one query request: the stage durations, the outcome, the result
 * counters, and the slow-query log line (issue #212, D3 and D5).
 *
 * <p>One instance is created at handler entry, handed to the job and the dispatcher, and completed
 * once. It carries the stage clock through a request whose work is split across three threads --
 * the gRPC thread that resolves, the worker that executes, and (for the streaming paths) the same
 * worker emitting messages -- which is why the timings live on an object passed along rather than
 * in thread-local state.
 *
 * <h2>The division of labor with the metrics</h2>
 *
 * <p>The histograms answer "is the service slow, and in which stage" across all requests. They
 * carry {@code rpc.method} and {@code dp.stage} and nothing else, because a PV name or a page token
 * as an attribute would multiply the series count by the size of the facility (D8). The slow-query
 * log line answers "which request was slow, and what did it ask for" for one request, where the PV
 * names are exactly what the operator needs and cost nothing. That split is the whole of D5: the
 * per-request detail a trace would carry is written to a log instead, at the same instrumentation
 * points a span would later occupy.
 *
 * <h2>Thread-safety</h2>
 *
 * <p>The stage fields are written on one thread and read on another -- {@code resolveNanos} on the
 * gRPC thread, everything else on the worker -- with the handoff through {@code enqueueJob}'s
 * {@code BlockingQueue}, which establishes happens-before for everything written before the put.
 * The response counters are the exception: {@code recordResponse} is called from the dispatcher,
 * and {@code QueryDataBidiStreamDispatcher} drives its dispatch from more than one thread. Those
 * three counters are therefore {@code volatile}-free but updated only under that dispatcher's
 * existing cursor lock, or from the single worker thread on every other path. They are counters for
 * a log line and a monotonic metric, not control flow: a lost update would understate a byte count,
 * never change a response.
 */
public class QueryTelemetry {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    /**
     * Dedicated logger for the slow-query line, so a facility can route it to its own file without
     * also capturing this class's ordinary logging. The name is a literal rather than a class name
     * for the same reason: it is a routing target named in {@code log4j2.xml}, and moving or
     * renaming the class must not silently redirect an operator's configured appender.
     */
    private static final Logger slowQueryLogger = LogManager.getLogger("dp.slowquery");

    // configuration
    public static final String CFG_KEY_SLOW_QUERY_LOG_THRESHOLD_MILLIS =
            "QueryHandler.slowQueryLogThresholdMillis";
    public static final int DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MILLIS = 1000;

    /** Number of PV names carried in the slow-query line; see {@link #describePvNames}. */
    private static final int SLOW_QUERY_PV_NAME_LIMIT = 3;

    /**
     * Threshold in nanos, resolved once per JVM. Read through a holder rather than a static
     * initializer on this class so that the {@code ConfigurationManager} lookup happens on first
     * query rather than at class load, which in an integration test is before the test's
     * configuration is in place.
     */
    private static final class ThresholdHolder {
        static final long THRESHOLD_NANOS = resolveThresholdNanos();

        private static long resolveThresholdNanos() {
            final int millis = ConfigurationManager.getInstance().getConfigInteger(
                    CFG_KEY_SLOW_QUERY_LOG_THRESHOLD_MILLIS, DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MILLIS);
            if (millis < 0) {
                return -1; // disabled
            }
            return TimeUnit.MILLISECONDS.toNanos(millis);
        }
    }

    // instance variables -- identity and clock
    private final String rpcMethod;
    private final long arrivalNanos = System.nanoTime();
    private final Attributes methodAttributes;

    // stage accumulators, all nanos
    private long resolveNanos = 0;
    private long queueWaitNanos = 0;
    private long dbNanos = 0;

    // result state
    private String outcome = DpMetrics.OUTCOME_SUCCESS;
    private long responseMessages = 0;
    private long responseBytes = 0;
    private long bucketCount = 0;
    private boolean completed = false;

    // request shape, captured for the slow-query line only (never an attribute -- D8)
    private String requestShape = "";

    public QueryTelemetry(String rpcMethod) {
        this.rpcMethod = rpcMethod;
        this.methodAttributes = Attributes.of(DpMetrics.ATTR_RPC_METHOD, rpcMethod);
    }

    public String getRpcMethod() {
        return rpcMethod;
    }

    // ---- stage marks -------------------------------------------------------------------------

    /**
     * Marks the end of resolution, which for the V2 methods includes the resolver's own Mongo reads
     * (PV existence, configuration activations). Called at enqueue; for the legacy methods, which
     * do no resolution, it is simply the small handler-entry-to-enqueue interval.
     */
    public void markEnqueued() {
        resolveNanos = System.nanoTime() - arrivalNanos;
    }

    /**
     * Copies the queue wait the worker measured (D2) into this request's stage breakdown. Taking it
     * from the job rather than measuring it again here keeps one definition of "queue wait": the
     * value recorded into {@code dp.handler.queue.wait} for every service and the value in the
     * query stage breakdown are then the same number by construction, and a dashboard comparing
     * them cannot be reading two slightly different measurements.
     */
    public void markJobStarted(HandlerJob job) {
        queueWaitNanos = job.getQueueWaitNanos();
    }

    /**
     * Adds time attributable to the database: the wall time of an {@code executeQuery*()} call plus,
     * through {@link #addCursorTime}, the time the dispatcher spent inside the cursor.
     *
     * <p>Additive rather than a setter because a single request can issue more than one database
     * operation -- the samples paths resolve status timestamps before retrieving buckets, and the
     * table path has both a find and an aggregate.
     */
    public void addDbNanos(long nanos) {
        dbNanos += nanos;
    }

    /** Folds a finished {@code TimedMongoCursor}'s accumulated time and document count in. */
    public void addCursorTime(long cursorNanos, long documentCount) {
        dbNanos += cursorNanos;
        bucketCount += documentCount;
    }

    // ---- outcome and results -----------------------------------------------------------------

    /** Records one response message of {@code bytes} serialized size sent to the client. */
    public void recordResponse(int bytes) {
        responseMessages++;
        responseBytes += bytes;
    }

    /**
     * Marks the request as rejected -- a client mistake, per the repo's reject/error split (#235).
     * A reject is deliberately a distinct outcome from an error rather than folded into it: a rise
     * in rejects means clients are sending something wrong, and a rise in errors means the service
     * is failing, and an alert that cannot tell them apart pages the wrong person.
     */
    public void markReject() {
        outcome = DpMetrics.OUTCOME_REJECT;
    }

    /** Marks the request as failed by the service. */
    public void markError() {
        outcome = DpMetrics.OUTCOME_ERROR;
    }

    /**
     * Marks a successful request that returned no data. Separate from {@code success} because an
     * empty result is a different operational question -- a query returning nothing quickly is
     * usually a client asking for the wrong window, and it would otherwise dilute the success
     * latency distribution with a population of near-zero requests.
     */
    public void markEmpty() {
        outcome = DpMetrics.OUTCOME_EMPTY;
    }

    // ---- request shape (slow-query line only) ------------------------------------------------

    /** Captures the shape of a resolved V2 query for the slow-query line. */
    public void captureShape(ResolvedQuery resolvedQuery) {
        if (resolvedQuery == null) {
            return;
        }
        final List<TimeInterval> intervals = resolvedQuery.getRetrievalIntervals();
        final StringBuilder shape = new StringBuilder();
        shape.append(describePvNames(resolvedQuery.getPvNames()));
        shape.append(" intervals=").append(intervals == null ? 0 : intervals.size());
        if (intervals != null && !intervals.isEmpty()) {
            final TimeInterval first = intervals.get(0);
            final TimeInterval last = intervals.get(intervals.size() - 1);
            shape.append(" range=[").append(first.getBeginSeconds())
                    .append(',').append(last.getEndSeconds()).append(')');
        }
        shape.append(" pageSize=").append(resolvedQuery.getPageSize())
                .append(" pageTokenPresent=").append(resolvedQuery.getPageStart() != null)
                .append(" mode=").append(resolvedQuery.getMode())
                .append(" streaming=").append(resolvedQuery.isStreaming())
                .append(" serializedColumns=").append(resolvedQuery.isUseSerializedColumns())
                .append(" excludeColumnMetadata=").append(resolvedQuery.isExcludeColumnMetadata())
                .append(" statusFilter=").append(resolvedQuery.getStatusFilter() != null);
        requestShape = shape.toString();
    }

    /**
     * Captures the shape of a legacy {@code QueryDataRequest.QuerySpec} for the slow-query line.
     *
     * <p>Shorter than the V2 shape because the V1 {@code QuerySpec} carries only a PV list and a
     * time range: it has no paging, no result mode, and no representation flags. A V1 line
     * therefore omits those fields rather than printing defaults that would read as choices the
     * client made.
     */
    public void captureShape(QueryDataRequest.QuerySpec querySpec) {
        if (querySpec == null) {
            return;
        }
        requestShape = describePvNames(querySpec.getPvNamesList())
                + " range=[" + querySpec.getBeginTime().getEpochSeconds()
                + ',' + querySpec.getEndTime().getEpochSeconds() + ')';
    }

    /** Captures the shape of a legacy {@code QueryTableRequest} for the slow-query line. */
    public void captureShape(QueryTableRequest request) {
        if (request == null) {
            return;
        }
        final String pvDescription = switch (request.getPvNameSpecCase()) {
            case PVNAMELIST -> describePvNames(request.getPvNameList().getPvNamesList());
            case PVNAMEPATTERN -> "pvNamePattern=" + request.getPvNamePattern().getPattern();
            default -> "pvSpec=none";
        };
        requestShape = pvDescription
                + " range=[" + request.getBeginTime().getEpochSeconds()
                + ',' + request.getEndTime().getEpochSeconds() + ')'
                + " format=" + request.getFormat();
    }

    /**
     * Renders a PV list as a count plus the first few names.
     *
     * <p>Bounded because a resolved pattern query can name thousands of PVs, and a log line that
     * grows with the facility's PV count is one an operator turns off. Three names are enough to
     * recognize which query this was; the count is what says how big it was.
     */
    private static String describePvNames(List<String> pvNames) {
        if (pvNames == null || pvNames.isEmpty()) {
            return "pvCount=0";
        }
        final List<String> sample = new ArrayList<>(
                pvNames.subList(0, Math.min(SLOW_QUERY_PV_NAME_LIMIT, pvNames.size())));
        final StringBuilder description = new StringBuilder("pvCount=")
                .append(pvNames.size()).append(" pvs=").append(String.join(",", sample));
        if (pvNames.size() > sample.size()) {
            description.append(",...");
        }
        return description.toString();
    }

    // ---- completion --------------------------------------------------------------------------

    /**
     * Records every stage histogram, the counters, and the slow-query line if the request was slow.
     *
     * <p>Idempotent, and deliberately so. Every job calls this from a {@code finally}, but the
     * streaming dispatchers can also complete a request early on an error path, and the bidi
     * dispatcher outlives its job entirely. A second call recording a second set of observations
     * would double-count a request in every histogram and counter -- a quiet inflation of the
     * request rate that no test would notice, since the numbers would still look plausible.
     */
    public synchronized void complete() {

        if (completed) {
            return;
        }
        completed = true;

        final long totalNanos = System.nanoTime() - arrivalNanos;

        // "process" is what is left after the stages that were measured directly: assembly,
        // serialization, and the onNext calls. Clamped at zero because the three measured stages
        // are taken from different threads and the bidi path can accumulate cursor time after the
        // job that started the clock has returned -- a small negative is arithmetic, not a signal,
        // and a negative observation would be rejected by the SDK anyway.
        final long processNanos = Math.max(0, totalNanos - resolveNanos - queueWaitNanos - dbNanos);

        recordStage(DpMetrics.STAGE_RESOLVE, resolveNanos);
        recordStage(DpMetrics.STAGE_QUEUE, queueWaitNanos);
        recordStage(DpMetrics.STAGE_DB, dbNanos);
        recordStage(DpMetrics.STAGE_PROCESS, processNanos);
        recordStage(DpMetrics.STAGE_TOTAL, totalNanos);

        DpMetrics.queryRequests().add(1, Attributes.of(
                DpMetrics.ATTR_RPC_METHOD, rpcMethod, DpMetrics.ATTR_OUTCOME, outcome));
        DpMetrics.queryBuckets().add(bucketCount, methodAttributes);
        DpMetrics.queryResponseMessages().add(responseMessages, methodAttributes);
        DpMetrics.queryResponseBytes().add(responseBytes, methodAttributes);

        logSlowQuery(totalNanos, processNanos);
    }

    private void recordStage(String stage, long nanos) {
        DpMetrics.queryStageDuration().record(
                DpMetrics.nanosToSeconds(nanos),
                Attributes.of(DpMetrics.ATTR_RPC_METHOD, rpcMethod, DpMetrics.ATTR_STAGE, stage));
    }

    private void logSlowQuery(long totalNanos, long processNanos) {

        final long thresholdNanos = ThresholdHolder.THRESHOLD_NANOS;
        if (thresholdNanos < 0 || totalNanos < thresholdNanos) {
            return;
        }

        try {
            slowQueryLogger.warn(
                    "slow query method: {} outcome: {} totalMs: {} resolveMs: {} queueMs: {} "
                            + "dbMs: {} processMs: {} buckets: {} messages: {} bytes: {} {}",
                    rpcMethod, outcome,
                    millis(totalNanos), millis(resolveNanos), millis(queueWaitNanos),
                    millis(dbNanos), millis(processNanos),
                    bucketCount, responseMessages, responseBytes, requestShape);
        } catch (Exception ex) {
            // Instrumentation must never disturb the request it measures, and complete() runs in a
            // finally on the response path -- an exception escaping here would be thrown after the
            // response was already sent, from a finally block, where it would replace whatever the
            // try block was doing.
            logger.error("error writing slow query log line: {}", ex.getMessage(), ex);
        }
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }
}
