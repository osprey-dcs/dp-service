package com.ospreydcs.dp.service.common.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Every instrument dp instrumentation records into, and the attribute keys it is allowed to attach
 * (issue #212).
 *
 * <p>Instruments are created lazily on first use rather than in a static initializer, because
 * {@link DpTelemetry#init} runs after this class may already have been loaded, and because
 * {@link DpTelemetry#shutdown()} and the test hooks swap the SDK out — an instrument built
 * eagerly against the meter that existed at class-load time would keep recording into a provider
 * that has since been shut down. {@link #discardInstruments()} discards them so the next lookup rebuilds
 * against the current SDK.
 *
 * <p><b>Cardinality policy (D8).</b> The attribute keys declared here are the complete vocabulary
 * dp instrumentation may attach to a metric. A PV name, provider id, client request id, page
 * token, or user identity must <em>never</em> become an attribute: a facility with 10^5 PVs would
 * turn a single histogram into 10^5 time series, which is how a metrics backend is taken down by
 * the service it monitors. Per-request detail of that kind belongs in the slow-query log line
 * instead, which carries the PV names precisely because this does not.
 */
public class DpMetrics {

    // metric names (D10: the dp. prefix matches the dp.config/DP_* configuration namespace, and
    // Prometheus renders e.g. dp.handler.queue.wait with unit s as dp_handler_queue_wait_seconds)

    public static final String METRIC_HANDLER_QUEUE_WAIT = "dp.handler.queue.wait";
    public static final String METRIC_HANDLER_JOB_DURATION = "dp.handler.job.duration";
    public static final String METRIC_HANDLER_WORKERS_ACTIVE = "dp.handler.workers.active";
    public static final String METRIC_HANDLER_WORKERS_MAX = "dp.handler.workers.max";

    public static final String METRIC_QUERY_STAGE_DURATION = "dp.query.stage.duration";
    public static final String METRIC_QUERY_REQUESTS = "dp.query.requests";
    public static final String METRIC_QUERY_BUCKETS = "dp.query.buckets";
    public static final String METRIC_QUERY_RESPONSE_MESSAGES = "dp.query.response.messages";
    public static final String METRIC_QUERY_RESPONSE_BYTES = "dp.query.response.bytes";

    public static final String METRIC_INGEST_REQUESTS = "dp.ingest.requests";
    public static final String METRIC_INGEST_DURATION = "dp.ingest.duration";
    public static final String METRIC_INGEST_BUCKETS = "dp.ingest.buckets";
    public static final String METRIC_INGEST_SAMPLES = "dp.ingest.samples";
    public static final String METRIC_INGEST_REQUEST_BYTES = "dp.ingest.request.bytes";

    public static final String METRIC_DB_OPERATION_DURATION = "db.client.operation.duration";

    // attribute keys — the complete vocabulary (D8), nothing else

    public static final AttributeKey<String> ATTR_SERVICE = AttributeKey.stringKey("dp.service");
    public static final AttributeKey<String> ATTR_JOB = AttributeKey.stringKey("dp.job");
    public static final AttributeKey<String> ATTR_STAGE = AttributeKey.stringKey("dp.stage");
    public static final AttributeKey<String> ATTR_OUTCOME = AttributeKey.stringKey("dp.outcome");
    public static final AttributeKey<String> ATTR_RPC_METHOD = AttributeKey.stringKey("rpc.method");
    public static final AttributeKey<String> ATTR_DB_OPERATION_NAME =
            AttributeKey.stringKey("db.operation.name");
    public static final AttributeKey<String> ATTR_DB_COLLECTION_NAME =
            AttributeKey.stringKey("db.collection.name");
    public static final AttributeKey<String> ATTR_DB_NAMESPACE =
            AttributeKey.stringKey("db.namespace");
    public static final AttributeKey<String> ATTR_ERROR_TYPE = AttributeKey.stringKey("error.type");

    // attribute values

    /** {@code dp.service} values, one per {@code QueueHandlerBase} implementation. */
    public static final String SERVICE_INGESTION = "ingestion";
    public static final String SERVICE_QUERY = "query";
    public static final String SERVICE_ANNOTATION = "annotation";
    public static final String SERVICE_INGESTION_STREAM = "ingestionstream";

    /** {@code dp.stage} values for the query pipeline (D3). */
    public static final String STAGE_RESOLVE = "resolve";
    public static final String STAGE_QUEUE = "queue";
    public static final String STAGE_DB = "db";
    public static final String STAGE_PROCESS = "process";
    public static final String STAGE_TOTAL = "total";

    /** {@code dp.outcome} values. */
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_REJECT = "reject";
    public static final String OUTCOME_ERROR = "error";
    public static final String OUTCOME_EMPTY = "empty";
    /**
     * A server-streaming response abandoned part-way because the client cancelled or stopped
     * draining the transport (#274). Deliberately distinct from {@code error}: the service did
     * nothing wrong and the data it did send was correct, but the response is incomplete, so
     * counting it as {@code success} would hide the one condition outbound flow control exists to
     * manage.
     */
    public static final String OUTCOME_ABANDONED = "abandoned";

    /** Unit for every duration instrument, per OTel semantic conventions. */
    public static final String UNIT_SECONDS = "s";
    public static final String UNIT_BYTES = "By";

    /**
     * Explicit histogram bucket boundaries, in seconds, for every dp duration histogram (D9).
     *
     * <p>The SDK's default boundaries (5, 10, 25, ... 10000) were chosen for milliseconds. With the
     * semantic-convention unit {@code s}, every observation a healthy service produces would land
     * in the first bucket and every percentile above p50 would read as the bucket edge — the
     * numbers would look plausible and be meaningless. This ladder runs from 1 ms to 120 s because
     * the query benchmark against the customer archive has produced multi-minute queries, so the
     * top of the range has to be wide enough that a pathological query is distinguishable from a
     * merely slow one rather than all of them piling into {@code +Inf}.
     */
    public static final List<Double> DURATION_BUCKET_BOUNDARIES_SECONDS = List.of(
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0);

    private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    // Instruments, created on first use and discarded by discardInstruments(). Every accessor
    // below is synchronized: the check-then-assign is not atomic, and two threads racing on first
    // use would each build an instrument. The SDK returns equivalent instruments for the same
    // name/unit/description so the duplicate records correctly, but it also logs a
    // duplicate-instrument warning under some configurations, and an accessor that looks
    // thread-safe but is not is the kind of thing that gets copied. Contention is a non-issue:
    // after the first call every invocation is an uncontended lock on an already-built field.


    private static volatile DoubleHistogram handlerQueueWait = null;
    private static volatile DoubleHistogram handlerJobDuration = null;
    private static volatile LongUpDownCounter handlerWorkersActive = null;
    private static volatile DoubleHistogram queryStageDuration = null;
    private static volatile LongCounter queryRequests = null;
    private static volatile LongCounter queryBuckets = null;
    private static volatile LongCounter queryResponseMessages = null;
    private static volatile LongCounter queryResponseBytes = null;
    private static volatile LongCounter ingestRequests = null;
    private static volatile DoubleHistogram ingestDuration = null;
    private static volatile LongCounter ingestBuckets = null;
    private static volatile LongCounter ingestSamples = null;
    private static volatile LongCounter ingestRequestBytes = null;
    private static volatile DoubleHistogram dbOperationDuration = null;

    private DpMetrics() {
    }

    private static Meter meter() {
        return DpTelemetry.meter();
    }

    private static DoubleHistogram durationHistogram(String name, String description) {
        return meter().histogramBuilder(name)
                .setDescription(description)
                .setUnit(UNIT_SECONDS)
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKET_BOUNDARIES_SECONDS)
                .build();
    }

    /** Converts a {@code System.nanoTime()} difference to the seconds the histograms record. */
    public static double nanosToSeconds(long nanos) {
        return nanos / NANOS_PER_SECOND;
    }

    // handler instruments (D2)

    /** Time a job spent between construction and the start of {@code execute()}. */
    public static synchronized DoubleHistogram handlerQueueWait() {
        DoubleHistogram instrument = handlerQueueWait;
        if (instrument == null) {
            instrument = durationHistogram(
                    METRIC_HANDLER_QUEUE_WAIT,
                    "time a handler job waited in the request queue before a worker started it");
            handlerQueueWait = instrument;
        }
        return instrument;
    }

    /** Wall time of {@code job.execute()}. */
    public static synchronized DoubleHistogram handlerJobDuration() {
        DoubleHistogram instrument = handlerJobDuration;
        if (instrument == null) {
            instrument = durationHistogram(
                    METRIC_HANDLER_JOB_DURATION,
                    "wall time a handler job spent executing");
            handlerJobDuration = instrument;
        }
        return instrument;
    }

    /**
     * Workers currently inside {@code job.execute()}.
     *
     * <p>There is deliberately <b>no queue-depth gauge</b> alongside this, and its absence is not
     * an oversight: {@code QueueHandlerBase} uses a {@code LinkedBlockingQueue} of capacity
     * {@code MAX_QUEUE_SIZE == 1}, so depth is never a meaningful number — it is 0 or 1, sampled
     * between two scrape intervals. Backpressure on this design does not accumulate in the queue
     * at all; it blocks the calling gRPC thread inside {@code enqueueJob}'s {@code put()}.
     * Sustained saturation therefore shows up here — active workers pinned at the configured
     * maximum — and the wait it causes shows up in {@link #handlerQueueWait()}, which is a
     * distribution rather than a sample.
     */
    public static synchronized LongUpDownCounter handlerWorkersActive() {
        LongUpDownCounter instrument = handlerWorkersActive;
        if (instrument == null) {
            instrument = meter().upDownCounterBuilder(METRIC_HANDLER_WORKERS_ACTIVE)
                    .setDescription("handler worker threads currently executing a job")
                    .build();
            handlerWorkersActive = instrument;
        }
        return instrument;
    }

    /**
     * Registers an observable gauge reporting a handler's configured worker count.
     *
     * <p>Returned so the caller can close the registration when the handler shuts down; an
     * integration test builds many handlers in one JVM, and a callback left registered would keep
     * reporting for a handler that no longer exists.
     *
     * @param serviceName {@code dp.service} attribute value
     * @param workerCount supplies the configured maximum, read at each collection
     */
    public static AutoCloseable registerHandlerWorkersMax(
            String serviceName, java.util.function.IntSupplier workerCount) {

        final Attributes attributes = Attributes.of(ATTR_SERVICE, serviceName);
        return meter().gaugeBuilder(METRIC_HANDLER_WORKERS_MAX)
                .setDescription("configured maximum worker threads for the handler")
                .ofLongs()
                .buildWithCallback(
                        measurement -> measurement.record(workerCount.getAsInt(), attributes));
    }

    // query instruments (D3)

    /** Per-stage durations of a query request; see {@code QueryTelemetry} for the stage set. */
    public static synchronized DoubleHistogram queryStageDuration() {
        DoubleHistogram instrument = queryStageDuration;
        if (instrument == null) {
            instrument = durationHistogram(
                    METRIC_QUERY_STAGE_DURATION,
                    "duration of one stage of query request handling");
            queryStageDuration = instrument;
        }
        return instrument;
    }

    /** Completed query requests, by outcome. */
    public static synchronized LongCounter queryRequests() {
        LongCounter instrument = queryRequests;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_QUERY_REQUESTS)
                    .setDescription("query requests completed, by outcome")
                    .build();
            queryRequests = instrument;
        }
        return instrument;
    }

    /** Bucket documents read from MongoDB while serving query requests. */
    public static synchronized LongCounter queryBuckets() {
        LongCounter instrument = queryBuckets;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_QUERY_BUCKETS)
                    .setDescription("bucket documents read from the database serving queries")
                    .build();
            queryBuckets = instrument;
        }
        return instrument;
    }

    /** Response messages sent to query clients. */
    public static synchronized LongCounter queryResponseMessages() {
        LongCounter instrument = queryResponseMessages;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_QUERY_RESPONSE_MESSAGES)
                    .setDescription("response messages sent to query clients")
                    .build();
            queryResponseMessages = instrument;
        }
        return instrument;
    }

    /** Serialized response bytes sent to query clients. */
    public static synchronized LongCounter queryResponseBytes() {
        LongCounter instrument = queryResponseBytes;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_QUERY_RESPONSE_BYTES)
                    .setDescription("serialized response bytes sent to query clients")
                    .setUnit(UNIT_BYTES)
                    .build();
            queryResponseBytes = instrument;
        }
        return instrument;
    }

    // ingestion instruments (Task 8)

    /** Ingestion requests whose handling completed, by outcome. */
    public static synchronized LongCounter ingestRequests() {
        LongCounter instrument = ingestRequests;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_INGEST_REQUESTS)
                    .setDescription("ingestion requests handled, by outcome")
                    .build();
            ingestRequests = instrument;
        }
        return instrument;
    }

    /**
     * Time from an ingestion request's arrival to the end of its handling.
     *
     * <p>This is the latency the gRPC call duration cannot see. Ingestion acknowledges a request
     * as soon as it is validated and enqueued, so {@code grpc.server.call.duration} on an
     * ingestion method measures validation and enqueue only — persistence happens afterwards on a
     * worker thread. An operator reading only the RPC duration would conclude ingestion was
     * healthy while the queue behind it fell arbitrarily far behind.
     */
    public static synchronized DoubleHistogram ingestDuration() {
        DoubleHistogram instrument = ingestDuration;
        if (instrument == null) {
            instrument = durationHistogram(
                    METRIC_INGEST_DURATION,
                    "time from ingestion request arrival to the end of its handling");
            ingestDuration = instrument;
        }
        return instrument;
    }

    /** Bucket documents written by the ingestion service. */
    public static synchronized LongCounter ingestBuckets() {
        LongCounter instrument = ingestBuckets;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_INGEST_BUCKETS)
                    .setDescription("bucket documents written by ingestion")
                    .build();
            ingestBuckets = instrument;
        }
        return instrument;
    }

    /** Individual samples ingested, across all columns of all handled requests. */
    public static synchronized LongCounter ingestSamples() {
        LongCounter instrument = ingestSamples;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_INGEST_SAMPLES)
                    .setDescription("individual samples ingested")
                    .build();
            ingestSamples = instrument;
        }
        return instrument;
    }

    /** Serialized bytes of the ingestion requests handled. */
    public static synchronized LongCounter ingestRequestBytes() {
        LongCounter instrument = ingestRequestBytes;
        if (instrument == null) {
            instrument = meter().counterBuilder(METRIC_INGEST_REQUEST_BYTES)
                    .setDescription("serialized bytes of ingestion requests handled")
                    .setUnit(UNIT_BYTES)
                    .build();
            ingestRequestBytes = instrument;
        }
        return instrument;
    }

    // database instrument (D4)

    /**
     * Duration of a MongoDB command round-trip, recorded by {@code DpMongoCommandListener}.
     *
     * <p>Named for the OTel database semantic conventions rather than with the {@code dp.} prefix,
     * so that a dashboard or alert written against the convention works against this service
     * without a dp-specific translation.
     */
    public static synchronized DoubleHistogram dbOperationDuration() {
        DoubleHistogram instrument = dbOperationDuration;
        if (instrument == null) {
            instrument = durationHistogram(
                    METRIC_DB_OPERATION_DURATION,
                    "duration of a MongoDB command round-trip");
            dbOperationDuration = instrument;
        }
        return instrument;
    }

    /**
     * Discards every cached instrument so the next use rebuilds against the current SDK.
     *
     * <p>Called by {@link DpTelemetry#shutdown()} as well as the test hooks, which is why this is
     * not named for tests: an instrument outliving the provider it was built against records into
     * a closed SDK forever, and {@code shutdown()} is a production path.
     */
    static synchronized void discardInstruments() {
        handlerQueueWait = null;
        handlerJobDuration = null;
        handlerWorkersActive = null;
        queryStageDuration = null;
        queryRequests = null;
        queryBuckets = null;
        queryResponseMessages = null;
        queryResponseBytes = null;
        ingestRequests = null;
        ingestDuration = null;
        ingestBuckets = null;
        ingestSamples = null;
        ingestRequestBytes = null;
        dbOperationDuration = null;
    }

}
