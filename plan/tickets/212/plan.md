# dp-service #212: metrics for ingestion and query request handling

## Overview

#212 was filed as a placeholder for "add metrics to the MLDP services". This plan scopes a
fast-track first increment whose primary purpose is **diagnosing query performance** at a
deployed facility, with ingestion covered by the same shared machinery plus a small set of
throughput counters. It delivers, for every gRPC server in the repo:

- **Request metrics** — rate, error rate, and latency histograms per RPC method, from the
  grpc-java OpenTelemetry module, plus JVM runtime metrics.
- **Handler metrics** — queue wait, job duration, and active-worker count for every
  `QueueHandlerBase` worker pool, recorded once in the base class so every current and future
  job type is covered.
- **MongoDB operation metrics** — per-command durations (`find`, `getMore`, `aggregate`,
  `insert`, …) by collection, from a driver `CommandListener` installed in the one place the
  driver client is built.
- **Query stage breakdown** — per-method histograms for resolve, queue wait, database cursor time,
  and processing time, with buckets/messages/bytes returned; and a **slow-query log line**
  carrying the same stage breakdown for one request, so an operator can answer "why was *this*
  query slow" without a trace backend.
- **Ingestion counters** — requests by outcome, buckets, samples, request bytes, and the
  arrival-to-persisted latency that the gRPC call duration cannot see (see triage §2).
- **A Prometheus scrape endpoint per service** by default, switchable to OTLP push with one
  environment variable, and an operator document (`doc/metrics.md`).

The audience is the operator diagnosing a slow `querySamples`/`queryData` at a facility, and the
developer running the query benchmark who today can only measure from the client side.

Scratch research that motivated the design is summarized in `~/dp/dev/research/metrics.md`
(outside the repo). This plan departs from it where triage showed the code differs from the
generic picture (§2, §3 below).

## Background: triage findings

### 1. There is no metrics or tracing library in the project today

`pom.xml` carries no Micrometer, OpenTelemetry, Prometheus, or Dropwizard dependency, and no
source file references any of them. The only timing in the repo is client-side, in the benchmark
programs (`doc/benchmark-overview.md`; the ingestion benchmark's write rate is measured at the
stream response, which is sent on enqueue, not on persistence). Every server-side number in this
plan is new.

### 2. Ingestion's gRPC call duration is not ingestion latency

`IngestionServiceImpl.handleIngestionRequest()` (`IngestionServiceImpl.java:257-290`) validates,
sends the ack via `responseObserver.onNext()`, and *then* hands the request to
`handler.handleIngestionRequest()`, which enqueues an `IngestDataJob`
(`MongoIngestionHandler.java:113-128`). `ingestData()` (`IngestionServiceImpl.java:232-243`)
calls `onCompleted()` immediately after. Persistence — provider lookup, bucket generation,
`insertMany`, `requestStatus` insert, subscription publish — runs later on a worker thread
(`IngestDataJob.handleIngestionRequest()`).

So an RPC-level duration for `ingestData` measures validation plus the time the gRPC thread
spends blocked in `requestQueue.put()`; it never includes the database write. The research
summary's "Phase 1: automatic instrumentation gives substantial visibility" is therefore true
for query and false for ingestion. Every ingestion metric that matters is job-side and
hand-written: arrival-to-persisted latency, insert duration, and throughput counters.

### 3. The request queue has capacity 1, so "queue depth" is not a signal

`QueueHandlerBase.java:17` declares `MAX_QUEUE_SIZE = 1` and `requestQueue` is a
`LinkedBlockingQueue<>(MAX_QUEUE_SIZE)` (`QueueHandlerBase.java:22`). Backpressure does not show
up as a growing queue; it shows up as the **gRPC thread blocking in `put()`** — in
`enqueueJob()` (`QueueHandlerBase.java:41-51`) and in the four direct `requestQueue.put()` sites
in `MongoIngestionHandler` and the eight in `MongoQueryHandler` (e.g.
`MongoQueryHandler.java:335`). For unary calls that block is inside the RPC; for
`ingestDataStream`/`ingestDataBidiStream` it stalls `onNext()` and surfaces client-side as
reduced throughput.

The research summary's `queue.depth` gauge would read 0 or 1 forever. The signals that carry the
same information here are **queue wait** (job creation → `execute()` start, which includes the
time blocked in `put()`) and **active workers** against `numWorkers`. Both are recorded in the
base class (D2).

### 4. Query resolution runs on the gRPC thread and reads Mongo before the job is enqueued

For the V2 methods, `MongoQueryHandler.resolveBucketsOrReject()`/`resolveSamplesOrReject()`
(`MongoQueryHandler.java:258-275, 308-325`) call `QueryV2Resolver.resolve()`
(`QueryV2Resolver.java:72`) *before* `enqueueQueryV2Job()` (`MongoQueryHandler.java:327-341`).
Resolution performs database reads — `resolvePvNamesByPattern` (`QueryV2Resolver.java:233`),
`resolvePvNamesByMetadata` (`:255`), `resolveConfigurationIntervals` (`:359`) — so a metadata
or configuration selector can spend real time before the request ever reaches a worker. A
job-only timer would miss it; the "resolve" stage is timed in the handler (D3).

### 5. Database time and assembly time are interleaved through a lazy cursor

`MongoSyncQueryClient.executeQueryBucketsV2()` (`:549`), `executeQuerySamplesV2()` (`:624`) and
`executeQueryData()` (`:243`) open the query through the shared `bucketFind()` helper (`:211`,
added by #271: `find(filter).sort(...).hint(...)`) and call `.cursor()` on the returned
`FindIterable`. The driver issues the initial `find`
on the first `hasNext()` and `getMore` batches as iteration proceeds, and BSON→POJO decoding
happens inside `next()`. Every dispatcher iterates that cursor while building and sending
responses (`QueryBucketsUnaryDispatcher.java:80-135`, `QueryBucketsStreamDispatcher.java:78-127`,
`QuerySamplesUnaryDispatcher.java:87-186` via `TabularDataUtility.addBucketsToTable()`,
`QuerySamplesStreamDispatcher.java:77-242`, `QueryDataStreamDispatcher.handleResult_()`).

Wrapping "the Mongo call" in a timer would time only cursor creation. Separating database time
from processing time therefore needs either a timing decorator around the cursor (D3) or the
driver's command-level events (D4). This plan uses both: they answer different questions
(per-request stage share vs. per-command server round-trip distribution).

`resolveMaxBucketSpanSeconds()` (`MongoSyncQueryClient.java:65,79`) is an eager `find` on
`pvStats` inside `executeQuery*V2()`; it is database time that is not cursor time, which is why
the `db` stage is defined as the `executeQuery*()` call plus cursor time (D3).

### 6. Query completion is dispatcher-driven, so RPC duration is meaningful for query

Unlike ingestion, every query dispatcher calls `onCompleted()`/`send*Response()` itself at the end
of the job (`QueryBucketsStreamDispatcher.java:127`, `QuerySamplesStreamDispatcher.java:172,242`,
`QueryDataStreamDispatcher.handleResult_()`), so `grpc.server.call.duration` for a query method
spans resolve + queue wait + job. It is the correct end-to-end number and needs no hand-written
counterpart; the stage histograms explain it.

### 7. Two single points of construction make the generic layer cheap

- `GrpcServerBase.start()` builds every production server (`GrpcServerBase.java:99-107`),
  including the benchmark servers (`BenchmarkQueryGrpcServer extends QueryGrpcServer`). One call
  to `GrpcOpenTelemetry.configureServerBuilder()` there instruments all four services.
- `MongoSyncClient.initMongoClient()` (`MongoSyncClient.java:49-52`) is
  `MongoClients.create(connectString)`; switching it to `MongoClientSettings` with
  `addCommandListener()` instruments every sync client. `MongoAsyncClient.java:37` is the same
  shape and is off every production path (CLAUDE.md, Schema Migration).

### 8. The integration tests bypass `GrpcServerBase`

`GrpcIntegrationServiceWrapperBase.java:38-50` builds an `InProcessServerBuilder` directly, so
anything added only in `GrpcServerBase.start()` is untested by the ITs. The server-builder
configuration must live in a helper both call (D6). Note also that CLAUDE.md's "Integration
Tests: `src/test/java/com/ospreydcs/dp/service/integration/`" is stale — the source root is
`src/test/integration/java` (added by `build-helper-maven-plugin`, `pom.xml:171-188`); fixed in
this ticket's CLAUDE.md edit.

### 9. The shaded jar does not merge `META-INF/services`

The shade configuration (`pom.xml:225-233`) has a `ManifestResourceTransformer` only. The
OpenTelemetry SDK's autoconfigure discovers exporters through `ServiceLoader`; two exporter jars
each ship a `META-INF/services/io.opentelemetry.sdk.autoconfigure.spi.metrics.ConfigurableMetricExporterProvider`
and without `ServicesResourceTransformer` the last one copied wins silently — the shaded jar
would find `prometheus` or `otlp` but not both, depending on jar order. This is a build change
the ticket owns (Task 1).

### 10. Verified artifact facts

- `io.grpc:grpc-opentelemetry:1.65.1` exists (dp-grpc pins `io.grpc.version` 1.65.1) and its
  public API is `GrpcOpenTelemetry.newBuilder().sdk(OpenTelemetry).build().configureServerBuilder(ServerBuilder<?>)`.
  It depends on `opentelemetry-api` 1.36.0 (forward-compatible with the 1.x API). Server metrics
  emitted: `grpc.server.call.started`, `grpc.server.call.duration`,
  `grpc.server.call.sent_total_compressed_message_size`,
  `grpc.server.call.rcvd_total_compressed_message_size`; attributes `grpc.method`, `grpc.status`.
- OpenTelemetry Java: `opentelemetry-bom` 1.66.0 (stable), `opentelemetry-exporter-prometheus`
  1.66.0-alpha, `opentelemetry-runtime-telemetry-java17` 2.27.0-alpha (instrumentation BOM 2.27.0),
  javaagent 2.31.1. The "-alpha" artifacts are the Prometheus exporter and the JVM runtime
  metrics; the API, SDK, autoconfigure and OTLP exporter are stable.

## Design decisions

### D1 — OpenTelemetry API + SDK in-process, Prometheus scrape by default

Instrumentation code uses the OpenTelemetry metrics API. The SDK is configured in-process via
`AutoConfiguredOpenTelemetrySdk`, with dp-supplied defaults (`otel.metrics.exporter=prometheus`,
`otel.traces.exporter=none`, `otel.logs.exporter=none`, `otel.service.name`, the Prometheus
port) that standard `OTEL_*` environment variables override. Switching to a collector is
`OTEL_METRICS_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=...` with no rebuild.

*Why:* the research direction is OpenTelemetry and a collector-fed backend; the same SDK later
carries traces (out of scope here, D5) from the same instrumentation points; the grpc-java team
ships the gRPC metrics module for the exact gRPC version in use. Given that the primary use case
is query diagnosis and that the ingestion path needs hand-written instrumentation regardless
(§2), the library choice is about where the numbers go, and OTel keeps that an operator decision.

*Rejected — Micrometer + Prometheus registry:* all-stable artifacts and everything (gRPC
interceptor, Mongo command/pool listeners, JVM binders) off the shelf; the fastest path to a
`/metrics` page. Rejected because it is not the OTel API, so a later tracing/exemplar phase
would either bridge or run a second SDK, and because the research explicitly standardizes on
OTel. Revisit only if the alpha exporters prove unreliable in Task 10's shaded-jar check.

*Rejected — OTel Java agent with API-only code:* least code and no shading concern, but
without the `-javaagent` flag every metric is silently a no-op, and the ITs need an SDK on the
test classpath anyway to assert anything. The agent remains the natural add-on for *traces*
(auto-instrumented gRPC and Mongo spans) and coexists with the in-process SDK; that is the
follow-on ticket's problem, not this one's.

### D2 — Job lifecycle timing lives in `QueueHandlerBase`, once

`HandlerJob` gains a `createdNanos` stamp set in its constructor (every job is constructed
immediately before `put()`; the twelve direct `requestQueue.put()` sites in
`MongoIngestionHandler` and `MongoQueryHandler` are routed through the existing `enqueueJob()`
so there is a single enqueue path). `QueueWorker.run()` records, around `job.execute()`:
`dp.handler.queue.wait` (created → execute start), `dp.handler.job.duration`, and increments /
decrements `dp.handler.workers.active`. `dp.handler.workers.max` is an observable gauge of
`getNumWorkers_()`. Attributes: `dp.service` (ingestion, query, annotation, ingestionstream)
and `dp.job` (job class simple name).

*Why:* every job type in every service is covered by one change, and a future job cannot forget
to be measured. There is deliberately **no queue-depth gauge** (§3) — the comment on the
instrument set says why, because its absence will look like an omission to anyone who has
read the research summary.

*Rejected:* per-job timing in each `execute()` — twelve copies today and one more per new job.

### D3 — Query stage timing rides a per-request context object; cursor time via a decorator

A `QueryTelemetry` object is created at handler entry (`handleQueryBuckets`, `handleQuerySamples`,
their stream variants, `handleQueryData`, `handleQueryDataStream`, `handleQueryTable`), carries
the method name and arrival timestamp, and is handed to the job and dispatcher. It records
`dp.query.stage.duration{rpc.method, dp.stage}` with stages:

| stage | measured from → to | where recorded |
|---|---|---|
| `resolve` | handler entry → job enqueue (V2 methods; includes resolver Mongo reads, §4) | `MongoQueryHandler` |
| `queue` | job created → `execute()` start | `QueueHandlerBase` (D2), copied into the context |
| `db` | wall time of `executeQuery*()` + accumulated time inside cursor `hasNext()`/`next()`/`close()` (§5) | `TimedMongoCursor` decorator returned by the query client |
| `process` | job duration − `db` (assembly, serialization, `onNext`) | derived at completion |
| `total` | handler entry → completion | derived at completion |

plus `dp.query.requests{rpc.method, dp.outcome}` (success / reject / error / empty),
`dp.query.buckets{rpc.method}` (counted in `TimedMongoCursor.next()`, free),
`dp.query.response.messages{rpc.method}` and `dp.query.response.bytes{rpc.method}` (recorded at
the dispatcher send sites, which already compute serialized sizes for the message-size budget).

`TimedMongoCursor<T>` implements `MongoCursor<T>` and delegates, accumulating nanos and a
document count; the query client wraps its return values. The bidi dispatcher's cursor lock
(`QueryDataBidiStreamDispatcher.java:25`) is unaffected — the decorator has no state that the
lock does not already protect.

*Why:* the alternative — timing inside each dispatcher's loop body — touches nine loops and
splits the accounting across two classes per method. The decorator makes "db" one definition.

*Rejected:* deriving `process` and `total` histograms on the dashboard side. Histograms cannot
be subtracted, so a derived stage must be emitted as its own measurement.

### D4 — MongoDB command durations from a driver `CommandListener`

`DpMongoCommandListener` records `db.client.operation.duration{db.operation.name,
db.collection.name, db.namespace, error.type}` (OTel semantic-convention names) from
`commandStarted`/`commandSucceeded`/`commandFailed`, and is installed in
`MongoSyncClient.initMongoClient()` via `MongoClientSettings.builder().applyConnectionString(...)
.addCommandListener(...)`. `MongoAsyncClient` is left as is (off every production path).

*Why:* this is the only view of server round-trips (`find` vs `getMore` vs `aggregate` vs
`insert`) and it covers the writes on the ingestion path and every annotation-service query
without per-call code. Collection and command names are bounded sets, so cardinality is safe.

*Rejected:* OTel's `opentelemetry-mongo-3.1` library instrumentation — it produces spans, not
metrics, and a span exporter is out of scope (D5).

### D5 — Per-request diagnosis is a slow-query log line; tracing is a follow-on

At query completion, if `total` ≥ `QueryHandler.slowQueryLogThresholdMillis` (default 1000;
−1 disables), `QueryTelemetry` logs one WARN line on the dedicated logger `dp.slowquery`:
method, outcome, the five stage durations, request shape (PV count and the first three PV
names, begin/end, page size, page-token present, result mode, representation flags), result
counts (buckets, messages, bytes), and `workers.active` at enqueue. The dedicated logger name
lets `log4j2.xml` route it to its own file at a facility.

*Why:* the research's "metrics → trace → diagnosis" workflow needs a trace backend, which is
another deployable at a facility that has none. The same stage breakdown in a log line gives
the operator the per-request answer today, and the instrumentation points are exactly where
spans go later. Distributed tracing (spans + `OTEL_TRACES_EXPORTER`) is deferred to a follow-on
ticket, filed at closure of this one, not to a later phase of #212.

### D6 — One telemetry bootstrap, initialized before the handler, shared with the ITs

`DpTelemetry` (`common/telemetry`) owns the SDK: `init(serviceName, metricsPort)` in
`GrpcServerBase.start()` **before** `initService_()` — the Mongo client and the handlers create
instruments during init and must find a real `Meter`. `Telemetry.enabled=false` installs
`OpenTelemetry.noop()`. Production code never touches `GlobalOpenTelemetry` (settable once per
JVM; the ITs build many servers per JVM); it obtains meters from `DpTelemetry.meter()`.
`DpTelemetry.initForTest(OpenTelemetrySdk)` lets a test install an SDK with an
`InMemoryMetricReader` and reset it. `DpTelemetry.configureServerBuilder(ServerBuilder<?>)`
applies `GrpcOpenTelemetry`, and both `GrpcServerBase.start()` and
`GrpcIntegrationServiceWrapperBase` call it (§8).

`stopServer()` shuts the SDK down after the gRPC server, flushing the last export.

A Prometheus port that cannot be bound **fails startup** (`DpRuntimeException`), following the
#254 rule that startup failure must stop the server: the alternative is a service that runs
with silently absent metrics. The override is one environment variable
(`DP_<SERVICE>_SERVER_METRICS_PORT`, or `DP_TELEMETRY_ENABLED=false`).

### D7 — Per-service Prometheus ports

`doc/running.md` runs all four services on one host, so each server has its own default:
ingestion 9464, query 9465, annotation 9466, ingestion-stream 9467, under
`<Service>Server.metricsPort`. The benchmark servers inherit their parent's key and must be given
a different port when co-located with a production service (documented in `doc/metrics.md`).
Host binds `Telemetry.prometheusHost` (default `0.0.0.0`).

### D8 — Cardinality policy is a fixed attribute vocabulary

Attributes emitted by dp instrumentation are exactly: `dp.service`, `dp.job`, `dp.stage`,
`dp.outcome`, `rpc.method`, `db.operation.name`, `db.collection.name`, `db.namespace`,
`error.type`. **Never** `pvName`, `providerId`, `clientRequestId`, a page token, or a user
identity — a facility with 10⁵ PVs would turn one histogram into 10⁵ time series. High-cardinality
values belong in the slow-query log line. The rule is recorded in CLAUDE.md so a future
"just add the PV name" change is recognizable as a regression.

### D9 — Duration histograms carry explicit second-scaled bucket advice

The OTel SDK's default explicit boundaries (5, 10, 25, … 10000) were chosen for milliseconds;
with the semantic-convention unit `s` every observation lands in the first bucket and p95/p99
are meaningless. Every dp duration histogram sets
`setExplicitBucketBoundariesAdvice` to a second-scaled ladder from 1 ms to 120 s (the query
benchmark on the customer archive has produced multi-minute queries — memory: a bound-less
scan is ~4 minutes). grpc-java's module sets its own ladder. The ladder lives in one constant
in `DpMetrics`.

### D10 — Metric names use the `dp.` prefix

`dp.` matches the configuration namespace (`dp.config`, `DP_*`) and the package. Prometheus
renders `dp.handler.queue.wait` with unit `s` as `dp_handler_queue_wait_seconds`. The research
summary's `mldp.` is a one-constant rename if the facility prefers it.

### D11 — Dependency versions are pinned by the OpenTelemetry instrumentation BOM

`opentelemetry-instrumentation-bom-alpha` (2.27.0) is imported in `dependencyManagement`; it
pins the SDK, autoconfigure, OTLP and Prometheus exporters, and the runtime-telemetry artifact
at mutually compatible versions. `grpc-opentelemetry` is pinned to `io.grpc.version` from
dp-grpc, whose 1.36 API dependency resolves to the BOM's newer API (verified compatible in
Task 1's build). Task 1 confirms that the alpha BOM transitively imports the stable
`opentelemetry-bom`; if not, both are imported explicitly.

## Implementation tasks

### Task 1 — Build: dependencies and shading

**Status: done.** `pom.xml`:
- `dependencyManagement`: import
  `io.opentelemetry.instrumentation:opentelemetry-instrumentation-bom-alpha:2.31.1-alpha`
  (pom, import scope).
- Dependencies (unversioned where the BOM pins them): `io.opentelemetry:opentelemetry-api`,
  `opentelemetry-sdk`, `opentelemetry-sdk-extension-autoconfigure`,
  `opentelemetry-exporter-prometheus`, `opentelemetry-exporter-otlp`,
  `io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry`;
  `io.grpc:grpc-opentelemetry:${io.grpc.version}` — add an `io.grpc.version` property to
  dp-service's pom matching dp-grpc's (1.65.1), with a comment that the two must move together.
- Test scope: `io.opentelemetry:opentelemetry-sdk-testing` (`InMemoryMetricReader`).
- Shade: add `org.apache.maven.plugins.shade.resource.ServicesResourceTransformer` (§9).

Four corrections to what this task assumed, found while executing it:

1. **The alpha BOM's version carries an `-alpha` suffix** — `2.31.1-alpha`, not `2.27.0`. The
   planned coordinate does not exist and fails the build at POM-read time. It does transitively
   import `opentelemetry-bom` and `opentelemetry-bom-alpha` (both 1.65.0), so the single import
   covers the stable artifacts as D11 hoped and no second import is needed.
2. **`opentelemetry-runtime-telemetry-java17` was renamed to `opentelemetry-runtime-telemetry`**,
   and its last published version under the old name is 2.27.0-alpha — which is very likely the
   version the research summary was written against. The entry point renamed with it:
   `RuntimeMetrics.create(OpenTelemetry)` is now
   `io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry.create(OpenTelemetry)`,
   returning the same `AutoCloseable`. Task 2 uses the new name.
3. **`grpc-opentelemetry` was already on the classpath transitively** through `grpc-all`, and
   `grpc-xds`/`grpc-gcp-csm-observability` were dragging in `opentelemetry-sdk` **1.36.0** at
   runtime scope. So the BOM import is not merely a convenience for omitting versions — it is
   what displaces that older SDK. `mvn dependency:tree -Dincludes=io.opentelemetry` after the
   change shows every artifact at 1.65.0. Without it the service would build against 1.65 APIs
   and run against a 1.36 SDK.
4. **`AutoConfiguredOpenTelemetrySdkBuilder.setResultAsGlobal` now takes no argument** — it opts
   *into* global registration rather than toggling it, and non-global is the default. D1's "never
   `GlobalOpenTelemetry`" is therefore satisfied by simply not calling it; calling it with `false`
   does not compile.

Verification performed:
- `mvn -DskipTests package`, then on the shaded jar: the Prometheus exporter registers as a
  **MetricReader** provider
  (`META-INF/services/…spi.internal.ConfigurableMetricReaderProvider` →
  `PrometheusMetricReaderProvider`) while OTLP registers as a **MetricExporter** provider
  (`…spi.metrics.ConfigurableMetricExporterProvider` → `OtlpMetricExporterProvider`) — two
  separate service files, not the single merged one this task predicted. Both are present.
  Evidence the transformer is load-bearing: `…spi.ResourceProvider` holds entries contributed by
  two different jars (`GCPResourceProvider` from grpc's contrib and `EnvironmentResourceProvider`
  from the SDK), and `…spi.internal.ComponentProvider` merges Prometheus with all six OTLP
  providers. Without the transformer one jar's copy silently replaces the other's.
- End-to-end export proven from the shaded jar with a throwaway main: autoconfigure with the D1
  property defaults, one `dp.handler.queue.wait` histogram value with the D9 boundaries, and
  `RuntimeTelemetry.create()`. `GET /metrics` returned 200 with
  `dp_handler_queue_wait_seconds_bucket{dp_service="query",le="0.05"} 1` (the D9 ladder rendered
  exactly, `dp.` → `dp_` translation confirmed) and the `jvm_*` families. This is the Task 10
  `DpTelemetryPrometheusTest` assertion, confirmed ahead of writing it.
- The `MongoClientSettings`/`GrpcOpenTelemetry`/`PrometheusHttpServerBuilder` signatures Tasks 5
  and 7 call were checked against the resolved jars: `GrpcOpenTelemetry.newBuilder().sdk(…)
  .build().configureServerBuilder(ServerBuilder<?>)` is present as planned.

### Task 2 — `DpTelemetry` bootstrap (`common/telemetry/DpTelemetry.java`)

**Status: done.**

- `static void init(String serviceName, String prometheusHost, int prometheusPort)`: reads
  `Telemetry.enabled`; if false installs `OpenTelemetry.noop()`. Otherwise builds
  `AutoConfiguredOpenTelemetrySdk.builder().addPropertiesSupplier(...)` with the defaults in D1
  and `otel.exporter.prometheus.host/port`, `disableShutdownHook()` is *not* used (the SDK's own
  hook flushes on JVM exit) but `shutdown()` is also called explicitly from `stopServer()`.
  Wrap the build in a catch that converts a bind failure into `DpRuntimeException` (D6).
- `static Meter meter()`; `static OpenTelemetry openTelemetry()`.
- `static void configureServerBuilder(ServerBuilder<?>)`: `GrpcOpenTelemetry.newBuilder().sdk(openTelemetry()).build().configureServerBuilder(builder)`.
- `static void initForTest(OpenTelemetrySdk)` / `resetForTest()`.
- JVM metrics: `RuntimeMetrics.create(openTelemetry())` after SDK init, held for close.
- Idempotence: a second `init()` in the same JVM is a no-op with a warning (the benchmark
  programs start a server and a client in one process).

### Task 3 — Instrument holder (`common/telemetry/DpMetrics.java`)

**Status: done.**

- Lazily created singletons for every instrument in D2–D4 and the ingestion counters (Task 8),
  built from `DpTelemetry.meter()` on first use, so a `resetForTest()` rebuilds them against the
  test SDK. The explicit bucket ladder constant (D9). `AttributeKey` constants for the D8
  vocabulary and nothing else.


#### What executing Tasks 2–3 established

1. **`init()` takes `(serviceName, prometheusPort)`, not the plan's
   `(serviceName, prometheusHost, prometheusPort)`.** The host is read from `Telemetry.prometheusHost`
   inside `init()`, as D7 specifies it should be — it is a deployment-wide setting, while the port is
   per-service. Passing it would have made every one of the four call sites in Task 7 read the same
   config key.

2. **`DpMetrics` exposes `registerHandlerWorkersMax(serviceName, IntSupplier)`** returning an
   `AutoCloseable`, rather than an instrument accessor like the others. An observable gauge is a
   registered callback, not a thing you record into, and the registration has to be closed when a
   handler shuts down — an IT builds many handlers in one JVM and a leaked callback would keep
   reporting for a handler that no longer exists. Task 4 calls this from `QueueHandlerBase.init()`
   and closes it in `fini()`.

3. **`DpTelemetry.shutdown()` is separate from `resetForTest()`.** `shutdown()` closes the SDK this
   class built (Task 7 calls it from `stopServer()`); `resetForTest()` only drops the references,
   because a test supplies its own SDK and owns its reader's lifecycle. `initForTest()` deliberately
   does not take ownership.

4. **Instruments cache in `volatile` fields and `resetForTest()` nulls them.** They cannot be
   `static final`: `DpTelemetry.init()` runs after this class may already be loaded, and the ITs swap
   SDKs between tests. Verified: after a reset the next use rebuilds against the new SDK, and nothing
   leaks into the discarded one.

5. **The Prometheus exporter inserts an `otel_scope_name` label between the dp attributes and
   `le`.** A line renders as
   `dp_handler_queue_wait_seconds_bucket{dp_service="query",otel_scope_name="com.ospreydcs.dp.service",le="0.05"} 1`.
   Task 10's `DpTelemetryPrometheusTest` must therefore not assert on a contiguous
   `{dp_service=...,le=...}` substring — match the metric name and the `le` label separately, or the
   test fails against a correct export. The scope name is `DpTelemetry.INSTRUMENTATION_SCOPE_NAME`.

6. **Traces and logs exporters must be explicitly `none`.** Autoconfigure defaults both to `otlp`, so
   without those two properties every service would try to reach a collector on `localhost:4317` and
   log an export failure on each interval. The plan listed them under D1; noting here that they are
   load-bearing rather than tidiness.

#### Verification performed

Behavioral, not just compile: 14 checks against an `InMemoryMetricReader` (no-op before init;
recording against the no-op does not throw; instruments visible after `initForTest`; unit is `s`; the
D9 ladder is the one in force, with a 42 ms observation landing in `(0.01, 0.05]`; the workers.max
gauge reports its supplier; rebuild-after-reset; no leak into the discarded SDK), plus a real-SDK run
that asserted `init()` → live `/metrics` with the ladder and `dp_service` label rendered, a second
`init()` as a no-op, `dp_ingest_buckets_total`, the `jvm_*` families, and — the D6 contract — an
already-bound Prometheus port producing `DpRuntimeException` rather than a silently metric-less
service. Full unit suite: 549 tests, 0 failures.

### Task 4 — `QueueHandlerBase` and `HandlerJob` (D2)

Status: done.

- `HandlerJob`: `final long createdNanos = System.nanoTime()`; `long queueWaitNanos` set by the
  worker before `execute()` (read by `QueryTelemetry`).
- `QueueHandlerBase`: `protected abstract String getServiceName_()` (four implementations:
  `MongoIngestionHandler`, `MongoQueryHandler`, `MongoAnnotationHandler`,
  `IngestionStreamHandler`); `QueueWorker.run()` records the three measurements around
  `job.execute()` — the `finally` must decrement `workers.active` even when `execute()` throws
  (the existing catch swallows the exception, which is the hang documented throughout
  CLAUDE.md; the counter must not leak on that path). Observable gauge `dp.handler.workers.max`
  registered in `init()`.
- Route the twelve direct `requestQueue.put()` blocks in `MongoIngestionHandler` (`:104, :124,
  :142, :175`) and `MongoQueryHandler` (`:108, :129, :150, :166, :184, :202, :220, :335`) through
  `enqueueJob(job, id)`; behavior is unchanged (same put, same interrupt handling) and the
  duplicated try/catch goes away. `IngestionStreamHandler.java:67, :95` likewise.

#### What executing Task 4 established

1. **`enqueueJob(job, jobId)` already existed** on `QueueHandlerBase` — the plan described adding
   the single enqueue path, but a prior change had introduced it and converted
   `MongoAnnotationHandler`'s 28 call sites. Task 4 therefore only converted the remaining
   handlers.
2. **Fourteen direct `put()` sites, not twelve.** The plan counted the twelve in
   `MongoIngestionHandler` and `MongoQueryHandler` and mentioned `IngestionStreamHandler`'s two
   separately without adding them in. All fourteen now go through `enqueueJob`; `requestQueue.put`
   appears nowhere outside `QueueHandlerBase`, which is the invariant worth grepping for.
3. **Three call sites keep a `logger.debug` that `enqueueJob` does not subsume**, because they log
   detail the shared line cannot know: `handleIngestionRequest` (provider and client request id),
   `enqueueQueryV2Job` (the V2 method label), and nothing else. The other eleven dropped their
   debug line entirely — `enqueueJob` logs the job class name and id.
4. **The timing lives in a private `executeJob(HandlerJob)`, not inline in `QueueWorker.run()`.**
   The worker loop body is now one call, and the measurement/catch/finally block reads as one
   unit rather than nesting three levels deep inside two `try` blocks.
5. **`workers.active` is attributed by `dp.service` only, while `queue.wait` and `job.duration`
   carry `dp.service` + `dp.job`.** An up-down counter split by job class would make "are workers
   saturated" a sum across a dozen series that each individually mean nothing; the histograms want
   the job breakdown, the saturation gauge does not.
6. **`HandlerJob.setQueueWaitNanos` is package-private** so only `QueueHandlerBase` writes it,
   while `getQueueWaitNanos()` is public for `QueryTelemetry` (Task 6) to read. A job subclass
   cannot corrupt its own measurement.
7. **The exception log at the job escape was rewritten** — it said `QueryWorker.run encountered
   exception` in all four services and logged `getMessage()` with no stack trace. It now names the
   job class, says the job was dropped without dispatching, and passes the exception object per
   the CLAUDE.md convention. This is the hang documented throughout CLAUDE.md, and the log line is
   the only evidence of it.
8. **`fini()` closes the `workers.max` registration before `fini_()`**, and the callback verifiably
   stops reporting afterward. Without this an IT that builds many handlers per JVM accumulates
   callbacks reporting a worker count for handlers that no longer exist.

#### Verification performed

28 checks against an `InMemoryMetricReader` driving a real `QueueHandlerBase` subclass: the
gauge's value and its live tracking of the supplier; `queue.wait` and `job.duration` unit,
count, attributes, magnitude (bounded below by a deliberate pre-submit sleep and above by 5 s, so
a nanos-recorded-as-seconds bug cannot pass), and the D9 ladder actually in force;
`getQueueWaitNanos()` agreeing with the recorded value to within a millisecond; and the two that
justify the task's shape — **`workers.active` nets to zero after `execute()` throws**, with
`job.duration` still recorded for the throwing job and the handler still serving afterward. A
second program ran the same handler lifecycle with telemetry **never initialized** (the state of
every existing unit test): init, a normal job, a throwing job, a job after the escape, and fini
all succeed against the no-op meter. Full unit suite 549 tests / 0 failures; integration tests
74 / 0 failures across the query, ingestionstream, ingest, and v2api packages — which between
them exercise every converted enqueue site.

### Task 5 — Mongo command listener (D4)

Status: done.

- `common/mongo/DpMongoCommandListener.java`: `CommandListener` recording
  `db.client.operation.duration` (unit `s`) with `db.operation.name = event.getCommandName()`,
  `db.collection.name` from the command document's first field value when it is a string (the
  collection for `find`/`insert`/`aggregate`/`getMore` (`collection` field)/`update`/`delete`/
  `count`), `db.namespace = event.getDatabaseName()`, `error.type` on `commandFailed`. Redaction:
  read only the collection name, never the filter.
- `MongoSyncClient.initMongoClient()`: build `MongoClientSettings` from the connection string and
  add the listener. `MongoAsyncClient` untouched.
- `PvStatsMaxSpanUpdater`'s `bulkWrite` and `insertMany` on the ingestion path are covered by
  this listener with no further code.

#### What executing Task 5 established

Everything below was verified empirically against MongoDB 8.0 with driver 5.4.0 rather than
reasoned about, because every one of these facts is the kind that produces a plausible-looking
wrong number instead of an error.

1. **Only `CommandStartedEvent` carries `getCommand()`.** The succeeded and failed events, which
   are the ones carrying `getElapsedTime()`, have no access to the command document — so the
   collection name cannot be read where the duration is recorded. The listener has to correlate
   start to end. The plan's task description assumed a single event could supply both.
2. **Correlation is a `ThreadLocal`, not a map.** The driver invokes the listener synchronously on
   the thread that issued the command and delivers that command's end event on the same thread:
   verified across 3364 commands on 25 threads with deliberate failures mixed in — zero request-id
   collisions while a command was in flight, zero end events on a different thread, zero orphans.
   A map keyed by request id would need an eviction policy for an end event that never arrives and
   would grow without bound if that policy were wrong; a single-slot ThreadLocal cannot leak more
   than one string per driver thread. The slot is cleared in a `finally`, so a thread cannot carry
   one command's collection into the next.
3. **`countDocuments()` issues an `aggregate`, not a `count`.** Only `estimatedDocumentCount()`
   issues `count`. A dashboard panel or alert filtering `db.operation.name="count"` would miss
   essentially every count this codebase performs. Recorded in the class javadoc.
4. **`getMore`'s first field is the cursor id, not the collection** — the collection is in a
   separate `collection` field, so the "first string field" rule needs that fallback or every
   batch fetch after the first loses its collection attribution. The plan named this, and the
   probe confirmed it is the only command with the shape. Confirmed collection-in-first-field for
   `find`, `insert`, `update`, `delete`, `aggregate`, `count`, `distinct`, `createIndexes`,
   `killCursors`; `hello`, `dropDatabase` and `endSessions` name no collection and correctly get
   no `db.collection.name` attribute at all.
5. **A server-selection failure emits no command events whatsoever.** Against an unreachable
   server, `countDocuments()` threw `MongoTimeoutException` having produced neither a started nor
   a failed event. So `db.client.operation.duration` covers round-trips that *reached* a server,
   and a total database outage reads as the metric going silent rather than as an `error.type`
   spike. An operator alerting on a database outage needs absence-of-data, not an error rate —
   documented in the class javadoc because the natural assumption is the opposite.
6. **`error.type` is the event's throwable, which is not always the caller's.** A bad index hint
   arrives on the event as `MongoCommandException` while the caller catches the driver's
   `MongoQueryException` wrapper. This matters directly to #271's missing-index guard: an alert
   written against the caller-facing class name would never fire.
7. **A throwing listener does not break the operation** — the driver swallows it (verified). The
   listener still guards both callbacks anyway: relying on that would make correctness depend on
   undocumented driver behavior, and a silently swallowed exception is the failure mode CLAUDE.md
   warns about throughout.
8. **`MongoClients.create(MongoClientSettings)` is the only way to attach a listener.**
   `applyConnectionString` preserves everything a deployment's URI specifies, so no deployment
   behavior changes. `MongoAsyncClient` is left alone per the plan — it is off every production
   path.

#### Verification performed

A 35-check program against a real MongoDB asserts the instrument's identity (name, unit `s`,
histogram type, D9 bucket ladder), that `find`/`getMore`/`insert`/`update`/`delete`/`aggregate`
are all recorded, that `getMore` is attributed to the collection rather than the cursor id, that
inserts into two collections produce two series, that durations are seconds and not nanos (max
series sum 0.013), that the attribute key set is **exactly** `{db.operation.name,
db.collection.name, db.namespace}` with no PV name anywhere, that a failed command produces a
separate series carrying `error.type` while the successful series stays clean, that a
no-collection command gets no collection attribute, and — under 12 threads interleaving finds
against two collections — that attribution stays exact (pvStats finds counted exactly 240 of 240)
with no null-collection series, which is the assertion that would catch a correlation bug. A
second program drives the real `MongoSyncClient.init()` production path through `MongoTestClient`
and confirms init alone records 90 commands including `createIndexes` and `hello` across the
`dp-test` and `admin` namespaces. A third check confirms the listener is harmless when telemetry
was never initialized — the state of every existing unit test. Full suites: 549 unit tests, 74
integration tests (query, ingest, ingestionstream, v2api), plus the annotation IT package.

### Task 6 — Query stage telemetry (D3, D5)

Status: done.

- `query/handler/QueryTelemetry.java`: method name, arrival nanos, `resolveNanos`, `queueWaitNanos`,
  `dbNanos`, outcome, counters, request-shape fields captured from `ResolvedQuery` (V2) or
  `QuerySpec`/`QueryTableRequest` (legacy) — PV count, first three names, time range, page
  size/token-present, mode, representation. `markEnqueued()`, `markJobStarted(HandlerJob)`,
  `recordResponse(int bytes)`, `markReject()/markError()/markEmpty()`, `complete()` — records
  the stage histograms, the counters, and the slow-query line (threshold from
  `QueryHandler.slowQueryLogThresholdMillis` via `ConfigurationManager`, read once).
- `common/mongo/TimedMongoCursor.java`: `MongoCursor<T>` decorator accumulating nanos in
  `hasNext()`, `next()`, `tryNext()`, `close()` and counting `next()` calls; exposes
  `elapsedNanos()`, `documentCount()`.
- `MongoSyncQueryClient`: `executeQueryBucketsV2`, `executeQueryBucketsV2Stream`,
  `executeQuerySamplesV2`, `executeQueryData`, `executeQueryTable` return their cursor wrapped;
  the interface return type stays `MongoCursor<BucketDocument>`. `bucketFind()` (#271) returns
  a `FindIterable`, so the wrap goes at the `.cursor()` call sites through one sibling helper
  (`timedCursor(MongoIterable<T>)`), keeping the #271 hint/sort helper untouched. **Corrected at
  execution (finding 1): there are three such sites, not five** — `executeQueryData` and
  `executeQueryTable` both delegate to `executeBucketDocumentQuery`, a `find` rather than an
  aggregate, which also serves `executeDataBlockQuery` (the annotation export path), so wrapping
  that one method instruments all five V1 paths. The `db` stage adds the wall time of the
  `executeQuery*()` call itself (covers `resolveMaxBucketSpanSeconds`, §5), and the samples
  dispatchers time `resolveSampleStatusTimestamps` separately since the bucket cursor cannot see
  it.
- `MongoQueryHandler`: create `QueryTelemetry` at entry of each `handleQuery*`, time resolution,
  pass it into `QueryV2Job`/`QueryDataJob`/`QueryTableJob` and the dispatcher; `enqueueQueryV2Job`
  calls `markEnqueued()`.
- `QueryV2Job.execute()`, `QueryDataJob.execute()`, `QueryTableJob.execute()`: `markJobStarted(this)`,
  then `try { … } finally { telemetry.complete() }` — completion is recorded even if a dispatcher
  throws (the escaped-exception hang is then at least visible as an `error` outcome in metrics).
- Dispatchers: at each `send*Response*`/`onNext` site add `telemetry.recordResponse(size)`; at
  each reject/error/empty branch set the outcome. Nine dispatcher classes; the bidi dispatcher
  records per stream, keyed by the dispatcher instance (one `QueryTelemetry` per bidi request
  is out of scope, noted below).
- `QueryServiceImpl`: no change — validation of legacy requests is trivial and rejects before the
  handler; those rejects are visible in `grpc.server.call.duration` counts and are not stage-timed.


#### What executing Task 6 established

1. **`executeQueryTable` issues a `find`, not an aggregate.** The task description says "the
   `executeQueryTable` aggregate cursor wraps through the same helper", but `executeQueryTable` and
   `executeQueryData` both delegate to `executeBucketDocumentQuery`, which is a single `find` —
   and the same method also serves `executeDataBlockQuery`, the annotation service's export path.
   So there are **three** `.cursor()` sites to wrap in this client, not five, and wrapping
   `executeBucketDocumentQuery` instruments the V1 unary, stream, bidi, table, and export paths at
   once. The aggregate cursors in this client (`executeQueryPvMetadata`, `executeQueryProviders`)
   belong to the metadata methods, which are out of Task 6's stage-timing scope.

2. **`timedCursor` cannot live inside `bucketFind()`.** `bucketFind` returns a `FindIterable`
   precisely so `MongoBucketQueryPlanTest` can `explain()` the exact query production issues
   (#232/#271). Wrapping there would have the plan test explaining a different object than the
   service opens. The helper is therefore a sibling taking `MongoIterable`, applied at each
   `.cursor()` call.

3. **The `db` stage is roughly half of `total` on a real query, which is the whole argument for the
   decorator.** Measured on a 50-bucket V2 query against local MongoDB: `db` 0.0202s of `total`
   0.0405s. Timing only the `executeQuery*()` call would have attributed nearly all of that to
   `process`, because opening a cursor fetches only the first batch — every subsequent batch is a
   `getMore` inside `hasNext()`/`next()`.

4. **The `db` stage is an upper bound on database time, not a measurement of it.** What the
   decorator measures is "time the dispatcher spent inside the cursor", which includes the driver's
   BSON decode of each document. That is attributable to retrieval rather than assembly, so it
   belongs here — but it means `db` is not purely server time. `db.client.operation.duration` (D4)
   is the server-side view; the two together separate "the server is slow" from "we are decoding a
   great deal". Documented on `TimedMongoCursor`.

5. **`Iterator.remove()` and `forEachRemaining` must be delegated explicitly.** `MongoCursor`
   inherits a default `remove()` that throws and a default `forEachRemaining`; a decorator that
   inherits them instead of delegating silently changes behavior (`remove`) or drains the cursor
   outside its own view, reporting a `db` stage of zero for that portion of the result
   (`forEachRemaining`). Both are overridden — `forEachRemaining` onto the *timed* `hasNext`/`next`,
   not the delegate's.

6. **A V2 request rejected during resolution never reaches a job**, so nothing would have counted
   it. A malformed page token, an unresolvable PV selector, or a streaming request carrying a page
   token would have made `dp.query.requests` report a reject rate of zero for exactly the failures
   clients generate most. `MongoQueryHandler.completeRejectedResolution()` completes the telemetry
   at the reject; the stage breakdown is almost entirely `resolve`, which is the honest answer.

7. **The V1 `QuerySpec` has no representation flags.** The task description's request-shape list
   (page size, mode, representation) applies to V2 only: `QueryDataRequest.QuerySpec` carries just
   a PV list and a time range. The V1 slow-query line omits those fields rather than printing
   defaults that would read as choices the client made.

8. **`complete()` had to be made idempotent, not merely called once.** Every job calls it from a
   `finally`, but the streaming dispatchers also complete early on error paths and the bidi
   dispatcher outlives its job. A second recording would double-count the request in every
   histogram and counter — an inflated request rate that still looks plausible, so no test would
   catch it. Verified: three `complete()` calls produce exactly one request, one byte total, one
   total-stage observation.

9. **`queryDataBidiStream` is measured to first response, and the class now says so.** The job's
   `finally` fires when the first response has been sent, not when the stream ends, so later pulls
   are not in the stage histograms. Recording per pull would count one client request as many in
   `dp.query.requests` (inflating the rate by the client's page count); holding the context open
   until the stream closes is unbounded for an abandoned stream. The later pulls are still visible
   in `db.client.operation.duration` and `grpc.server.call.duration`. This is the plan's existing
   out-of-scope item, now documented at the code.

10. **`process` must be clamped at zero.** It is derived (`total − resolve − queue − db`), and the
    inputs come from different threads, so a small negative is arithmetic rather than signal — and
    the SDK rejects a negative observation outright. Observed in the slow-query verification when a
    synthetic 1500 ms `db` exceeded a 2 ms real elapsed.

11. **`mvn test-compile` reports BUILD SUCCESS without compiling anything** when its incremental
    staleness check decides nothing changed — it did so here while four test files had
    constructor-arity errors. `rm -rf target/test-classes` first when a signature change must
    actually be checked.

#### Verification performed

`TimedMongoCursor` semantics (delegation, accumulation across `hasNext`/`next`/`close`, time
accumulated even when the cursor throws mid-iteration, `tryNext` null not counted as a document);
the V2 `queryBuckets` unary path end-to-end through the real `QueryV2Job` +
`QueryBucketsUnaryDispatcher` against MongoDB (all five stages recorded exactly once, `db` non-zero
and ≤ `total`, seconds-scaled, D9 ladder, unit `s`, 50/50 buckets counted, response messages and
bytes); the empty outcome distinguished from success on a no-rows query with the `db` stage still
recorded; V1 `queryData` (20/20 buckets, non-zero `db`) and V1 `queryTable` (10/10) through their
real jobs; the D8 guard — query-metric attribute keys are exactly `{rpc.method, dp.stage,
dp.outcome}` and **no PV name appears as an attribute value anywhere**; `complete()` idempotence;
a null cursor counted as `error` and never as `success`. 48/48. Slow-query line: 11/11 — one WARN
line on the `dp.slowquery` logger carrying method, outcome, all five stage durations in ms, byte
count, PV count, the first three PV names with `,...` truncation, and the time range; plus a
separate run confirming the default 1000 ms threshold suppresses a fast query. Regression: 549 unit
tests, 74 integration tests (query, ingest, ingestionstream, v2api), and the annotation suite — all
0 failures.

### Task 7 — Server bootstrap (D6, D7)

**Status: done.**

- `GrpcServerBase`: two new abstract methods, `protected abstract String getServiceName_()` and
  `protected abstract int getMetricsPort_()` (the port is a method, not a constant, because each
  implementation reads its own config key — see finding 1); `start()` calls `DpTelemetry.init(...)`
  first, then `initService_()`, then `DpTelemetry.configureServerBuilder(builder)` before
  `.build()`; `stopServer()` shuts the SDK down after `server.shutdown().awaitTermination(...)`,
  and the `initService_()` failure path shuts it down too (finding 3).
- The four servers implement both methods, each with a `CFG_KEY_METRICS_PORT` /
  `DEFAULT_METRICS_PORT` pair (D7: 9464/9465/9466/9467) and the matching `DpMetrics.SERVICE_*`
  constant as its name.
- The two **benchmark** servers override `getMetricsPort_()` as well (finding 2), on 60451/60452.
- `GrpcIntegrationServiceWrapperBase`: `DpTelemetry.configureServerBuilder()` on the
  `InProcessServerBuilder` before `build()`. The test SDK is installed in `GrpcIntegrationTestBase`
  rather than per-service intermediates (finding 4): `setUp()` installs an `InMemoryMetricReader`
  SDK via `initForTest()` before the wrappers are created, `tearDown()` calls `resetForTest()`
  after every wrapper `fini()` and closes the SDK. The reader is exposed as
  `protected InMemoryMetricReader metricReader` for Task 10's metric-asserting ITs.

#### What executing Task 7 established

1. **The metrics port had to be an abstract method, not the plan's "new config keys" alone.** Each
   server reads its own `<Service>Server.metricsPort` key, so there is no single key
   `GrpcServerBase` could read; and the two benchmark servers need to return a constant rather than
   read config at all. This mirrors `getPort_()` exactly, which exists for the same reason.

2. **The benchmark servers must override the metrics port, and this is new breakage that D7's
   "inherit their parent's key" note did not cover.** `BenchmarkIngestionGrpcServer` and
   `BenchmarkQueryGrpcServer` subclass the production servers and override only `getPort_()`.
   Inheriting `getMetricsPort_()` would have them try to bind 9464/9465 — and because D6 makes an
   unbindable Prometheus port a startup failure, a benchmark run against a host with the live
   service would no longer start *at all*, where before it merely shared a config key. They now
   override with 60451/60452, matching the 6005x convention their gRPC ports already use.

3. **`start()` must shut telemetry down if `initService_()` fails.** Telemetry is initialized first
   (the ordering D6 requires), but the shutdown hook that would eventually call `stopServer()` is
   registered *after* the failure throw, so on that path nothing releases the Prometheus port. In
   production the JVM exits and the OS reclaims it; for an in-process caller that catches and
   retries, the retry would hit a port its own previous attempt still held and report a telemetry
   bind error in place of the real initialization failure — the #254 failure mode with the cause
   swapped out. Verified with a deliberately failing server: the port is released and a retry
   reports the genuine failure.

4. **The test SDK belongs in `GrpcIntegrationTestBase`, not in per-service intermediates.** The
   plan assumed a per-service intermediate for each of the four services; there is only one
   (`AnnotationIntegrationTestIntermediate`), and every metric-bearing IT extends
   `GrpcIntegrationTestBase`, which is also the single place that creates and destroys all four
   service wrappers. Installing there gets the ordering right for free — before any wrapper's
   `init()` builds a handler, and reset after every `fini()`, so a handler closing its
   observable-gauge registration still finds a live meter provider.

5. **`DpTelemetry.init()`'s failure message needed the root cause.** The autoconfigure module wraps
   a bind failure three levels deep and its own message is the unhelpful "Unexpected configuration
   error"; that string was all the `DpRuntimeException` carried, i.e. all an operator would see for
   the failure that stops startup. `init()` now reports the deepest cause, so the line reads
   `... exception: BindException: Address already in use`.

6. **gRPC's own metrics use `grpc_method`/`grpc_status`, not the D8 `rpc.method`.** They are
   emitted by grpc-java's instrumentation under scope `grpc-java`, which D8 does not govern (D8 is
   the vocabulary for dp-emitted attributes). Worth stating because a reader of D8 would expect
   `rpc.method` on these series; the Task 9 docs should name the real label. Cardinality is bounded
   by the method set, so the intent of D8 is not violated.

7. **The in-process IT server produces no gRPC metrics even with `configureServerBuilder`
   applied** — the wrapper is wired for parity with production, but what the ITs can assert on is
   the dp instruments, via the `metricReader`. Task 10's metric-asserting ITs must not expect
   `grpc.server.*` series.

#### Verification performed

A throwaway program (36 checks) against the real classes: the two new methods are abstract on
`GrpcServerBase`; each of the four servers has the D7 port and key and returns its `DpMetrics`
service constant; the four ports and four names are distinct and no server's metrics port collides
with its gRPC port; both benchmark servers override the metrics port while inheriting the service
name; before `init()` the meter is the no-op; a real `init()` binds the Prometheus endpoint, serves
`/metrics`, and the scrape carries `dp_ingest_requests_total{dp_outcome="success"} 7.0`, the
`otel.service.name` resource, and the JVM runtime metrics; a second `init()` returns the same SDK
instance rather than rebuilding; a bound port throws `DpRuntimeException` naming both the endpoint
and `BindException: Address already in use`. A second JVM with
`-Ddp.Telemetry.enabled=false` (3 checks) confirmed the no-op instance is installed, no port is
bound, and instruments remain usable. A failing-init server (4 checks) confirmed finding 3.

End-to-end (12 checks) a real `IngestionGrpcServer` was started through `GrpcServerBase.start()`
against MongoDB: the scrape carries `dp_handler_workers_max{dp_service="ingestion"} 7.0` — the
instrument the handler builds during `initService_()`, which is the direct evidence the
init-before-init ordering works, since a reversed order would bind it to the no-op meter; a
`registerProvider` call over the wire produced
`grpc_server_call_duration_seconds_count{grpc_method="...registerProvider",grpc_status="OK"}`,
confirming `configureServerBuilder` was applied before `build()`; the D8 guard found the `dp_`
series carrying only `{dp_job, dp_service, le, otel_scope_name}`; and `stopServer()` shut the SDK
down and released the port.

Regression: 549 unit tests and the full integration suite, 0 failures.

### Task 8 — Ingestion counters and latency (§2)

Status: done.

- `HandlerIngestionRequest`: add `final long arrivalNanos` (constructor stamps it; the two
  construction sites are `IngestionServiceImpl.handleIngestionRequest()` at
  `IngestionServiceImpl.java:288` and `IngestDataStreamRequestObserver.java:59` — the bidi
  observer delegates to the former). Stamping in the constructor rather than at the call sites
  means a third entry point added later is instrumented without having to know it should be.
- `IngestionServiceImpl.getNumRequestColumns(IngestDataRequest)`: the 16-arm column sum extracted
  from `ingestionResponseAck()`, so the samples counter and the ack agree by construction rather
  than by two copies staying in step (see finding 2).
- `IngestDataJob`: a private `IngestionTelemetry` accumulates the request's outcome, bucket count,
  sample count and byte count, and `handleIngestionRequest()` records it in a `finally` around the
  renamed `handleIngestionRequest_()` body (finding 1). Records `dp.ingest.requests{dp.outcome}`
  (`success`/`reject`/`error` — the shared `DpMetrics.OUTCOME_*` values, not the "rejected"
  spelling this task originally used, so the ingestion and query sides are queryable with one
  expression), `dp.ingest.duration{dp.outcome}` (arrival → now; the latency the RPC cannot see),
  `dp.ingest.buckets` (the count the database acknowledged, not the generated batch size —
  finding 3), `dp.ingest.samples` (rows × columns) and `dp.ingest.request.bytes`.
  `insertMany`/`requestStatus` insert durations come from Task 5.

#### What executing Task 8 established

1. **Recording on the return path alone would lose exactly the requests that matter most.** An
   exception escaping `IngestDataJob.execute()` is caught and dropped by the `QueueHandlerBase`
   worker (the repo-wide hang failure mode). Recorded beside the `return`, a request that failed
   hard enough to skip its own `requestStatus` insert would appear in **no** counter at all, so a
   burst of such failures would show up as `dp.ingest.requests` *falling* rather than as an error
   rate rising — a drop in throughput is ambiguous (quiet accelerator, dead provider, broken
   service) where an error rate is not. The recording is therefore in a `finally`, and
   `IngestionTelemetry.outcome` defaults to `error` so the escape counts as the failure it is.
   Verified by injecting a throwing `insertBatch`.

2. **The column count had to become a shared method, not a second copy.** `dp.ingest.samples` is
   rows × columns, and the only existing column sum was 16 inline `getXxxColumnsCount()` terms
   inside `ingestionResponseAck()`. CLAUDE.md's "Systematic Process for Adding New Protobuf Column
   Types" has seven steps and none of them mentions metrics, so a private copy of that sum would
   have silently started undercounting the first time a column type was added — and reported a
   plausible number while doing it. Extracted as `IngestionServiceImpl.getNumRequestColumns()`,
   used by both the ack and the counter.

3. **The bucket counter records what the database acknowledged, not the batch size.** The two
   differ exactly when `insertMany` partially failed, which is the case an operator is trying to
   see; `recordBucketsInserted()` is called from the branch that has already checked
   `wasAcknowledged()` and the inserted-count match. Confirmed against MongoDB: the counter and
   `countDocuments()` on the buckets collection agree.

4. **Samples and bytes are counted on every outcome, buckets only on success.** A rejected or
   failed request contributes real offered load and zero stored buckets, so counting all three
   unconditionally keeps `dp.ingest.buckets / dp.ingest.samples` readable as "what fraction of the
   offered load was actually stored". Excluding failures from the denominator would make that
   ratio read as 1.0 no matter how much was being dropped.

5. **`IngestionRequestStatus` already carried the outcome, so no second classification was
   needed.** Its three values map exactly onto the `dp.outcome` vocabulary, and it is assigned
   before the `requestStatus` insert — so `setStatus()` reads the same classification the request
   status document stores, and the metric cannot disagree with the database record of the same
   request.

6. **Arrival is stamped per request on a streaming call, not per stream.** `IngestDataStreamRequestObserver`
   builds a `HandlerIngestionRequest` inside `handleIngestionRequest_()`, which
   `IngestionStreamRequestObserverBase.onNext()` calls once per request — so a long-lived bidi
   stream does not report durations that grow with stream age. Measured end-to-end: three requests
   sent 120 ms apart on one stream produced a maximum duration of 0.046 s against 0.36 s of client
   think time.

7. **`IngestDataRequest` and `RegisterProviderRequest` have no `requestTime` field.** Both carry
   only the ids and the payload. Noted because the verification program was written against the
   assumption that they did; nothing in the implementation depended on it.

#### Verification performed

Job-level, against real MongoDB with the production `MongoSyncIngestionClient` and
`MongoIngestionHandler`: the arrival stamp is non-zero and set by the constructor; `dp.ingest.requests`
carries exactly one point per outcome with the vocabulary `{success, reject, error}`;
`dp.ingest.duration` is seconds-scaled with unit `s` and the D9 ladder and measures from arrival
rather than from job start (a 15 ms pre-job hold appeared in the histogram); `dp.ingest.buckets`
equals the column count and the buckets actually written; `dp.ingest.samples` equals rows ×
columns; `dp.ingest.request.bytes` equals `getSerializedSize()` with unit `By`; a reject adds no
buckets but still counts its samples and bytes; an invalid `providerId` records `error`; an
exception escaping the job is recorded as `error` rather than lost; and the **D8 guard** —
ingestion metric attribute keys are exactly `{dp.outcome}`, with no PV name, provider id, or
client request id appearing as any attribute value. 40/40.

End-to-end through the real `IngestionServiceImpl` (the object the gRPC server serves), with an
in-memory reader installed: the unary `ingestData` path records one success with its buckets,
samples and duration, and the refactored ack still reports the right row and column counts; a bidi
stream carrying three requests records four successes (one per `onNext`, not one per stream) whose
maximum duration excludes the client think time between them; and a request failing validation at
the RPC is recorded as `reject` without disturbing the success count. 15/15.

Regression: 549 unit tests, 20 ingestion/ingestion-stream integration tests, 54 query/v2api
integration tests — all 0 failures.

### Task 9 — Configuration, logging, documentation

Status: done.

- `src/main/resources/application.yml` **and** `src/test/resources/application.yml` (the test
  copy shadows the main one): `Telemetry.enabled`, `Telemetry.prometheusHost`,
  `IngestionServer.metricsPort` (9464), `QueryServer.metricsPort` (9465),
  `AnnotationServer.metricsPort` (9466), `IngestionStreamServer.metricsPort` (9467),
  `QueryHandler.slowQueryLogThresholdMillis` (1000); each with its `DP_*` environment override.
  The test copy sets `Telemetry.enabled: false` so unit tests that construct handlers without a
  server get the no-op SDK, and the ITs override through `initForTest()`.
- `src/main/resources/log4j2.xml`: a `<Logger name="dp.slowquery" level="warn"/>` entry
  showing where a facility routes it; `io.opentelemetry` at `warn`.
- `doc/metrics.md` (new): the metric table (name, type, unit, attributes, meaning), the stage
  definitions from D3 with the interleaving caveat from §5, the ingestion caveat from §2, the
  queue-capacity caveat from §3, enable/disable and ports, `OTEL_*` overrides with a
  Prometheus scrape-config snippet and an OTLP example, PromQL for the p50/p95/p99 table per
  method, the slow-query line format, and the cardinality rule.
- `doc/running.md`: one paragraph pointing to `doc/metrics.md` and listing the metrics ports
  next to the gRPC ports.
- `CLAUDE.md`: a "Metrics and Telemetry (issue #212)" section carrying the invariants that
  outlive the ticket — init ordering (D6), never `GlobalOpenTelemetry`, the attribute vocabulary
  (D8), no queue-depth gauge and why (§3), ingestion RPC duration ≠ persistence (§2), every new
  job is timed by the base class, every new cursor-returning query-client method wraps in
  `TimedMongoCursor`, the shared server-builder helper is what the ITs exercise (§8), and the
  `ServicesResourceTransformer` must stay (§9). Also fix the stale integration-test path (§8).
- `doc/release-notes`: entry for the next release listing the new ports and the
  `Telemetry.enabled` switch (metrics are on by default, which is a deployment-visible change).

#### What executing Task 9 established

1. **`Telemetry.enabled: false` in the test yml would have broken the only test that covers the
   export path.** The plan called for it, reasoning that unit tests constructing handlers without
   a server should get the no-op SDK. They already do, and not because of that key: `DpTelemetry`
   holds the no-op instance until `init()` is called, whose only production caller is
   `GrpcServerBase.start()`, which no unit test runs; and the ITs install their own SDK through
   `initForTest()`, which never consults the key. The one test that *does* read it is
   `DpTelemetryPrometheusTest`, which calls the real `init()` and asserts a live `/metrics` scrape
   — the only coverage of the exporter, the renderer, and the endpoint. The key is therefore
   mirrored at its production default with the reasoning recorded in the file.

2. **The test yml replaces the main one rather than merging, so a key absent there falls back to
   the code `DEFAULT_*` constant, not to the main file's value.** Verified by resolving all seven
   new keys under both classpaths. Nothing under test reads `getMetricsPort_()` today, so the four
   `metricsPort` keys were initially left out — but a key that silently resolves somewhere other
   than where a reader expects is exactly the trap this file sets, so they are mirrored too. All
   seven keys resolve to their code defaults under the main yml, i.e. adding them changed no
   behavior; the test yml differs only in `slowQueryLogThresholdMillis: 0`, deliberately.

3. **The query metadata methods have no `dp.query.*` metrics, and the metric set is eight methods
   rather than the six the docs first listed.** `QueryTelemetry` is created for `queryData`,
   `queryDataStream`, `queryDataBidiStream`, `queryTable`, `queryBuckets`, `queryBucketsStream`,
   `querySamples`, `querySamplesStream` — the bucket-retrieval paths. `queryPvMetadata`,
   `queryProviders`, `queryProviderStats`, and `queryPvStats` have no stage breakdown, no outcome
   counter, and no slow-query line; they remain visible only in `grpc.server.call.duration` and
   `db.client.operation.duration`. Likewise `dp.ingest.*` covers the `IngestDataJob` payload path
   only, not `registerProvider` or the subscription methods. Both gaps are now stated in
   `doc/metrics.md` rather than left for a reader to infer from an incomplete table.

4. **A request rejected by gRPC-layer field validation is invisible to `dp.query.requests`.**
   Found by driving an empty `QueryDataRequest` at a running server: two
   `grpc_server_call_started_total` for the method, one `dp_query_requests_total` point.
   `QueryServiceImpl` validates and rejects before the handler creates a `QueryTelemetry`, so no
   reject is recorded. Resolution-stage rejects *are* counted (Task 6 finding 6), which makes the
   asymmetry easy to miss.

   **And the obvious fallback does not exist either**: my first draft of the docs said these show
   up as a non-`OK` `grpc_status`. They do not — the service reports a rejection as an `OK`
   response carrying an `ExceptionalResult`, and the scrape confirmed `grpc_status="OK"` for the
   rejected call. (`grpc_server_call_started_total` also carries no status label at all, so the
   query I had written could not have worked regardless.) These requests are counted nowhere;
   the only signal is the difference between the gRPC started count and `dp.query.requests`, which
   the docs present as a prompt to read the log rather than a number to alert on. Instrumenting
   them properly needs a telemetry context created at the service layer — a follow-on, noted in
   Out of scope.

5. **`grpc_method` is fully qualified; `rpc_method` is bare.** The gRPC series carry
   `grpc_method="dp.service.query.DpQueryService/queryData"` while the dp series carry
   `rpc_method="queryData"`, so the two families cannot be joined on the method label without a
   rewrite. Documented with the suffix-match workaround.

6. **The slow-query line's two halves use different separators**, which the docs had to be
   corrected to match: the stage breakdown is `field: value` and the request shape is
   `field=value` (`pvCount=2 pvs=... mode=BUCKET`). The first draft of `doc/metrics.md` invented a
   uniform `field: value` example from the format string; the real line was captured from a test
   run instead. `mode` renders as `BUCKET`/`SAMPLE`, not the proto enum's full name.

7. **The startup log line reports the configured Prometheus endpoint even when no port is bound.**
   Under `OTEL_METRICS_EXPORTER=none` the service still logs "telemetry initialized for service:
   query prometheus endpoint: 0.0.0.0:9475" — the exporter, not the port, is what the variable
   changed. An operator verifying an override took effect must check whether the port is listening
   rather than trust that line. (`DP_TELEMETRY_ENABLED=false` does log distinctly.) Noted in
   `doc/metrics.md`; the line itself is left alone as out of scope for a docs task.

8. **Only one shade execution is live, and it has the `ServicesResourceTransformer`.** The two
   per-service executions in `pom.xml` are commented out and carry only a
   `ManifestResourceTransformer`; re-enabling one would need the transformer added. Recorded in
   CLAUDE.md beside the invariant.

#### Verification performed

Configuration resolution was checked programmatically rather than by reading the files: all seven
new keys resolved under the main yml classpath equal their code `DEFAULT_*` constants (so adding
them changed nothing), and under the test classpath the shadowing behaves as intended
(`slowQueryLogThresholdMillis` 0, the rest at defaults). Both yml files and both log4j2 files were
parsed to confirm syntax.

Every operator-facing claim in `doc/metrics.md` was verified against a **running service** built
from the shaded jar rather than inferred from the source: the startup ordering (telemetry
initialized, then the gRPC port), a live `/metrics` scrape whose rendered names, label names, and
D9 `le` ladder match the doc's PromQL exactly; the bind-failure message quoted verbatim from a
real failure; `OTEL_METRICS_EXPORTER=none` and `DP_TELEMETRY_ENABLED=false` each starting the
service with no port bound; and the `-Ddp.<key>` override applying. The slow-query example line was
captured from an actual run. Four of the doc's statements were corrected as a result — findings
3, 4, 5, and 6 above.

Regression, after the config and log4j2 changes: unit suite **577 tests, 0 failures**; full
integration suite **344 tests across 60 classes, 0 failures**, 1 pre-existing skip — both matching
the Task 10 baseline exactly, so adding the seven config keys and the two loggers changed no
behavior.

This also discharges most of **Task 11**: the manual verification it describes was performed here,
against the shaded jar, as the means of checking the documentation. What remains for Task 11 is the
query-benchmark comparison.

### Task 10 — Tests

Status: done.

- `DpTelemetryTest` (unit, 5 tests): no-op before init and recording into it does not throw;
  `initForTest` receives measurements with the seconds unit, the D9 ladder and the dp scope;
  a reset rebinds the instruments to the next SDK with nothing leaking into the discarded one;
  the `workers.max` registration stops reporting when closed; `configureServerBuilder` leaves a
  servable in-process server.
- `DpTelemetryPrometheusTest` (unit, 3 tests): the only test that exercises the real export
  path — a genuine SDK, a bound port, and an HTTP scrape whose text is asserted. Covers the
  rendered histogram with every D9 boundary present as an `le` bucket, the `dp_service` and
  `otel_scope_name` labels, the JVM runtime families, the fail-closed bind error naming
  `Address already in use`, and a second `init()` as a no-op that binds no second endpoint.
- `QueueHandlerBaseMetricsTest` (unit, 7 tests, `common/handler`): a real `QueueHandlerBase`
  subclass driven by its real worker pool — queue wait and job duration with their attributes and
  seconds scaling, the queue wait a job actually incurred behind a blocked worker,
  `workers.active` reading 1 during execution and netting to zero after a job throws, the
  `workers.max` gauge reporting and then stopping at `fini()`, and the D8 attribute guard.
- `DpMongoCommandListenerTest` (7 tests, against `dp-test`): insert and find recording their
  operation, collection and namespace; a server-refused command recording `error.type`; a
  duplicate-key write recorded *without* `error.type`; `getMore` attributed to its collection;
  the command-shape parser; a collectionless command not inheriting the previous collection; and
  the D8 redaction guard on the filter.
- `TimedMongoCursorTest` (unit, 6 tests): document counting, accumulated time, a mid-iteration
  failure still contributing its time, the timed `close()`, `forEachRemaining` counted rather
  than delegated, and the pass-through accessors.
- `QueryMetricsIT` (`integration/query`, 4 tests): all five query methods over the gRPC channel
  recording the complete five-stage breakdown and the counters; a rejection counted as `reject`
  with no buckets and no db time; the slow-query line carrying the stage breakdown and the PV
  names; and the D8 guard on every query metric.
- `IngestionMetricsIT` (`integration/ingest`, 4 tests): a unary request's outcome, duration,
  buckets, samples and bytes; a rejection counted as `reject` with no buckets but with its
  offered load still counted; a stream recording one request per message; and the D8 guard.

#### What executing Task 10 established

1. **`error.type` never reflects a write error, only a command the server refused.** A
   duplicate-key insert throws to the caller while the driver emits `commandSucceeded` — the
   server answered the command and reported the write error inside the response body. So nothing
   in `error.type` will ever reflect a duplicate key, a failed document validation, or any other
   per-document write error. Found by writing the test against the plan's suggested duplicate-key
   failure and watching it record zero errors. Both halves are now pinned by their own tests
   (`testFailedCommandRecordsErrorType` uses a malformed command; `testWriteErrorIsNotACommandFailure`
   pins the negative), because an alert written on the assumption that every database exception
   raises this rate would never fire. Task 9's `doc/metrics.md` must say so.

2. **Every metric assertion in an IT has to wait for a measurement recorded after the response.**
   Both services record in a worker thread's `finally`, *after* the response the client is
   waiting on has been sent — `QueryTelemetry.complete()` runs once `executeAndDispatch` returns,
   and ingestion's recording happens on the worker long after the enqueue ack. A test asserting
   as soon as the stub returns is racing the recording it asserts on, and fails intermittently
   against entirely correct code. Both ITs poll `dp.*.requests` to a deadline rather than sleeping.
   This is not a test artifact: it is the same asynchrony `dp.ingest.duration` exists to expose.

3. **The same race exists in the unit test, one layer down.** `QueueHandlerBase` records
   `job.duration` and the `workers.active` decrement in the worker's `finally`, which runs after
   the job's own body. `QueueHandlerBaseMetricsTest` therefore enqueues a `BarrierJob` — a
   distinct class, so it gets its own `dp.job` point and cannot inflate the counts under test —
   and waits for it to start, which on a single-worker handler proves the previous job's finally
   block completed.

4. **The slow-query threshold cannot be set per test, so it is set for the suite.**
   `QueryTelemetry` resolves `QueryHandler.slowQueryLogThresholdMillis` once per JVM in a holder,
   and the ITs share one JVM; `ConfigurationManager` also folds `-D` overrides in once at
   singleton init, so a runtime `System.setProperty` has no effect either. The threshold is
   therefore set to `0` in `src/test/resources/application.yml` (added as part of this task,
   ahead of the rest of Task 9's config), making every test query slow so the line can be
   asserted. The production default of 1000 is unaffected.

5. **`IngestionTestBase.IngestionRequestParams.values` is indexed by column, not by row.** Each
   inner list holds one column's samples. Written the other way the request builder fails an
   assertion on the column count rather than producing a wrong result, so this cost only time —
   but it is worth recording next to the sample-count metric, which is rows × columns and would
   be silently wrong if the two were ever confused.

6. **Two requests for the same PV and the same start second are the same bucket**, so a streaming
   test that sends N identical requests records one success and N-1 errors rather than the
   throughput it means to measure. `IngestionMetricsIT` staggers its stream requests by one
   second each.

7. **The two ITs live beside their services' wrappers, not in a `telemetry` package.** The
   wrappers' `sendIngestData`/`sendIngestDataStream` helpers are `protected`, which is
   package-private to anything outside `integration.ingest` — so a separate telemetry package
   would have required widening the wrapper's access purely for the tests. Placing each IT in its
   service's package matches the existing convention and needed no production-side change.

#### Verification performed

Every new class was run individually and then as part of the full suites. Two assertions were
checked counterfactually rather than trusted: the Prometheus D9 ladder assertion was re-run
against a deliberately wrong boundary list and failed as it should, and
`QueueHandlerBaseMetricsTest` was run four times to confirm the recording barrier removed the
race rather than narrowing it. Unit suite: **577 tests, 0 failures** (549 before this task, so all
28 new unit tests ran). Full integration suite: **344 tests across 60 classes, 0 failures**, 1
pre-existing skip — 336/57 before this task, so all 8 new integration tests ran.

(A caution for anyone re-running these numbers: `target/failsafe-reports` accumulates across runs
and is not cleaned between them, so a class that has been renamed or moved leaves a stale report
behind that a naive tally counts twice. The first tally here read 348/61 for exactly that reason,
from a report left by an earlier package layout.)

### Task 11 — Manual verification (recorded in the PR description)

Status: done.

- ~~Build the shaded jar; run `QueryGrpcServer` from it with defaults; `curl :9465/metrics` shows
  `grpc_server_call_duration_seconds`, `dp_handler_*`, `db_client_operation_duration_seconds`,
  `jvm_*`. Repeat with `OTEL_METRICS_EXPORTER=none` (endpoint absent, service up) and
  `DP_QUERY_SERVER_METRICS_PORT` set to a port already in use (startup fails with the D6
  message).~~ **Done during Task 9**, as the means of checking the documentation against reality.
  All confirmed from the shaded jar: startup logs telemetry init before the gRPC bind (the D6
  ordering, in production rather than in a test); a real `queryData` over the wire produced the
  full `dp_query_*` set alongside `db_client_operation_duration_seconds`, `grpc_server_call_*`,
  and the `jvm_*` families, with the D9 `le` ladder and the documented label names; the bind
  failure produced `DpRuntimeException ... exception: BindException: Address already in use`
  naming the endpoint; `OTEL_METRICS_EXPORTER=none` and `DP_TELEMETRY_ENABLED=false` each started
  the service with nothing listening on the metrics port; and `-Ddp.QueryServer.metricsPort`
  overrode the config. Four documentation errors were caught this way (Task 9 findings 3-6).
- ~~Run the query benchmark against the instrumented server and confirm the stage histograms and
  the slow-query line agree with the benchmark's client-side timings to within the RPC overhead.~~
  **Done.** `BenchmarkQueryDataStream` against `BenchmarkQueryGrpcServer` from the shaded jar
  (1000 PVs, 10 per request, 5 threads, 60s of 1000Hz data = 240,000 buckets loaded), two runs.

#### What executing Task 11 established

1. **The benchmark reports no per-request client timing**, so "agree with the benchmark's
   client-side timings" could not be a per-request comparison as this bullet assumed when written.
   `queryScenario` times the whole thread pool with one `Instant` pair around `invokeAll` and
   reports only an aggregate values/sec rate; `QueryTaskResult` carries counts and bytes, no
   duration. The check performed instead is the one the numbers support: request **count**, the
   result **volume counters**, and total **thread-seconds** against the server-side stage sums.

2. **The three timing layers nest strictly, and the gaps are the RPC overhead.** Run 2, 100
   requests: client 17.750 thread-seconds (3.55s wall x 5 threads) >= `grpc_server_call_duration_
   seconds_sum` 16.138s >= `dp_query_stage_duration_seconds_sum{dp_stage="total"}` 15.245s. That
   is 16.1 ms/request between the client and the gRPC layer (wire, client-side decode, and pool
   idle) and **8.9 ms/request between the gRPC call span and the handler span** — the handler
   clock starts at handler entry, after the gRPC layer has decoded the request, which is exactly
   the interval this bullet called "the RPC overhead".

3. **Every result counter matches the client exactly.** `dp_query_buckets_total` = 60,000 =
   1000 PVs x 60 buckets; `dp_query_response_messages_total` = 200 = 2 per request;
   `dp_query_response_bytes_total` = 663,107,160, against the client's measured 186.84 MB/sec x
   3.55s = 663 MB. `dp_query_requests_total{dp_outcome="success"}` = 100, one per task, and all
   five stage histograms have count 100.

4. **The slow-query line and the histograms are the same measurement.** With
   `-Ddp.QueryHandler.slowQueryLogThresholdMillis=50`, all 100 requests logged a line, and the
   summed `totalMs` (15.193s) is **52 ms below** the histogram sum (15.245s) over 100 requests —
   0.52 ms/request, the sub-millisecond truncation of the line's integer-ms rendering against the
   histogram's nanosecond observations. The line's `buckets`/`messages`/`bytes` totals match
   their counters exactly (60,000 / 200 / 663,107,160), and each line's stage fields sum to its
   own `totalMs` within the per-field rounding.

5. **Zero slow-query lines at the shipped 1000 ms default was verified as correct behavior, not a
   broken logger.** The `dp_stage="total"` ladder put all 100 requests in the (0.1, 0.5] bucket
   (`le="0.1"` = 2, `le="0.5"` = 100), so nothing reached the threshold. Confirming this needed
   the histogram: an absent log line alone does not distinguish "nothing was slow" from "the
   logger is misrouted", which is the general hazard with a threshold-gated log.

6. **The stage decomposition is exact by construction and proves only arithmetic.** resolve +
   queue + db + process equalled total to 7e-15 s — inevitable, since `complete()` computes
   `process` as the residual. The load-bearing checks are the ones against independently measured
   quantities (findings 2-4); a future reader should not read the decomposition identity as
   evidence that the stages are correctly attributed.

7. **`doc/metrics.md` overstated the stage-fraction sum, and it was corrected.** It said the four
   fractions "should sum to roughly 1", which invites reading the sum as a consistency check. They
   sum to exactly 1 (measured 1.000000) because `process` is the residual, so the sum holds even
   when a stage is mis-attributed. Reworded to say so.

8. **The db stage dominates this workload: 76.8% of total** (113.9 ms of the 148.4 ms mean
   request in run 1), with process 23.1% and resolve + queue together under 0.2%. This is the
   stage-fraction query in `doc/metrics.md` returning a sensible answer on real traffic, and it
   says the query path's cost here is bucket retrieval rather than assembly or queueing.

9. **Run-to-run noise is ~3.5%** (17.52M vs 16.91M values/sec), consistent with the ~3% recorded
   for the ingestion benchmark. Stage sums should be compared across alternating runs, not
   between single runs, for anything finer than that.

#### Verification performed

- Two full benchmark runs from `target/dp-service-1.16.0-shaded.jar`, the second with the
  slow-query threshold lowered to 50 ms through the `-Ddp.` override (which also re-confirmed
  that override path on a second key).
- Baseline scrape before any traffic: no `dp_query_*` series present, `jvm_*` and
  `dp_handler_workers_max` present — so every series compared afterward was raised by the run.
- `BenchmarkQueryGrpcServer`'s dedicated metrics port (60452, D7) bound alongside the benchmark
  gRPC port (60052) with the telemetry init logged ahead of the gRPC bind, on both runs.
- No ERROR or exception in either server log; both ports released on shutdown.

#### Documentation changed by Task 11

- `doc/metrics.md`: the stage-fraction sum reworded from "should sum to roughly 1" to sum-by-
  construction, with the warning not to read it as a consistency check (finding 7); and a new
  "what these do not cover" entry recording that `dp.query.stage.duration` excludes wire time,
  with the measured ~9 ms and ~16 ms gaps (finding 2).
- `CLAUDE.md`: new subsection "The three timing layers nest, and the two gaps are not
  interchangeable", recording where the handler clock starts relative to the gRPC and client
  spans, the measured gaps, the non-joinable method labels, and the residual-`process` identity.
- `doc/metrics.md`: a **step 0** at the head of "Diagnosing a slow query" — "Is the time even
  inside the handler?" — with the PromQL that subtracts the handler mean from the gRPC mean. The
  workflow previously began at the stage histograms, which is exactly where a wire-time problem
  hides; step 0 is what makes the gap something an operator finds rather than something they have
  to already suspect. Selectors validated against the Task 11 scrape: each resolves to exactly one
  series (an ambiguous match would make the subtraction silently wrong), the difference is +8.9 ms,
  and an exact `grpc_method="queryDataStream"` match finds nothing, so the suffix match is
  required rather than stylistic.

#### Release documentation completed after Task 11

- `doc/runbooks/upgrade-1.16-slac.md` gained the metrics material the sequencing note called for:
  a **Before the window** prerequisite (the four ports, the check that nothing holds them, the
  no-auth/no-TLS warning, and the two off-switches) because an unbindable port is the one 1.16.0
  change that can stop a service coming back up inside the window; and an **After the upgrade**
  verification step using `rate(dp_query_buckets_total[5m]) / rate(dp_query_requests_total[5m])`,
  since buckets-read-per-query is the most direct evidence of whether #232's bound did its job.
- **A `dp_service` label collision was found and fixed in `doc/metrics.md`.** The example scrape
  config attached `dp_service` as a *target* label, but `dp_handler_*` already carries a
  `dp_service` attribute from the code (verified in the Task 11 scrape: `dp_service="query"` on
  all eight handler series). Prometheus resolves that collision by keeping the target's value and
  renaming the original to `exported_dp_service`, so following the doc's own example would have
  silently broken every handler query written against `dp_service`. Renamed to `dp_instance` with
  the reason recorded. This is the class of error that only appears when the doc is read as
  instructions rather than as prose.

#### A race in the slow-query IT, found at commit time

The first full IT run after the documentation work failed with "no slow query line was produced"
in `QueryMetricsIT` (344 tests, 1 failure). It was **not** an ordering dependency: the class
passes in isolation, passes with its immediate predecessor, passes with its whole package, and
passed on a second unchanged full run. The cause is a genuine race in the test —
`QueryTelemetry.complete()` increments `dp.query.requests` *before* it writes the slow-query
line, so `awaitRequestRecorded()` can return in the window between the two, and a loaded
full-suite run widens that window enough to lose.

Fixed in the test by waiting for the line itself (`awaitSlowQueryLines`), kept deliberately
separate from `awaitRequestRecorded` so that a test awaiting one signal cannot silently get the
other. The production ordering is left alone — recording the metric before writing the log line
is correct, since the metric is the thing that must not be lost.

Worth noting for anyone adding a telemetry IT: **any assertion that reads the slow-query appender
after awaiting a metric has this race**, and it will pass locally almost every time.

#### Kubernetes deployment and PromQL verification (post-PR, for the 1.16.0 release)

The customer deployment (TIDF) runs Prometheus/Grafana on Kubernetes, managed by their platform
team — they need to point a scrape at a URL. That relocated the remaining gap from "no dashboard"
to "nothing tells a k8s operator which port to name", and prompted verifying the PromQL for real.

1. **The released image declared the wrong port.** `Dockerfile` carried `EXPOSE 8080`, which no
   service in this repo listens on, and declared neither the gRPC nor the metrics ports. The image
   (`ghcr.io/osprey-dcs/dp-service`, published by `release-image.yml` on a tag) is what the customer
   deploys, so that line is the machine-readable hint their platform team reads. Now declares all
   eight real ports, with a comment that `EXPOSE` documents rather than publishes and that k8s
   reaches a `containerPort` regardless. Verified by building a stub image and reading
   `.Config.ExposedPorts`: exactly `[9464-9467, 50051-50054]`, and 8080 gone.

2. **`doc/metrics.md` gained an "On Kubernetes" section** — `containerPort` with a named port, a
   `ServiceMonitor` (the Prometheus Operator form, which is what "already set up by the k8s guys"
   usually means), and the `prometheus.io/*` annotation fallback. Includes the two traps: a
   `ServiceMonitor` whose labels miss the Operator's `serviceMonitorSelector` is *silently* ignored,
   and a metrics port collision is `CrashLoopBackOff` rather than a pod running without metrics,
   because the bind failure is deliberate.

3. **Every PromQL query in the doc was executed against a real Prometheus** (`prom/prometheus` in
   Docker, scraping a live instrumented service, with the benchmark generating sustained traffic so
   `rate()` had two points in its window). All 16 parse and execute — but two returned **empty**:

   **`buckets read per query` and `bytes per request` were broken.** Both divided
   `rate(dp_query_buckets_total[...]) / rate(dp_query_requests_total[...])` directly, but
   `dp_query_requests_total` carries a `dp_outcome` label the other counter does not, so vector
   matching found no pair and the result was empty — no error, just a blank panel. This is the
   third instance of the same label-matching class in this ticket's PromQL, and the one that got
   past the earlier selector-level checking, which is exactly what executing the queries was for.
   Both now aggregate with `sum by (rpc_method)` on each side, and the doc explains the rule.
   Verified: 600 buckets/query (10 PVs x 60 buckets — correct for the benchmark) and 6,631,071
   bytes/request (matching the Task 11 byte total).

4. **The `dp_instance` fix was confirmed against a live Prometheus.** Scraping with a
   `dp_instance` target label leaves `dp_service` at its code-emitted value (`query`) and creates no
   `exported_dp_service` — the collision the doc now warns about does not occur.

5. **The Mongo command metrics are absent from the *benchmark* server's scrape, and that is a
   benchmark quirk rather than a shipping bug.** `BenchmarkQueryGrpcServer.main()` calls
   `prepareBenchmarkDatabase()` *before* `server.start()`, so its first Mongo client is built while
   `DpTelemetry` is still the no-op instance, and `DpMetrics`' lazily created histogram is cached
   against the no-op meter. The production servers call `start()` first: a real `QueryGrpcServer`
   exports **314 `db_client_operation_*` series**, and the step-3 query returns 24 correctly
   labelled series there. Worth knowing before anyone reads a benchmark scrape and concludes the
   Mongo instrumentation is broken; a follow-on could reorder the benchmark main().

#### Why the clock was not moved earlier

Considered and rejected during Task 11, recorded so it is not re-opened without the reasoning:

- **Moving the `QueryTelemetry` clock to the service layer does not close the gap.** The gRPC
  layer decodes the request before any dp code runs, so a service-layer clock still starts after
  decode — it would shrink the measured 8.9 ms without eliminating it, produce a number subtly
  different from the one every stage baseline was taken against, and still not cover response
  transmission, which on a 6.6 MB response is likely the larger half.
- **Making the two families joinable was rejected on D8 grounds.** Adding a fully-qualified
  method attribute to `dp.query.*` doubles the method cardinality on every dp query series to buy
  a join the documented suffix match already provides. If a facility wants a real join it belongs
  in Prometheus relabeling, which is a dashboard concern, not a service change.
- **The wire time is not unmeasured.** `grpc_server_call_duration_seconds` covers the whole call,
  including decode and transmission, and is exported today. The gap was never a blind spot in the
  data — only in the workflow, which is why the fix is step 0 rather than new instrumentation.

## Out of scope

- **Distributed tracing / spans / exemplars** — follow-on ticket to be filed when this one closes
  ("OpenTelemetry tracing for query and ingestion"); the instrumentation points in D3 are where
  the spans go, and the OTel agent is the candidate for auto-instrumented gRPC/Mongo spans.
- **A local Prometheus + Grafana developer stack** — filed as
  [#278](https://github.com/osprey-dcs/dp-service/issues/278). Its most valuable piece is a check
  that executes the doc's PromQL, since manually doing that once during Task 11 is what caught two
  queries that returned empty.
- **A starter Grafana dashboard** — deliberately deferred rather than built blind. The doc's PromQL
  is a diagnostic workflow walked on a complaint; a dashboard is a monitoring surface, and what
  belongs on it depends on what the deployment watches. Ask the customer once data is flowing;
  #278 makes building it cheap.
- **Counting gRPC-layer validation rejects** — a request that fails `QueryServiceImpl`'s field
  validation is rejected before any `QueryTelemetry` exists, and the rejection travels as an `OK`
  response carrying an `ExceptionalResult`, so it raises neither `dp.query.requests` nor a non-`OK`
  `grpc_status`. Counting it needs a telemetry context created at the service layer rather than in
  the handler; found while writing Task 9's docs and documented in `doc/metrics.md` as a known gap.
- **Stage telemetry for the query metadata methods** (`queryPvMetadata`, `queryProviders`,
  `queryProviderStats`, `queryPvStats`) — they are not bucket retrieval, and their database work is
  a single aggregate rather than a cursor iterated during dispatch, so the stage model does not fit
  them as it stands.
- **Mongo connection-pool metrics** (`ConnectionPoolListener`) — seven workers against a default
  pool of 100 cannot contend today; add with the pool-size configuration if that changes.
- **Per-request telemetry for `queryDataBidiStream`** — the dispatcher serves many requests on
  one stream; it is measured per stream here. Its use is the legacy client path.
- **Annotation-service and ingestion-stream-specific counters** (export sizes, event-monitor
  triggers) — both services get the generic gRPC / handler / Mongo layer from this ticket.
- **Per-PV statistics** (#201) and **operator-visible conditions** (#202) — the slow-query log
  is a natural feed for #202's collection, but that is #202's design.
- **Per-request client timing in the query benchmark** — `queryScenario` times the whole thread
  pool and reports only an aggregate values/sec rate, so there is no client-side per-request
  duration to compare against `dp_query_stage_duration_seconds` (Task 11 finding 1). Recording a
  per-task duration in `QueryTaskResult` would let a benchmark run assert the client/server gap
  per request rather than in aggregate; that is a benchmark change, for `doc/benchmark-overview.md`'s
  owner alongside the ingestion item below.
- **The ingestion benchmark measuring persistence** — with `dp.ingest.buckets` exported, the
  benchmark could read the server's counter instead of polling the collection count; that is a
  benchmark change for `doc/benchmark-overview.md`'s owner.
- **CI running the new ITs** — #250 (CI does not run integration tests) is unchanged; the unit
  tests in Task 10 run under `mvn test`, the ITs under the failsafe package selection.

## Dependencies and sequencing

- Task 1 (build) first; Tasks 2–3 (bootstrap, instruments) next; everything else depends on
  them. Tasks 4, 5, 8 are independent of each other and of Task 6 once 2–3 exist. Task 7 must
  land before the ITs in Task 10 — not, as this said before Task 7 was executed, because those ITs
  observe gRPC metrics (the in-process transport emits none, Task 7 finding 7), but because Task 7
  is what installs the `InMemoryMetricReader` the ITs collect from, in `GrpcIntegrationTestBase`.
  Task 9's CLAUDE.md and docs are written last, from the code as landed.
- No dp-grpc change: the ticket adds no proto field. `io.grpc.version` must stay equal to
  dp-grpc's; a dp-grpc gRPC upgrade must bump `grpc-opentelemetry` in the same change.
- No schema migration: nothing stored changes.
- Does **not** block on #257/#198/#201: the `db` stage and the Mongo command histogram are what
  will show whether those tickets' index work pays off, which is a reason to land this first.
- Deployment note for the release: four new listening ports, on by default. The SLAC upgrade
  runbook (`doc/runbooks/upgrade-1.16-slac.md`) gets a line if this ships in the same release.
