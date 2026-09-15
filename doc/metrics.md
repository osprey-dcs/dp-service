# Metrics

Every MLDP service exports metrics over a Prometheus scrape endpoint, on by default. This document
is the operator's reference: what is measured, what the numbers mean, what they deliberately do not
cover, and the PromQL to answer the question the metrics exist for — **why was this query slow**.

Added by [issue #212](https://github.com/osprey-dcs/dp-service/issues/212).

## Contents

- [Quick start](#quick-start)
- [On Kubernetes](#on-kubernetes)
- [Configuration](#configuration)
- [Exporting somewhere other than Prometheus](#exporting-somewhere-other-than-prometheus)
- [The metrics](#the-metrics)
  - [Query](#query-metrics)
  - [Ingestion](#ingestion-metrics)
  - [Handler (all services)](#handler-metrics-all-services)
  - [Database](#database-metrics)
  - [gRPC and JVM](#grpc-and-jvm-metrics)
- [Query stages](#query-stages)
- [The slow query log](#the-slow-query-log)
- [Diagnosing a slow query](#diagnosing-a-slow-query)
- [What these metrics do not cover](#what-these-metrics-do-not-cover)
- [Cardinality: why there are no PV names here](#cardinality-why-there-are-no-pv-names-here)

## Quick start

Start any service normally and scrape its metrics port:

```
curl -s localhost:9465/metrics | grep '^dp_query'
```

| Service          | gRPC port | metrics port |
|------------------|-----------|--------------|
| Ingestion        | 50051     | 9464         |
| Query            | 50052     | 9465         |
| Annotation       | 50053     | 9466         |
| Ingestion Stream | 50054     | 9467         |

The benchmark servers use 60451 (ingestion) and 60452 (query), matching the 6005x convention their
gRPC ports already follow. They must differ from the production ports: an unbindable metrics port
fails startup, so a benchmark run on a host running the live service would otherwise not start.

**The benchmark servers export no `db_client_operation_*` metrics**, and that is a property of the
benchmark entry point rather than of the instrumentation. Their `main()` prepares the benchmark
database *before* calling `start()`, so the first Mongo client is built while telemetry is still the
no-op instance and the histogram is cached against it. The production servers initialize telemetry
first and export the family normally. Do not read a benchmark scrape as evidence that the database
instrumentation is broken.

A minimal Prometheus scrape config for all four:

```yaml
scrape_configs:
  - job_name: dp-service
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9464']
        labels: {dp_instance: ingestion}
      - targets: ['localhost:9465']
        labels: {dp_instance: query}
      - targets: ['localhost:9466']
        labels: {dp_instance: annotation}
      - targets: ['localhost:9467']
        labels: {dp_instance: ingestionstream}
```

**Do not name that target label `dp_service`.** The `dp_handler_*` series already carry a
`dp_service` attribute from the code, and a target label of the same name collides with it: by
default Prometheus keeps the target's value and renames the original to `exported_dp_service`, so
every query written against `dp_service` silently stops matching the handler metrics. The label
above is only for telling the four endpoints apart, which is what `dp_instance` does without
shadowing anything. The `job` label Prometheus adds on its own is usually enough.

### On Kubernetes

Prometheus needs a target, and on Kubernetes that means naming the port on the pod. The metrics
port is an ordinary container port — declare it alongside the gRPC port and give it a name, because
both the `ServiceMonitor` and the annotation form below refer to the port by name:

```yaml
    ports:
      - name: grpc
        containerPort: 50052
      - name: metrics          # the name the scrape config targets
        containerPort: 9465
```

If the cluster runs the **Prometheus Operator** (a `ServiceMonitor`/`PodMonitor` CRD exists), that
is the idiomatic target. It selects a `Service` in front of the pods, and `port` is the *`Service`
port name*, not the container's:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: dp-query
  labels:
    release: prometheus       # must match the Operator's serviceMonitorSelector, or it is ignored
spec:
  selector:
    matchLabels:
      app: dp-query           # matches the Service, not the Pod
  endpoints:
    - port: metrics
      interval: 15s
```

A `ServiceMonitor` the Operator's `serviceMonitorSelector` does not match is silently ignored — no
error, just no data — so if a target never appears, check that label first. Which label the cluster
expects is a property of how the Operator was installed; ask whoever installed it rather than
guessing.

Without the Operator, the annotation form works with a stock kubernetes-pods scrape config:

```yaml
  template:
    metadata:
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9465"
        prometheus.io/path: "/metrics"
```

Three things specific to these services:

- **Each service is its own deployment with its own port.** The four ports exist so all four can run
  on one host; in separate pods they could all use the same number, but the defaults differ, so the
  port must match the service the pod runs. The image's `Main-Class` starts *ingestion*; the other
  three are started by overriding the container command.
- **A pod whose metrics port is already in use will not start.** Metrics fail closed by design (a
  service silently running without them is discovered only when someone goes looking), so a port
  collision is `CrashLoopBackOff`, not a running pod without metrics. `DP_TELEMETRY_ENABLED=false`
  disables the endpoint and binds nothing.
- **Do not add a `dp_service` target label.** See the warning above — it collides with the attribute
  the services already emit. Kubernetes SD labels (`pod`, `namespace`, `container`) do not collide
  and are worth keeping: with several replicas they are how one pod's numbers are told from another's.

The endpoint has **no authentication and no TLS.** It exposes operational metrics, not archive data,
but it should not be reachable outside the cluster — do not put it behind an Ingress.

## Configuration

All settings live in `application.yml` and take the usual `DP_*` environment overrides.

| Setting | Environment override | Default | Meaning |
|---|---|---|---|
| `Telemetry.enabled` | `DP_TELEMETRY_ENABLED` | `true` | Collect and export metrics at all |
| `Telemetry.prometheusHost` | `DP_TELEMETRY_PROMETHEUS_HOST` | `0.0.0.0` | Interface the scrape endpoint binds to |
| `IngestionServer.metricsPort` | `DP_INGESTION_SERVER_METRICS_PORT` | `9464` | Ingestion scrape port |
| `QueryServer.metricsPort` | `DP_QUERY_SERVER_METRICS_PORT` | `9465` | Query scrape port |
| `AnnotationServer.metricsPort` | `DP_ANNOTATION_SERVER_METRICS_PORT` | `9466` | Annotation scrape port |
| `IngestionStreamServer.metricsPort` | `DP_INGESTION_STREAM_SERVER_METRICS_PORT` | `9467` | Ingestion Stream scrape port |
| `QueryHandler.slowQueryLogThresholdMillis` | `DP_QUERY_HANDLER_SLOW_QUERY_LOG_THRESHOLD_MILLIS` | `1000` | Slow query log threshold; `0` logs every query, negative disables |

Each can also be set as a JVM property with a `dp.` prefix, before the class name:
`-Ddp.QueryServer.metricsPort=19465`.

**Metrics are on by default, and a metrics port that cannot be bound fails service startup.** This
is deliberate, and it is the one deployment-visible consequence of enabling metrics: a second port
per service must be free. The alternative — starting anyway and running without metrics — produces
a service an operator believes is instrumented and is not, which is discovered only when someone
goes looking for a number that is not there. The startup error names the endpoint and the root
cause:

```
error initializing telemetry for service: query with prometheus endpoint: 0.0.0.0:9465
  exception: BindException: Address already in use
```

To turn metrics off entirely, set `DP_TELEMETRY_ENABLED=false`. Instrumentation still runs but
records into a no-op implementation, and no port is bound.

**Restrict these ports at the firewall to the monitoring host.** The default bind is `0.0.0.0`, so
on a routable interface the endpoint is readable by anything that can reach the host.

The default is deliberately not loopback. Prometheus scrapes these ports over the network — in
Kubernetes it reaches a pod's metrics port across the pod network, not the pod's loopback — so
`127.0.0.1` silently yields no data in every scrape topology documented above. Because metrics fail
closed on a *bind* failure but not on an unreachable one, that misconfiguration produces no error
anywhere: the service starts, the port is bound, and the target simply never reports. Binding wide
and restricting at the firewall is the combination that works. Note also that the gRPC ports
(50051–50054) already bind all interfaces and serve archive data in plaintext, so a loopback metrics
port would not change what an attacker with network access can reach.

Set `DP_TELEMETRY_PROMETHEUS_HOST=127.0.0.1` only when the scraper or sidecar runs on the service
host itself, where it is both safe and sufficient.
**There is no authentication on the scrape endpoint.** It carries no data values and no PV names
(see [cardinality](#cardinality-why-there-are-no-pv-names-here)), but it does reveal request rates,
latencies, and collection names, so on a shared host bind it to the loopback interface.

## Exporting somewhere other than Prometheus

The SDK is configured through `AutoConfiguredOpenTelemetrySdk` with dp-supplied defaults, so every
standard `OTEL_*` environment variable overrides what the service chose. No rebuild, no code change.

Push to an OpenTelemetry collector instead of being scraped:

```
OTEL_METRICS_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317
```

Leave instrumentation on but export nothing (useful for measuring instrumentation overhead):

```
OTEL_METRICS_EXPORTER=none
```

Change the export interval for a push exporter: `OTEL_METRIC_EXPORT_INTERVAL=30000`.

Note when checking that an `OTEL_*` override took effect: the startup line still reads
`telemetry initialized for service: query prometheus endpoint: 0.0.0.0:9465` even under
`OTEL_METRICS_EXPORTER=none` or `otlp`, because it reports the configured port rather than what the
exporter did with it. Confirm by whether the port is actually listening, not by that line.
(`DP_TELEMETRY_ENABLED=false` does log distinctly: "telemetry disabled by config key".)

The service sets `otel.service.name` to its own name (`ingestion`, `query`, `annotation`,
`ingestionstream`), and sets the traces and logs exporters to `none` — autoconfigure defaults both
to `otlp`, so without that a metrics-only deployment would try to reach a collector on
`localhost:4317` and log an export failure on every interval. Setting `OTEL_TRACES_EXPORTER`
yourself overrides that; there are no spans to export yet (tracing is a follow-on).

## The metrics

Names below are the OpenTelemetry instrument names. **Prometheus renders them differently**: dots
become underscores, the unit is appended, and counters gain `_total`. So `dp.query.requests`
appears in a scrape as `dp_query_requests_total`, and `dp.query.stage.duration` (unit `s`) as
`dp_query_stage_duration_seconds_bucket` / `_count` / `_sum`. Attribute `dp.service` renders as
label `dp_service`.

Every duration is in **seconds**, per OpenTelemetry semantic convention, and every duration
histogram uses the same explicit bucket ladder:

```
0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0
```

It runs to 120 s because queries against a large archive have been measured taking minutes; the top
of the range has to be wide enough that a pathological query is distinguishable from a merely slow
one rather than every slow query piling into `+Inf`. Note the consequence for percentiles: with ten
buckets, `histogram_quantile` interpolates within a bucket, so a p99 reported as 0.5–1.0 s means
"somewhere in that decade", not a precise figure. The buckets are for spotting a shift, and the
slow-query log is for the individual request.

### Query metrics

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `dp.query.stage.duration` | histogram | s | `rpc.method`, `dp.stage` | Time in one stage of handling a query — see [Query stages](#query-stages) |
| `dp.query.requests` | counter | — | `rpc.method`, `dp.outcome` | Completed query requests |
| `dp.query.buckets` | counter | — | `rpc.method` | Bucket documents read from MongoDB serving queries |
| `dp.query.response.messages` | counter | — | `rpc.method` | Result-bearing response messages sent to clients |
| `dp.query.response.bytes` | counter | By | `rpc.method` | Serialized bytes of those messages, as sent on the wire |

The two response counters cover **result-bearing** messages only. Reject and error
(`ExceptionalResult`) responses, and the empty payloads sent before a page is assembled, are not
counted: they carry a status rather than data, and a near-constant term would distort a counter
whose purpose is measuring result volume. Those requests are identified by `dp.outcome` on
`dp.query.requests` instead.

The consequence for a dashboard: `dp.query.response.bytes / dp.query.requests` is bytes per
**request** — a rejected request contributes a denominator with no numerator. Divide by
`dp.query.response.messages` for a per-message average. The byte count is the size of the message
actually put on the wire, including the response envelope and its timestamp, not the nested result
payload.

`rpc.method` is the bare method name, and covers the eight **data retrieval** methods:

| V2 | `queryBuckets`, `queryBucketsStream`, `querySamples`, `querySamplesStream` |
|---|---|
| V1 (legacy) | `queryData`, `queryDataStream`, `queryDataBidiStream`, `queryTable` |

(The gRPC metrics label the same call differently — see [gRPC and JVM](#grpc-and-jvm-metrics).)

**The metadata methods are not covered by any `dp.query.*` metric**: `queryPvMetadata`,
`queryProviders`, `queryProviderStats`, and `queryPvStats` have no stage breakdown, no outcome
counter, and no slow-query line. They are not bucket retrieval — the concern this instrumentation
was built for — and their database work is a single aggregate rather than a cursor iterated during
dispatch. They remain visible in `grpc.server.call.duration` and in `db.client.operation.duration`
(by collection: `pvMetadata`, `providers`, `pvStats`), which is enough to see them slow down.
Extending the stage telemetry to them is a follow-on if it is ever needed.

`dp.outcome` has four values, and the distinctions are the ones an alert needs:

- **`success`** — data returned.
- **`empty`** — succeeded, returned nothing. Separate from `success` because a query returning
  nothing quickly is usually a client asking for the wrong window, and folding it into `success`
  would dilute the latency distribution with a population of near-zero requests.
- **`reject`** — the client sent something wrong (a malformed page token, an unresolvable selector,
  a streaming request carrying a page token). Per the repo's #235 classification this is a client
  mistake, not a service failure.
- **`error`** — the service failed to handle a valid request.

A rising `reject` rate means clients are sending something wrong; a rising `error` rate means the
service is failing. **An alert that cannot tell them apart pages the wrong person**, which is why
these are one attribute and not one counter.

Requests rejected during **resolution** — a malformed page token, an unresolvable selector, a
streaming request carrying a page token — are counted, even though no job is ever enqueued for them.
They would otherwise be among the most common client failures and report a reject rate of exactly
zero.

**Requests rejected by gRPC-layer request validation are not**, and this is a real gap to know
about. A request malformed enough to fail `QueryServiceImpl`'s field validation — an empty
`QueryDataRequest`, a missing time range — is rejected before the handler is entered, so no
`QueryTelemetry` exists and nothing increments `dp.query.requests`. Verified against a running
service: an empty `queryData` request produced two `grpc_server_call_started_total` for the method
and only one `dp_query_requests_total` point.

**And they are not visible as a gRPC error either.** The service reports a rejection as a normal
response carrying an `ExceptionalResult` payload, not as a gRPC error status — verified: the
rejected request above was recorded as `grpc_status="OK"`. So such a request appears in the metrics
only as an extra `grpc_server_call_started_total` with no corresponding `dp_query_requests_total`
point.

The one signal available is the difference between the two counts, which is awkward to express
because the label sets differ (`grpc_method` is fully qualified, `rpc_method` is bare) and
`grpc_server_call_started_total` carries no status label at all:

```promql
sum(rate(grpc_server_call_started_total{grpc_method=~".*/queryData"}[5m]))
  - sum(rate(dp_query_requests_total{rpc_method="queryData"}[5m]))
```

A persistently positive difference means requests are being rejected at validation. It is a blunt
instrument — a stream method's accounting differs, and the two rates are sampled independently — so
treat a non-zero value as a prompt to read the service log, where each rejection is logged with its
reason, rather than as a number to alert on directly.

Instrumenting the validation rejects properly is a follow-on; it needs a telemetry context created
at the service layer rather than in the handler.

### Ingestion metrics

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `dp.ingest.requests` | counter | — | `dp.outcome` | Ingestion requests whose handling completed |
| `dp.ingest.duration` | histogram | s | `dp.outcome` | Arrival to end of handling, **including persistence** |
| `dp.ingest.buckets` | counter | — | *(none)* | Bucket documents the database acknowledged writing |
| `dp.ingest.samples` | counter | — | *(none)* | Individual samples ingested (rows × columns) |
| `dp.ingest.request.bytes` | counter | By | *(none)* | Serialized bytes of requests handled |

The last three carry no attributes at all — they are whole-service totals. Splitting them by
outcome would invite a ratio computed over a filtered denominator; see the next paragraph.

**These cover data ingestion only** — the `ingestData`, `ingestDataStream`, and `ingestDataBidiStream`
payload path, recorded in `IngestDataJob`. `registerProvider` and the subscription methods are not
counted here; they appear in `grpc.server.call.duration` and, for their database work, in
`db.client.operation.duration`. "Ingestion requests" in these metrics means data requests.

**`dp.ingest.duration` is the ingestion number that matters, and `grpc.server.call.duration` is
not.** Ingestion acknowledges a request as soon as it is validated and enqueued; persistence —
provider lookup, bucket generation, `insertMany`, request-status insert, subscription publish —
happens afterwards on a worker thread. The gRPC call duration therefore measures validation and
enqueue only. An operator watching it would see a healthy-looking few milliseconds while the queue
behind it fell arbitrarily far behind. `dp.ingest.duration` measures from request arrival to the
end of handling, which is the latency a data source actually experiences.

`dp.ingest.samples` and `dp.ingest.request.bytes` are counted on **every** outcome; `dp.ingest.buckets`
only on success, and it records what the database acknowledged rather than the batch size — the two
differ exactly when `insertMany` partially failed, which is the case worth seeing. That combination
keeps `dp.ingest.buckets / dp.ingest.samples` readable as "what fraction of the offered load was
actually stored". Excluding failures from the denominator would make that ratio read as 1.0 no
matter how much was being dropped.

On a streaming call, arrival is stamped **per request, not per stream**, so a long-lived bidi
stream does not report durations that grow with the stream's age.

**The two families are not counting the same thing, so their counts will not agree.**
`grpc.server.call.duration` counts *calls* — one per stream — while `dp.ingest.*` counts
*requests*, of which a stream carries many. A benchmark run measured 50 gRPC calls against 3,000
ingestion requests (60 requests per stream): a mean "call" of 1.21 s next to a mean request of
52 ms. Neither number is wrong and neither is a latency the other can be checked against. Use
`dp.ingest.duration` for how long an ingestion took, and read a gRPC call duration on a streaming
method as the stream's lifetime.

### Handler metrics (all services)

Every service's request handler is a worker pool, and the base class times every job — so every
current and future job type is covered without touching the job.

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `dp.handler.queue.wait` | histogram | s | `dp.service`, `dp.job` | Job creation to the start of `execute()` |
| `dp.handler.job.duration` | histogram | s | `dp.service`, `dp.job` | Wall time of `execute()` |
| `dp.handler.workers.active` | up-down counter | — | `dp.service` | Workers currently executing a job |
| `dp.handler.workers.max` | gauge | — | `dp.service` | Configured worker count (`*Handler.numWorkers`) |

`dp.service` is `ingestion`, `query`, `annotation`, or `ingestionstream`. `dp.job` is the job class
name (`IngestDataJob`, `QueryV2Job`, `SaveAnnotationJob`, …).

`workers.active` is attributed by service only, not by job: an up-down counter split by job class
would make "are workers saturated" a sum across a dozen series that individually mean nothing.

**There is deliberately no queue-depth gauge**, and its absence is not an oversight. The request
queue has capacity 1, so depth reads 0 or 1 forever; backpressure shows up as the gRPC thread
blocking to enqueue. The two signals that carry the same information are `queue.wait` — a
distribution, not an instantaneous sample — and `workers.active` against `workers.max`. Sustained
saturation is `workers.active` pinned at `workers.max` with `queue.wait` climbing.

`workers.active` nets to zero even when a job throws.

### Database metrics

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `db.client.operation.duration` | histogram | s | `db.operation.name`, `db.collection.name`, `db.namespace`, `error.type` | One MongoDB command round-trip |

Named for the OpenTelemetry database semantic conventions rather than with a `dp.` prefix, so a
dashboard or alert written against the convention works here without translation. Recorded by a
driver `CommandListener`, so it covers every command the service issues, from any code path.

Four properties of this metric are not what a reader would assume, and each was verified against a
real MongoDB rather than reasoned about:

1. **`error.type` covers only commands the server refused — never a write error.** A duplicate-key
   insert, a failed document validation, or any other per-document write error is carried in the
   *response body* of a command the server answered successfully, so the driver reports
   `commandSucceeded` while throwing to the caller. Nothing in `error.type` will ever show a
   duplicate key. An alert written on the assumption that every database exception raises this rate
   would never fire. Use the application-level outcome counters (`dp.ingest.requests{dp_outcome="error"}`)
   for write failures.

2. **A total database outage makes this metric go silent, it does not raise an error rate.** A
   server-selection failure emits no command events at all — verified: against an unreachable
   server, a count threw `MongoTimeoutException` having produced neither a started nor a failed
   event. So alert on **absence of data** for a database outage, not on `error.type`.

3. **`error.type` is the driver event's exception class, which is not always the one the caller
   catches.** A bad index hint arrives on the event as `MongoCommandException` while the caller
   sees the driver's `MongoQueryException` wrapper. This matters for the #271 missing-index guard:
   an alert written against the caller-facing class name would never fire.

4. **`countDocuments()` issues an `aggregate`, not a `count`.** Only `estimatedDocumentCount()`
   issues `count`. A panel filtering `db_operation_name="count"` would miss essentially every count
   this codebase performs.

Commands that name no collection (`hello`, `dropDatabase`, `endSessions`) carry no
`db.collection.name` attribute at all rather than an empty one. `getMore` is attributed to the
collection it is fetching from, not to the cursor id.

### gRPC and JVM metrics

From the grpc-java and OpenTelemetry runtime instrumentation libraries, unmodified:

- `grpc.server.call.duration` (histogram, seconds), `grpc.server.call.started`, and the sent/received
  message-size histograms, labeled **`grpc_method` and `grpc_status`** — *not* `rpc.method`. The dp
  attribute vocabulary governs dp-emitted attributes; these series come from grpc-java's own
  instrumentation, under scope `grpc-java`.

  **`grpc_method` is the fully-qualified method**, not the bare name the dp metrics use:

  ```
  grpc_server_call_duration_seconds_count{grpc_method="dp.service.query.DpQueryService/queryData",grpc_status="OK"}
  dp_query_stage_duration_seconds_count{rpc_method="queryData",dp_stage="total"}
  ```

  So the two families cannot be joined on the method label without rewriting one of them; match on
  the suffix (`grpc_method=~".*/queryData"`) when correlating them.
- `jvm_memory_used_bytes`, `jvm_gc_duration_seconds`, `jvm_thread_count`, `jvm_class_loaded`, and
  the rest of the JVM family — so a query that slowed down because the process is in GC trouble is
  diagnosable from the same endpoint as the query metrics themselves.

For query methods, `grpc.server.call.duration` is the correct end-to-end number: every query
dispatcher completes the RPC itself at the end of the job, so the RPC duration spans resolve, queue
wait, and the job. The stage histograms explain it. For **ingestion** methods it is not — see
[Ingestion metrics](#ingestion-metrics).

## Query stages

`dp.query.stage.duration` splits handling into five stages, carried on the `dp.stage` attribute.
Four are disjoint and sum to the fifth.

| Stage | What it covers |
|---|---|
| `resolve` | Handler entry to enqueue. For the V2 methods this includes the resolver's own database reads — PV existence, name-pattern expansion, configuration activations — which can be substantial before the request ever reaches a worker. For the legacy methods it is a small handler-entry-to-enqueue interval. |
| `queue` | Waiting for a worker. The same number recorded in `dp.handler.queue.wait`, taken from the job rather than measured again, so the two cannot drift. |
| `db` | Time attributable to the database: the `executeQuery*()` call plus all time spent inside the result cursor. |
| `process` | Everything else — assembly, serialization, and the `onNext` calls. Derived as `total − resolve − queue − db`, clamped at zero. |
| `total` | Handler entry to completion. |

**Two things about the `db` stage.**

It has to include cursor time, and this is the whole reason for measuring it with a decorator rather
than timing the query call. Opening a cursor fetches only the first batch; every subsequent batch is
a `getMore` issued inside `hasNext()`/`next()` as the dispatcher iterates while building and sending
responses. Measured on a 50-bucket V2 query: `db` was 0.0202 s of a 0.0405 s total. Timing only the
`executeQuery*()` call would have attributed nearly all of that to `process`.

And it is an **upper bound** on database time, not a measurement of it: the time the dispatcher
spends inside the cursor includes the driver's BSON decode of each document. That is attributable to
retrieval rather than assembly, so it belongs here — but it means `db` is not purely server time.
`db.client.operation.duration` is the server-side view. The two together separate "the server is
slow" from "we are decoding a great deal": if `db` greatly exceeds the sum of the command durations
over the same period, the time is in decode, not in MongoDB.

`queryDataBidiStream` is measured to its **first** response, not to the end of the stream, and is
not represented in these histograms beyond that point. Recording per pull would count one client
request as many in `dp.query.requests`, inflating the request rate by the client's page count;
holding the context open until the stream closes is unbounded for an abandoned stream. Later pulls
remain visible in `db.client.operation.duration` and `grpc.server.call.duration`.

## The slow query log

A query whose total handling time reaches `QueryHandler.slowQueryLogThresholdMillis` (default 1000)
writes one WARN line to the logger named **`dp.slowquery`**:

```
slow query method: querySamples outcome: success totalMs: 3204 resolveMs: 412 queueMs: 3 \
  dbMs: 2601 processMs: 188 buckets: 48211 messages: 12 bytes: 41288104 \
  pvCount=812 pvs=S01-BPM01,S01-BPM02,S01-GCC01,... intervals=1 range=[1698767462,1698771062) \
  pageSize=10000 pageTokenPresent=false mode=SAMPLE streaming=true serializedColumns=false \
  excludeColumnMetadata=false statusFilter=false
```

(One line in the log; wrapped here to fit. The stage breakdown uses `field: value` and the request
shape `field=value`, which is how the two halves are told apart.)

A legacy V1 method logs the same stage breakdown with a shorter shape:

```
slow query method: queryData outcome: success totalMs: 12 resolveMs: 3 queueMs: 0 dbMs: 6 \
  processMs: 2 buckets: 10 messages: 1 bytes: 1996 pvCount=1 pvs=S01-GCC01 \
  range=[1789425206,1789425216)
```

**This is the counterpart to the metrics, and the division of labor is deliberate.** The histograms
answer "is the service slow, and in which stage", across all requests, with bounded attributes. The
log line answers "which request was slow, and what did it ask for", for one request — where the PV
names are exactly what an operator needs and cost nothing, because a log line's contents do not
multiply time series. Together they are the per-request detail a distributed trace would carry,
written at the same instrumentation points a span would later occupy.

The PV list is bounded to a count plus the first three names: a resolved pattern query can name
thousands of PVs, and a log line that grows with the facility's PV count is one an operator turns
off. The V1 methods (`queryData`, `queryTable`) log a shorter shape, because the V1 request carries
only a PV list and a time range — no paging, no result mode, no representation flags. It omits those
fields rather than printing defaults that would read as choices the client made.

The logger name is a literal string, not a class name, so that it stays a stable routing target: the
class can move or be renamed without silently redirecting a configured appender. To route these
lines to their own file, add an appender reference to the existing entry in `log4j2.xml`:

```xml
<Logger name="dp.slowquery" level="warn" additivity="false">
    <AppenderRef ref="slowQueryFile"/>
</Logger>
```

Set the threshold to `0` to log every query (useful when reproducing a problem), or to a negative
value to disable the line. **The threshold is resolved once per process at the first query**, so
changing it requires a restart.

## Diagnosing a slow query

The intended workflow, in order.

**0. Is the time even inside the handler?**

Start here whenever the complaint came from a client rather than from a dashboard. Every
`dp_query_*` duration starts at handler entry, so a request that is slow to decode or slow to
transmit looks perfectly healthy in steps 1-5.

```promql
# mean seconds per call at the gRPC layer, vs. mean seconds per request inside the handler
  sum(rate(grpc_server_call_duration_seconds_sum{grpc_method=~".*/querySamples"}[5m]))
    / sum(rate(grpc_server_call_duration_seconds_count{grpc_method=~".*/querySamples"}[5m]))
-
  sum(rate(dp_query_stage_duration_seconds_sum{rpc_method="querySamples",dp_stage="total"}[5m]))
    / sum(rate(dp_query_stage_duration_seconds_count{rpc_method="querySamples",dp_stage="total"}[5m]))
```

The suffix match is required: `grpc_method` is fully qualified where `rpc_method` is bare, so the
two families cannot be joined on the method label (see [gRPC and JVM metrics](#grpc-and-jvm-metrics)).

The difference is request decode plus response transmission. A few milliseconds is normal — on a
100-request concurrent benchmark returning 6.6 MB per response it measured about 9 ms. If it is a
large fraction of the total, the time is on the wire, not in the query: look at response bytes per
request (step 5), the client's own network path, and whether the caller is reading the stream
slowly, since a slow consumer holds the server's `onNext` calls open and inflates this gap without
any dp stage growing.

**1. Which method, and is it slow across the board or in the tail?**

```promql
histogram_quantile(0.50, sum by (rpc_method, le) (rate(dp_query_stage_duration_seconds_bucket{dp_stage="total"}[5m])))
histogram_quantile(0.95, sum by (rpc_method, le) (rate(dp_query_stage_duration_seconds_bucket{dp_stage="total"}[5m])))
histogram_quantile(0.99, sum by (rpc_method, le) (rate(dp_query_stage_duration_seconds_bucket{dp_stage="total"}[5m])))
```

A p50 near p99 means everything is slow — usually the database or a saturated worker pool. A normal
p50 with a bad p99 means a subset of queries is expensive; go to the slow-query log to see which.

**2. Which stage is the time in?**

```promql
sum by (dp_stage) (rate(dp_query_stage_duration_seconds_sum{rpc_method="querySamples",dp_stage!="total"}[5m]))
  / scalar(sum(rate(dp_query_stage_duration_seconds_sum{rpc_method="querySamples",dp_stage="total"}[5m])))
```

`scalar()` on the denominator because the `total` series has no `dp_stage` label in common with the
four it is being divided into — a plain vector division would match nothing and return empty. The
`dp_stage!="total"` on the numerator keeps `total` out of its own breakdown.

This is the fraction of total time in each stage. The four sum to 1 by construction rather than as
a consistency check — `process` is computed as what is left of `total` after the three measured
stages, so the sum holds even if a stage is mis-attributed. Read it as a breakdown, not as evidence
the breakdown is right. It points at what to look at next:

- **`db` dominant** → step 3.
- **`queue` dominant** → the worker pool is saturated; step 4.
- **`resolve` dominant** → a selector is expensive. A metadata or name-pattern selector reads the
  database before the request is ever enqueued. Look at `db.client.operation.duration` for the
  `pvMetadata` and `configurationActivations` collections.
- **`process` dominant** → assembly and serialization. Check response bytes per request (step 5) and
  the JVM GC metrics.

**3. Is MongoDB slow, or are we reading too much?**

```promql
# server-side command time, by collection and operation
histogram_quantile(0.95, sum by (db_collection_name, db_operation_name, le)
  (rate(db_client_operation_duration_seconds_bucket[5m])))

# buckets read per query
sum by (rpc_method) (rate(dp_query_buckets_total[5m]))
  / sum by (rpc_method) (rate(dp_query_requests_total[5m]))
```

Both sides of that division are aggregated with `sum by (rpc_method)` rather than divided directly:
`dp_query_requests_total` carries a `dp_outcome` label that `dp_query_buckets_total` does not, so a
plain `a / b` finds no matching pair and returns **empty** — no error, just a blank panel. The same
applies to bytes-per-request in step 5. Aggregating both sides to the labels they share is the
general fix whenever two dp counters are combined.

High command durations mean the database is slow. Normal command durations with a high buckets-per-
query figure means the query is reading too much — check the time range and PV count in the
slow-query line, and see `doc/runbooks/` for the index-related causes (a missing or unhinted bucket
index, or an over-wide lookback from a PV's recorded bucket span).

If `db` stage time greatly exceeds total command time over the same window, the time is in BSON
decoding, not in MongoDB.

**4. Is the worker pool saturated?**

```promql
dp_handler_workers_active{dp_service="query"} / dp_handler_workers_max{dp_service="query"}
histogram_quantile(0.95, sum by (dp_job, le) (rate(dp_handler_queue_wait_seconds_bucket{dp_service="query",le!=""}[5m])))
```

Sustained ratio at 1.0 with climbing queue wait means raise `DP_QUERY_HANDLER_NUM_WORKERS` — or find
the job type holding workers, via `dp.handler.job.duration` by `dp_job`.

**5. Rates and result sizes.**

```promql
# request rate by outcome
sum by (rpc_method, dp_outcome) (rate(dp_query_requests_total[5m]))

# error and reject rates as fractions
sum by (rpc_method) (rate(dp_query_requests_total{dp_outcome="error"}[5m]))
  / sum by (rpc_method) (rate(dp_query_requests_total[5m]))

# bytes per request
sum by (rpc_method) (rate(dp_query_response_bytes_total[5m]))
  / sum by (rpc_method) (rate(dp_query_requests_total[5m]))
```

**Ingestion health**, for completeness:

```promql
# ingestion latency, the number grpc.server.call.duration cannot show
histogram_quantile(0.95, sum by (le) (rate(dp_ingest_duration_seconds_bucket[5m])))

# samples stored per second
rate(dp_ingest_samples_total[5m])

# fraction of requests failing
sum(rate(dp_ingest_requests_total{dp_outcome="error"}[5m])) / sum(rate(dp_ingest_requests_total[5m]))
```

## What these metrics do not cover

Stated explicitly, because each of these is something an operator might reasonably assume is here.

- **No distributed tracing.** There are no spans and no trace ids, so a request cannot be followed
  across services. The stage histograms and the slow-query log are the substitute within one
  service. Tracing is a follow-on.
- **`dp.query.stage.duration` does not include wire time.** Its clock starts at handler entry, so
  request decode, response transmission, and client-side deserialization are all outside it. Under
  a 100-request concurrent benchmark the handler span ran about 9 ms/request shorter than
  `grpc_server_call_duration_seconds` and about 16 ms/request shorter than the client's own
  measurement. A client reporting slow queries is therefore not contradicted by healthy
  `dp_query_stage_duration`; compare the gRPC family as well, remembering that `grpc_method` is
  fully qualified where `rpc_method` is bare, so the two cannot be joined on method.
- **Ingestion RPC duration is not ingestion latency.** See [Ingestion metrics](#ingestion-metrics).
- **A database outage is absence of data, not an error rate.** See [Database metrics](#database-metrics).
- **`error.type` never reflects a write error.** See [Database metrics](#database-metrics).
- **No queue-depth gauge.** See [Handler metrics](#handler-metrics-all-services).
- **No connection-pool metrics.** Seven workers against a default pool of 100 cannot contend today.
- **`queryDataBidiStream` is measured to its first response only.** See [Query stages](#query-stages).
- **The query metadata methods have no `dp.query.*` metrics** — `queryPvMetadata`,
  `queryProviders`, `queryProviderStats`, `queryPvStats`. See [Query metrics](#query-metrics).
- **`dp.ingest.*` covers the data path only**, not `registerProvider` or the subscription methods.
  See [Ingestion metrics](#ingestion-metrics).
- **gRPC-layer validation rejects are counted nowhere** — not in `dp.query.requests` (no telemetry
  context exists yet) and not as a gRPC error (a rejection is an `OK` response carrying an
  `ExceptionalResult`). Affects the V1 methods only; the V2 methods validate inside the handler,
  where a context exists, and their resolution rejects are counted. Tracked as
  [#279](https://github.com/osprey-dcs/dp-service/issues/279). See [Query metrics](#query-metrics).
- **The annotation and ingestion-stream services have handler and database metrics but no
  request-level ones of their own** — no equivalent of `dp.query.requests` or `dp.ingest.requests`.
  Their `dp.handler.*` series (by `dp_service` and `dp_job`) plus `grpc.server.call.duration` are
  what is available.
- **No per-PV anything.** See below.

## Cardinality: why there are no PV names here

The complete set of attributes dp instrumentation may attach to any metric is:

```
dp.service   dp.job   dp.stage   dp.outcome   rpc.method
db.operation.name   db.collection.name   db.namespace   error.type
```

Every one is bounded by something small and fixed — four services, a dozen job classes, five stages,
four outcomes, the method set, the collection set.

**A PV name, provider id, client request id, page token, or user identity must never become a
metric attribute.** A facility with 10^5 PVs would turn a single histogram into 10^5 time series,
and with ten bucket boundaries that is 10^6 samples per scrape from one instrument. That is how a
metrics backend is taken down by the service it is monitoring, and the service would be exporting
happily while it happened.

Per-request detail of that kind belongs in the [slow-query log line](#the-slow-query-log), which
carries the PV names precisely because the metrics do not. This is enforced by integration tests
that assert the attribute key set of every dp metric and that no PV name appears as any attribute
value.
