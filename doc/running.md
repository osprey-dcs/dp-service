# Running MLDP Services and Applications

## core services

Below are java command lines for running the Data Platform Core Services, using the standard "dp" MongoDB database.  Use these commands for production and test systems.

The data-platform repo includes [[bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-scripts)](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-scripts) for starting the services.

#### Ingestion Service
```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer"
```

#### Query Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer"
```

#### Annotation Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer"
```

#### Ingestion Stream Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingestionstream.server.IngestionStreamGrpcServer"
```

## service metrics

Each service exports Prometheus metrics on its own port, in addition to its gRPC port. This is on by
default, so **each service binds a second port**; if that port is not free the service fails to
start. Scrape with `curl -s localhost:9465/metrics`.

| Service          | gRPC port | metrics port |
|------------------|-----------|--------------|
| Ingestion        | 50051     | 9464         |
| Query            | 50052     | 9465         |
| Annotation       | 50053     | 9466         |
| Ingestion Stream | 50054     | 9467         |

The benchmark servers use 60451 (ingestion) and 60452 (query), so a benchmark can run on a host that
is also running the live services.

To turn metrics off, set `DP_TELEMETRY_ENABLED=false`. To bind the endpoint to the loopback interface
only, set `DP_TELEMETRY_PROMETHEUS_HOST=127.0.0.1`. To push to an OpenTelemetry collector instead of
being scraped, set `OTEL_METRICS_EXPORTER=otlp` and `OTEL_EXPORTER_OTLP_ENDPOINT`.

On Kubernetes the metrics port is an ordinary container port; `doc/metrics.md` has the
`containerPort`, `ServiceMonitor`, and pod-annotation forms, and the released image
(`ghcr.io/osprey-dcs/dp-service`) declares all eight ports. Images released after 1.16.0 are signed
(earlier ones are not); the "Container Image" section of [`README.env`](../README.env) has the `cosign verify` command.

See [`doc/metrics.md`](metrics.md) for what is measured, the query stage breakdown, the slow-query
log, and the PromQL for diagnosing a slow query.

## startup, health and Kubernetes probes

A service binds its gRPC port as soon as it starts, and initializes after that, in the background
([#293](https://github.com/osprey-dcs/dp-service/issues/293)). Initialization includes any pending
[schema migration](runbooks/schema-migration.md), which on a large archive takes hours. Until
initialization has succeeded the service is up but not serving:

- the gRPC health service reports `NOT_SERVING`;
- every other call ends with gRPC status `UNAVAILABLE` and a description saying why, one of
  `service is initializing`, `service is initializing: applying schema migration version N (target M)`,
  or `service is initializing: waiting for a schema migration running in another process`;
- the log line `service ready after N s of initialization` marks the end of it.

**A client should treat `UNAVAILABLE` as "retry later"**, which is what gRPC retry policies already
do. Before this change the same client got a refused connection, which gRPC also reports as
`UNAVAILABLE`, so retry handling that worked before still works; the description is new.

**If initialization fails the process exits with status 1**, as it did before; the cause is in the
log. A fatal JVM error during initialization exits with status 3 (see
[out-of-memory behavior](#out-of-memory-behavior)). Run the services under a supervisor that
restarts on a non-zero exit.

### the health service

Every service serves the standard gRPC health service, `grpc.health.v1.Health` (`Check` and
`Watch`), on its gRPC port. Three names are registered:

| Health service name | Status |
|---|---|
| `""` (the server as a whole) | `NOT_SERVING` until the service has initialized, then `SERVING` |
| the service's full name, e.g. `dp.service.query.DpQueryService` | the same |
| `liveness` | `SERVING` from the moment the port is bound |

All three report `NOT_SERVING` once the process has begun to shut down.

`liveness` says only that the process is up and answering. It exists because a Kubernetes gRPC
probe passes only on `SERVING`: a gRPC **liveness** probe that names no service would fail for the
whole of a migration, and the kubelet would kill the pod that is migrating.

The services do not serve gRPC reflection, so `grpcurl` needs the health proto
(`grpcurl -plaintext -proto health.proto -d '{"service":"liveness"}' localhost:50052 grpc.health.v1.Health/Check`).
`grpc_health_probe -addr=localhost:50052 -service=liveness` needs nothing.

### Kubernetes probes

Use these. The ports are the query service's; see the table above for the others.

```yaml
livenessProbe:
  tcpSocket:
    port: 50052
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  grpc:
    port: 50052        # no service name: passes only once the service has initialized
  periodSeconds: 5
  failureThreshold: 1
# no startupProbe
```

A gRPC liveness probe is equivalent, **provided it names the `liveness` service**:

```yaml
livenessProbe:
  grpc:
    port: 50052
    service: liveness
```

- **Never use a `grpc` liveness probe with no `service`.** It checks `""`, which is `NOT_SERVING`
  for the whole of initialization, so it kills the pod during a long migration. That is the
  incident this design exists to end.
- **A `startupProbe` is not needed.** If one is kept, it must have the liveness form, not the
  readiness form: a startup probe that waits for `SERVING` kills the pod at
  `failureThreshold × periodSeconds`, however alive the migration is.
- **The readiness probe fails for as long as initialization takes**, and that is correct: the pod
  is kept out of the Service's endpoints and is not restarted for it.
- `grpc` probes are built into the kubelet from Kubernetes 1.24. Before that, use `tcpSocket` for
  liveness and an `exec` of `grpc_health_probe` for readiness.
- A probe that still kills the migrating pod is stopped by the migration's takeover limit: after a
  migration has been abandoned four times at one schema version, the next process exits and waits
  for an operator. That is intended; see the
  [runbook](runbooks/schema-migration.md#the-schema-migration-from-version-n-has-been-abandoned-by-4-processes-in-a-row).

Readiness does not make a rolling update safe across a schema change: the old pods keep serving
until the new one is ready, which is after the migration. **Stop every dp service before an upgrade
that changes the schema**; the procedure is in the
[runbook](runbooks/schema-migration.md#upgrading-to-a-release-that-changes-the-schema).

### a sharded deployment needs the `inprog` privilege

Through mongos, the service's database user needs the `inprog` action to list its own running
operations. Without it, the probes above still stop Kubernetes from killing a migrating pod, and
the other services still wait for it. But a migrating pod that dies for any other reason (a node
drain, an out-of-memory kill, a deleted pod) leaves a migration claim that must be cleared by
hand, where with the grant the next pod takes the migration over.

The role to create is in the
[runbook](runbooks/schema-migration.md#the-privilege-a-sharded-deployment-needs). To confirm it on
a running service, either is enough:

- the gauge `dp_schema_migration_takeover_enabled` is 1 (0 means the privilege is missing);
- the startup log has no `warn` line beginning `this service cannot take over an abandoned schema
  migration`.

A standalone server or a replica set needs nothing.

## out-of-memory behavior

A service that runs out of heap exits instead of continuing in an undefined state, so that its
supervisor (Kubernetes, systemd, a wrapper script) restarts it. Before this, an `OutOfMemoryError`
inside a request ended the worker thread that hit it, permanently; once every worker was gone the
service still accepted requests and answered none of them
([#314](https://github.com/osprey-dcs/dp-service/issues/314)).

There are two mechanisms, and a deployment should have both. Each exits with **status 3**.
Requests in flight on other threads are cut off; clients see the connection drop and should retry.

- **A JVM flag, `-XX:+ExitOnOutOfMemoryError`: the primary mechanism.** The JVM exits the moment
  it runs out of heap (or metaspace, or another JVM-managed area), on whatever thread that happens:
  a request worker, a gRPC thread, the MongoDB driver.
- **Built in, always on: the backstop.** An `OutOfMemoryError` or other JVM-level failure that
  reaches a handler worker, or the thread that initializes the service at startup, halts the
  process. This covers a launch without the flag. It also
  covers, on a worker, the case the flag does not: an `OutOfMemoryError` that library code throws
  itself, which the JVM does not see as running out of memory. Examples are a buffer asked to grow
  past the largest array the JVM allows, and direct (off-heap) buffer memory running out.

**Neither mechanism covers a library-thrown `OutOfMemoryError` on a thread that is not a handler
worker**, such as a gRPC or MongoDB driver thread. The flag does not see it and no worker catches
it, so the process keeps running. It is logged as an exception on that thread. Checked on JDK 21:
with the flag set, exhausting `-XX:MaxDirectMemorySize` throws and the process continues.

The container image sets the flag for every service through the `JAVA_TOOL_OPTIONS` environment
variable. Two cases need action from the operator:

- **You set `JAVA_TOOL_OPTIONS` yourself** (for example to set `-Xmx`). Your value replaces the
  image's, so include the flag: `JAVA_TOOL_OPTIONS="-Xmx3g -XX:+ExitOnOutOfMemoryError"`.
- **You start the services with your own `java` command**, outside the image. Add
  `-XX:+ExitOnOutOfMemoryError` to that command.

The JVM logs `Picked up JAVA_TOOL_OPTIONS: ...` to stderr at startup, which is the quick way to
confirm what is in effect.

Run the services under a supervisor that restarts on a non-zero exit, and alert on restarts with
status 3: each one is a request the heap could not hold. To capture a heap dump for diagnosis, add
`-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<a volume with room for the whole heap>`. The
image does not set this, because a dump is as large as the heap and would be written to the
container's filesystem.

## performance benchmark applications

The Data Platform includes tools for measuring performance of the Ingestion and Query Services.  There are special versions of the server applications that override the standard network port number and database, using the MongoDB database "dp-benchmark".  To run a benchmark for either service you must run both the server and client application.  Java command lines are given below.

The data-platform repo includes [bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-performance-benchmarks) for starting the benchmark servers and running the client applications.

### ingestion performance benchmark

#### ingestion benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer"
```

#### ingestion benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkBidiStreamingIngestion
```

### query performance benchmark

#### query benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryGrpcServer"
```

#### query benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataStream
```

## sample data generator

Recognizing the previous use of the Ingestion Service performance benchmark to generate sample data for web development and demo purposes, a sample data generator application was created to generate sample data for one minute and 4,000 signals each sampled at 1 KHz.  The data generator uses the standard "dp" database and data is created with a fixed date/time starting at "2023-10-31T15:51:00.000+00:00".  Happy Halloween...

To use the sample data generator, run the standard Ingestion Service as shown above, and use the Java command line below to run the client application.

The data-platform repo includes [bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#sample-data-generator-scripts) for starting the standard Ingestion Service and running the sample data generator application.

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.utility.TestDataGenerator
```

# Service Configuration


## Default config file

The default configuration file for the Data Platform services is in [application.yml](https://github.com/osprey-dcs/dp-service/blob/main/src/main/resources/application.yml). The config file contains in-line comments for each section and individual configuration options.

The default settings are probably reasonable for most development systems, though you'll want to override the MongoDB URI resource to match your configuration. 

Options for overriding the config file and individual resources are discussed below.


## Overriding config file

The default configuration file can be overridden in two different ways, by specifying an alternative file on either the command line used to start the application, or as an environment variable.

To specify an alternative on the command line, add a VM option (e.g., on the command line BEFORE the class name) like the following: "-Ddp.config=/path/to/config/override.yml".

To specify an alternative via an envoronment variable, define a variable in the environment "DP.CONFIG=/path/to/config/override.yml" before running the Ingestion Service application.  For example:
```
java -Ddp.config=$DP_HOME/config/dp.yml -Dlog4j.configurationFile=$DP_HOME/config/log4j2.xml -cp $DP_HOME/lib/dp-service/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```


## Overriding individual configuration properties on the command line

In addition to overriding the default config file, individual configuration settings can be overridden on the command line.  To do so, use a VM option (in the java command line BEFORE the class name), prefixing the configuration setting name with "dp.".  For example, to override the gRPC port for the Ingestion Service, use "-Ddp.IngestionServer.port=50052".## Custom Configuration via Environment Variables

## Overriding individual configuration properties via environment variables

- **What:** The service reads configuration values from `application.yml` using the ${ENV_VAR:default} substitution syntax. If an environment variable is set, it overrides the default value declared in the YAML. If it is not set, the YAML default is used.
- **How:** Set environment variables before starting the JVM or provide them in your container/compose environment. Example (shell):

```bash
export DP_MONGO_DB_URI=mongodb://admin:admin@localhost:27017/
java <other options> com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```

- **Docker / docker-compose:** Put the same variables in the `environment:` block for the container. Example (docker-compose.yml):

```yaml
services:
	dp-service:
		image: your-image
		environment:
			- DP_MONGO_DB_URI=mongodb://admin:admin@localhost:27017/
```

- **Notes:**
	- Boolean values should be set as the strings `true` or `false` (unquoted or quoted are both accepted by the shell/container). Numeric values should be plain numbers. Strings containing colons or special characters should be quoted.

**Accepted environment variables**

Below is the list of environment variables referenced in the project's `application.yml`. Each variable maps to a configuration property; if not set, the YAML default applies.

- `DP_MONGO_DB_URI` : Full MongoDB connection string. Example: `mongodb://user:pass@host:27017/?authSource=admin`
- `DP_MONGO_RUN_SCHEMA_MIGRATIONS_ON_STARTUP` : Whether the service applies pending schema migrations at startup (default: `true`; see [doc/runbooks/schema-migration.md](runbooks/schema-migration.md)). Setting this to `false` skips *applying* migrations, **not** the version check — a service whose expected schema version does not match the database still refuses to start.
- `DP_MONGO_SCHEMA_MIGRATION_HEARTBEAT_INTERVAL_SECONDS` : How often the process running a schema migration increments the heartbeat on its claim (default: `10`). A starting process that sees the heartbeat stop takes the migration over; see [doc/runbooks/schema-migration.md](runbooks/schema-migration.md).
- `DP_MONGO_SCHEMA_MIGRATION_STALE_AFTER_SECONDS` : How long a waiting process must see no heartbeat before it takes an abandoned migration over (default: `60`). The service refuses to start unless this is at least three heartbeat intervals and below 300.
- `DP_MONGO_BENCHMARK_DB_NAME` : Name of the MongoDB database used by the benchmark (default: `dp-benchmark`). **WARNING: this database is dropped and recreated at the start of every benchmark run — never point it at a database containing data you want to keep.**
- `DP_MONGO_TEST_DB_NAME` : Name of the MongoDB database used by integration tests (default: `dp-test`). **WARNING: this database is dropped and recreated at the start of every test run — never point it at a database containing data you want to keep.**
- `DP_GRPC_CLIENT_INGESTION_CONNECT_STRING` : Ingestion Service `host:port` used by the client API layer (default: `localhost:50051`)
- `DP_GRPC_CLIENT_QUERY_CONNECT_STRING` : Query Service `host:port` used by the client API layer (default: `localhost:50052`)
- `DP_GRPC_CLIENT_ANNOTATION_CONNECT_STRING` : Annotation Service `host:port` used by the client API layer (default: `localhost:50053`)
- `DP_GRPC_CLIENT_INGESTION_STREAM_CONNECT_STRING` : Ingestion Stream Service `host:port` used by the client API layer (default: `localhost:50054`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIME_SECONDS` : gRPC client keepalive time in seconds (default: `45`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC client keepalive timeout in seconds (default: `20`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC client keepalive without calls (default: `true`)
- `DP_GRPC_SERVER_INCOMING_MESSAGE_SIZE_LIMIT_BYTES` : gRPC incoming message size limit in bytes (default: `4096000`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIME_SECONDS` : gRPC server keepalive time in seconds (default: `60`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC server keepalive timeout in seconds (default: `20`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS` : gRPC server permit-keepalive time in seconds (default: `30`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC server permit keepalive without calls (default: `true`)
- `DP_TELEMETRY_ENABLED` : Collect and export metrics (default: `true`; see [doc/metrics.md](metrics.md))
- `DP_TELEMETRY_PROMETHEUS_HOST` : Interface the metrics endpoints bind to (default: `0.0.0.0`)
- `DP_INGESTION_SERVER_PORT` : Ingestion Service gRPC port (default: `50051`)
- `DP_INGESTION_SERVER_METRICS_PORT` : Ingestion Service Prometheus metrics port (default: `9464`)
- `DP_INGESTION_HANDLER_NUM_WORKERS` : Number of ingestion handler worker threads (default: `7`)
- `DP_INGESTION_HANDLER_SOURCEMONITOR_VALIDATE_PVS` : Ingestion SourceMonitor validate PVs (default: `true`)
- `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS` : Largest time span, in seconds, that the ingestion service accepts in one data frame (default: `86400`). Read by the ingestion service only — since 1.16.0 the query-side lower bound is derived per PV from the `pvStats` collection, so changing this affects only what ingestion accepts from then on.
- `DP_INGESTION_BENCHMARK_GRPC_CONNECT_STRING` : Ingestion benchmark gRPC `host:port` connect string (default: `localhost:60051`)
- `DP_INGESTION_BENCHMARK_START_SECONDS` : Ingestion benchmark fixed start time (epoch seconds)
- `DP_QUERY_SERVER_PORT` : Query Service gRPC port (default: `50052`)
- `DP_QUERY_SERVER_METRICS_PORT` : Query Service Prometheus metrics port (default: `9465`)
- `DP_QUERY_HANDLER_NUM_WORKERS` : Number of query handler worker threads (default: `7`)
- `DP_QUERY_HANDLER_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES` : Query handler outgoing message size limit (default: `4096000`)
- `DP_QUERY_HANDLER_SLOW_QUERY_LOG_THRESHOLD_MILLIS` : Slow query log threshold in ms; `0` logs every query, negative disables (default: `1000`)
- `DP_QUERY_HANDLER_QUERY_V2_DEFAULT_PAGE_SIZE` : Default page size for the Query API V2 methods when the request's limit is unset (default: `10000`)
- `DP_QUERY_HANDLER_QUERY_V2_MAX_PAGE_SIZE` : Maximum page size for the Query API V2 methods; a larger requested limit is clamped (default: `100000`)
- `DP_QUERY_HANDLER_QUERY_V2_MAX_RESOLVED_PV_COUNT` : Maximum number of PVs a Query API V2 `PvSelector` may resolve to before the request is rejected (default: `10000`)
- `DP_QUERY_HANDLER_STREAM_READY_TIMEOUT_SECONDS` : How long a server-streaming query waits for a slow client to drain the outbound buffer before the response is abandoned; the worker serving the stream is held for the wait (default: `300`)
- `DP_QUERY_BENCHMARK_GRPC_CONNECT_STRING` : Query benchmark gRPC `host:port` connect string (default: `localhost:60052`)
- `DP_ANNOTATION_SERVER_PORT` : Annotation Service gRPC port (default: `50053`)
- `DP_ANNOTATION_SERVER_METRICS_PORT` : Annotation Service Prometheus metrics port (default: `9466`)
- `DP_ANNOTATION_HANDLER_NUM_WORKERS` : Number of annotation handler worker threads (default: `7`)
- `DP_ANNOTATION_HANDLER_SAMPLE_STATUS_QUERY_DEFAULT_PAGE_SIZE` : Default page size (SampleStatusBuckets per response) for `querySampleStatuses`/`querySampleStatusesStream` when the request's limit is unset (default: `10000`)
- `DP_ANNOTATION_HANDLER_SAMPLE_STATUS_QUERY_MAX_PAGE_SIZE` : Maximum page size for those methods; a larger requested limit is silently clamped (default: `100000`)
- `DP_ANNOTATION_HANDLER_SAMPLE_STATUS_SAVE_MAX_STATUSES` : Maximum total individual sample statuses (timestamps x columns, summed over frames) accepted in one `saveSampleStatuses` request; oversized requests are rejected (default: `1000000`)
- `DP_EXPORT_SERVER_MOUNT_POINT` : Export server mount point (default: `/tmp`)
- `DP_EXPORT_SHARE_MOUNT_POINT` : Export share mount point (defaults to server mount point if unset)
- `DP_EXPORT_URL_BASE` : Base URL for accessing export files (default: `http://localhost:8081`)
- `DP_EXPORT_TABULAR_EXPORT_FILE_SIZE_LIMIT_BYTES` : Tabular export file size limit (default: `4096000`)
- `DP_INGESTION_STREAM_SERVER_PORT` : Ingestion Stream Service gRPC port (default: `50054`)
- `DP_INGESTION_STREAM_SERVER_METRICS_PORT` : Ingestion Stream Service Prometheus metrics port (default: `9467`)
- `DP_INGESTION_STREAM_HANDLER_NUM_WORKERS` : Number of ingestion stream handler worker threads (default: `7`)
- `DP_INGESTION_STREAM_EVENTMONITOR_MAX_MESSAGE_SIZE_BYTES` : EventMonitor max message size (default: `4096000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_FLUSH_INTERVAL_MILLIS` : DataBuffer flush interval in ms (default: `500`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_BYTES` : DataBuffer max bytes (default: `524288`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_ITEMS` : DataBuffer max items (default: `50`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_LIMIT_NANOS` : DataBuffer age limit in nanoseconds (default: `2000000000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_CUSHION_NANOS` : DataBuffer age cushion in nanoseconds (default: `1000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_EXPIRATION_NANOS` : TriggeredEventManager expiration in nanoseconds (default: `5000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_CLEANUP_INTERVAL_MILLIS` : TriggeredEventManager cleanup interval in ms (default: `5000`)

TODO: add a short example `docker-compose.yml` snippet that demonstrates mounting configuration files while still using environment overrides, I can add that as a follow-up.

## Running integration tests against a non-local MongoDB cluster

The integration test framework is designed for development use: it drops and recreates the test database before each test class runs, starting from a completely clean state. This is intentional — the tests are not designed to tolerate pre-existing data.

### Prerequisites

- The MongoDB user in the connection URI must have **`dbOwner`** (or equivalent `dbAdmin` + `readWrite`) privileges on the test database. The framework drops the entire database on each run; a `readWrite`-only user is insufficient.
- The test database name must be a **dedicated throwaway name** that does not conflict with any production or shared database on the cluster.

### Configuration

Set two environment variables before running the tests:

```bash
# MongoDB connection URI for the target cluster
export DP_MONGO_DB_URI="mongodb://testuser:testpass@cluster-host:27017/?authSource=admin"

# Name of the dedicated test database (will be DROPPED before each test class)
export DP_MONGO_TEST_DB_NAME="dp-test-functional"

mvn test -Dtest=PvMetadataIT
```

Or to run all integration tests:

```bash
export DP_MONGO_DB_URI="mongodb://testuser:testpass@cluster-host:27017/?authSource=admin"
export DP_MONGO_TEST_DB_NAME="dp-test-functional"

mvn test
```

> **WARNING:** The database named by `DP_MONGO_TEST_DB_NAME` is **dropped without confirmation** at the start of every test run. Use a name that is reserved exclusively for this purpose.

## Running benchmarks against a non-local MongoDB cluster

The benchmark server drops and recreates the benchmark database at the start of every run, starting from a completely clean state. This is intentional — benchmarks require an empty database for accurate measurements.

### Prerequisites

- The MongoDB user in the connection URI must have **`dbOwner`** (or equivalent `dbAdmin` + `readWrite`) privileges on the benchmark database. The server drops the entire database on each run; a `readWrite`-only user is insufficient.
- The benchmark database name must be a **dedicated throwaway name** that does not conflict with any production or shared database on the cluster.

### Configuration

Set two environment variables before starting the benchmark server:

```bash
# MongoDB connection URI for the target cluster
export DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin"

# Name of the dedicated benchmark database (will be DROPPED at the start of every run)
export DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional"

java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

Then run the benchmark client (no MongoDB connection required — connects to the server via gRPC only):

```bash
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream
```

> **WARNING:** The database named by `DP_MONGO_BENCHMARK_DB_NAME` is **dropped without confirmation** at the start of every benchmark server run. Use a name that is reserved exclusively for this purpose.
