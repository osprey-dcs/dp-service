# dp-service #185: Override Database URI and Database Name for Performance Benchmark Applications

> **Archival note.** This plan was written against an earlier revision of issue #185 and was
> overtaken by it. The issue was later widened to cover two further changes that this document does
> not plan, and both shipped with the ticket:
>
> - `QueryBenchmarkBase` reads the gRPC connect string from configuration
>   (`QueryBenchmark.grpcConnectString` / `DP_QUERY_BENCHMARK_GRPC_CONNECT_STRING`) instead of a
>   hardcoded constant, matching `IngestionBenchmarkBase`
>   (`QueryBenchmarkBase.java:50`, `:934`).
> - The ingestion benchmark clients default to `DOUBLE_COLUMN` rather than the legacy `DATA_COLUMN`
>   (`BenchmarkIngestDataStream.java:143`, and the same line in `BenchmarkIngestDataBidiStream`).
>
> The "Scope" section below is likewise narrower than what shipped: the query benchmark clients
> call `BenchmarkMongoClient.prepareBenchmarkDatabase()` directly, so the database-name override
> reaches them too. The document is kept as written, as the record of the plan the first part of
> the work was executed against; the issue is the authority on the delivered scope.

## Overview

The performance benchmark server application (`BenchmarkIngestionGrpcServer`) uses a hardwired MongoDB database name `"dp-benchmark"` defined as a constant in `BenchmarkMongoClient`. A customer needs to run the benchmark against their own MongoDB cluster, so the database name must be configurable — consistent with the existing pattern used for the integration test framework in issue #183.

The MongoDB connection URI is already overridable via `DP_MONGO_DB_URI` / `MongoClient.uri` (established before issue #183) and is already picked up by the benchmark server. The missing piece is the ability to specify a different database name on the customer's cluster.

## Background

`BenchmarkMongoClient.init()` calls `MongoClientBase.setMongoDatabaseName(BENCHMARK_DATABASE_NAME)` where `BENCHMARK_DATABASE_NAME = "dp-benchmark"` is a hardcoded constant. There is no configuration key or environment variable to change this.

`BenchmarkMongoClient.dropBenchmarkDatabase()` also references the constant directly rather than `MongoClientBase.getMongoDatabaseName()`, which means that even if the global database name were set by some other means, the drop would still target the wrong name.

The benchmark client applications (`BenchmarkIngestDataStream`, `BenchmarkIngestDataBidiStream`) connect to the server via gRPC only — they do not touch MongoDB directly — so no changes are needed there.

## Approach

Add a new configuration property `MongoClient.benchmarkDatabaseName` to `application.yml`, backed by the environment variable `DP_MONGO_BENCHMARK_DB_NAME`, defaulting to `"dp-benchmark"`. `BenchmarkMongoClient.init()` reads this property through `ConfigurationManager` instead of using the hardwired constant. This is the same pattern used by `MongoTestClient` for `MongoClient.testDatabaseName` / `DP_MONGO_TEST_DB_NAME`.

## Constraints and Warnings

- The named database is **dropped without confirmation** at the start of every benchmark run. The customer must use a name that is reserved exclusively for this purpose and contains no data they want to keep.
- The MongoDB user in the connection URI must have `dbOwner` (or equivalent `dbAdmin` + `readWrite`) privileges on the benchmark database.
- A safety guard must prevent accidentally dropping the production database `"dp"` (parallel to the guard added to `MongoTestClient.dropTestDatabase()` in issue #183).

## Implementation Tasks

### Task 1 — `BenchmarkMongoClient.java`

**File:** `src/main/java/com/ospreydcs/dp/service/common/benchmark/BenchmarkMongoClient.java`

- Add constant `CFG_KEY_BENCHMARK_DATABASE_NAME = "MongoClient.benchmarkDatabaseName"`.
- In `init()`, resolve the database name via `configMgr().getConfigString(CFG_KEY_BENCHMARK_DATABASE_NAME, BENCHMARK_DATABASE_NAME)` instead of using the hardcoded constant directly.
- Call `MongoClientBase.setMongoDatabaseName(configuredName)` with the resolved name.
- Log at `warn` level when the resolved name differs from the default `BENCHMARK_DATABASE_NAME` (parallel to `MongoTestClient` behavior).
- Fix `dropBenchmarkDatabase()` to use `MongoClientBase.getMongoDatabaseName()` instead of the hardcoded `BENCHMARK_DATABASE_NAME` constant, so the drop always targets the configured database name.
- Add a safety guard in `dropBenchmarkDatabase()` that throws `IllegalStateException` if the resolved database name is the production database `"dp"`.

### Task 2 — `application.yml`

**File:** `src/main/resources/application.yml`

Add a new config key under the `MongoClient:` section:

```yaml
  # MongoClient.benchmarkDatabaseName: Name of the MongoDB database used by the benchmark.
  # WARNING: this database is dropped and recreated at the start of every benchmark run —
  # never point it at a database containing data you want to keep.
  benchmarkDatabaseName: ${DP_MONGO_BENCHMARK_DB_NAME:dp-benchmark}
```

### Task 3 — `doc/running.md`

**File:** `doc/running.md`

- Add `DP_MONGO_BENCHMARK_DB_NAME` to the accepted environment variables reference table, with a warning about the drop behavior.
- Add a new section **"Running benchmarks against a non-local MongoDB cluster"** covering:
  - Prerequisites: MongoDB user privileges (`dbOwner` or equivalent on the benchmark database).
  - The destructive drop-and-recreate behavior that occurs at the start of every benchmark run.
  - Example shell commands showing `DP_MONGO_DB_URI` and `DP_MONGO_BENCHMARK_DB_NAME` set together before running the benchmark server.

## Scope

- No changes to the benchmark client applications (`BenchmarkIngestDataStream`, `BenchmarkIngestDataBidiStream`).
- No changes to `MongoClientBase`.
- No changes to production service code or integration test infrastructure.
- No changes to `src/test/resources/application.yml` (benchmark code is not on the test classpath).
