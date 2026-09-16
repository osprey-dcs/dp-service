# dp-service #275: deep-history plan-shape test, docs-examined assertions, and benchmark coverage for the V2 query methods

## Overview

Companion to #274. Two deliverables, both aimed at the release concern that the customer archive
(33.8M buckets, sharded, some 42-day bucket spans) cannot be reproduced locally:

1. A deterministic, CI-runnable extension of `MongoBucketQueryPlanTest` that pins the property the
   #232/#271 work exists for: **scan cost is proportional to the window plus the span, independent
   of how deep a PV's history is.** It also pins the measured fact that the overlap residual runs
   after fetch (docs examined equals keys examined), so the cost model behind #274's span-class partition (D11) is visible in a
   test, not just a plan.
2. Query benchmark coverage for `queryTable`, `querySamples`, `querySamplesStream`, `queryBuckets`,
   and `queryBucketsStream`, plus a history-depth mode on the benchmark loader so a local run can
   have millions of buckets of history behind a recent window.

Target: 1.16.0, after #274 PR 1.

## Background: triage findings

### 1. What the plan-shape test covers today

`MongoBucketQueryPlanTest` (`src/test/java/.../query/handler/mongo/client/`) seeds two PVs with 300
one-second buckets each (`NUM_BUCKETS_PER_PV`, `:87`), spans of 300 s and 7 s, an adversarial index
set, and a window of five buckets near the end of the history. It asserts the winning plan is on the
shipped index with no `SORT`, the `firstTime.seconds` interval is `[begin − span, end]`, and (in
`testTighterSpanExaminesFewerKeysForTheSameResult`, `:255-279`) `totalKeysExamined` equals the
interval width. It does not assert `totalDocsExamined`, and 300 buckets of history is not deep enough
to distinguish "bounded by the window" from "bounded by the history".

### 2. The residual filter runs on FETCH

Measured 2026-09-14 on the local `dp-benchmark` archive (240,000 buckets, MongoDB 8.0) with the exact
production filter, sort, and hint, via `mongosh`:

| PVs | span | nReturned | totalKeysExamined | totalDocsExamined |
|---|---|---|---|---|
| 1 | 0 | 5 | 6 | 6 |
| 1 | 60 | 5 | 56 | 56 |
| 5 | 60 | 25 | 289 | 280 |

Winning plan: `FETCH` (with the `$and`/`$or` overlap filter) over `IXSCAN` with bounds
`firstTime.seconds: [begin − span, end]`. Every key in the interval is a document fetch; the index's
trailing `lastTime` keys are not used to filter before fetch. So the #232 span bound is a
document-fetch bound. Before #274 D11 a long span on one PV of a request cost fetches for every PV
in it; #274 partitions the request into span classes so each class find carries its own bound.

### 3. Benchmark coverage and its fixture

`QueryBenchmarkBase.loadBucketData()` (`QueryBenchmarkBase.java:170-240`) loads 4,000 PVs × 60
one-second buckets of 1,000 samples (240,000 buckets, ~2.4 GB) starting at `now`, then
`queryExperiment()` runs `queryScenario()` over the same 60 s. The three clients
(`BenchmarkQueryDataStream`, `BenchmarkQueryDataBidiStream`, `BenchmarkQueryDataUnary`) all build a
`QueryDataRequest`; the task hierarchy (`QueryTask` → `QueryDataRequestTask` →
`QueryDataResponseTask`, `:262-310`) and `newQueryTask()` are `QueryDataRequest`-specific. There is
no history behind the window, so the run cannot show the span/upper-bound effect, and no scenario
exercises the V2 methods or `queryTable`.

The loader already writes `pvStats` through the production updater
(`BenchmarkDbClient.insertBucketDocuments`, `:83-113`), one `recordSpan` per batch. Benchmark
buckets are legacy `DataColumnDocument`s (`BucketUtility.createBucketDocuments`, `BucketUtility.java:25`),
so value counting on the client side uses `DataBucket.getDataColumn().getDataValuesCount()`.

`BenchmarkMongoClient.prepareBenchmarkDatabase()` (`BenchmarkMongoClient.java:52-55`) constructs a
client whose `init()` **drops** the `dp-benchmark` database (`:32-34`, `dropBenchmarkDatabase()` at
`:41`) before initializing it, so a `--skip-load` mode must bypass it entirely.

### 4. Ingestion benchmark scale-up is not the right lever

`BenchmarkIngestDataStream` measures the rate at which requests are acknowledged, which is on enqueue
(see memory: ingestion benchmark measures enqueue). A larger ingestion run measures Mongo write
throughput; it does not create the shape that stresses queries, which is history depth per PV.
Nothing in this ticket changes the ingestion benchmarks.

## Design decisions

### D1 — Depth, not volume: a deep PV with tiny buckets

Scan cost is set by keys and fetched documents inside `[begin − span, end]`, not by document size or
collection size. The deep-history fixture is therefore one PV (`deeppv_1`) with `DEEP_BUCKETS =
20,000` one-second buckets of **one** sample each, inserted in batches of 5,000 (a few hundred
milliseconds; keeps the class fast for CI). Its `pvStats` span is recorded as 0 (the buckets are
one second wide, matching what ingestion would write).

### D2 — Assert history-independence by querying two positions in the same history

Two five-second windows, one 1,000 s into the history and one 19,000 s in. Both must return the same
five buckets' worth (`nReturned == 5`), and `totalKeysExamined` and `totalDocsExamined` must be
**equal to each other across the two positions** and equal to `window + span + 1`. That is the
assertion "cost does not depend on where in the history the window sits", which no result-level test
and no shallow fixture can make.

### D3 — Pin the FETCH-stage residual as a measured fact, not a wish

Add a `totalDocsExamined` read to the existing wide-span case: assert `docsExamined ==
keysExamined`. If a future server version evaluates the residual on the index scan, this assertion
fails, which is the signal to revisit the cost model behind #274 D11 (the partition may then be unnecessary). The
javadoc says so explicitly, so a reader does not "fix" the test by loosening it.

### D4 — Pin the span-class partition with a mixed-span request

A request naming `deeppv_1` (span 0) together with `planpv_1` (span 300) resolves to two span
classes (#274 D11). Explain each class find from the package-private `spanClassFinds()` builder:
the deep PV's find is bounded at `begin` and examines `window + 1` keys and documents; `planpv_1`'s
is bounded at `begin − 300` and examines `window + 300 + 1`. A counterfactual on the same request
through the old single-bound builder (`bucketDocumentQuery` with the request maximum) shows the deep
PV examining `window + 300 + 1`, so the test cannot pass if the partition silently stops
partitioning. A second case with `planpv_1` and `planpv_2` (spans 300 and 7: classes 9 and 3) pins
that classes are decided per PV, not by the request maximum.

### D5 — Benchmark: generalize the task hierarchy rather than fork it

`QueryDataRequestTaskParams` is renamed `QueryTaskParams` (same fields: stream number, PV names,
start seconds, duration); `newQueryTask()` returns `QueryTask`, and `queryScenario()` depends only on
`QueryTask`. The three existing clients keep their `QueryDataRequest`-specific intermediate classes.
New clients implement `QueryTask` directly with an inline `StreamObserver` that counts data values,
data bytes (sum of column serialized sizes), and gRPC bytes (`response.getSerializedSize()`), matching
`QueryDataResponseObserver`'s counters so the printed rates are comparable.

### D6 — Five new clients, one per method, unary methods page to completion

| Class | Method | Notes |
|---|---|---|
| `BenchmarkQueryTable` | `queryTable` (column format) | count = rows × columns |
| `BenchmarkQuerySamples` | `querySamples` | loops on `nextPageToken` until empty; counts pages |
| `BenchmarkQuerySamplesStream` | `querySamplesStream` | counts messages |
| `BenchmarkQueryBuckets` | `queryBuckets` | loops on `nextPageToken` |
| `BenchmarkQueryBucketsStream` | `queryBucketsStream` | the direct comparison to `queryDataStream` |

Default scenario matrix stays `{1000 PVs, 10 per request, 5 threads}`; the samples clients add a wide
case `{1000 PVs, 100 per request, 5 threads}` because assembly cost is per column (#274 D9).

### D7 — History-depth loader with a marker document and `--skip-load`

`loadBucketData` takes a `LoadParams` record: `numPvs` (4,000), `samplesPerSecond` (1,000),
`secondsPerBucket` (1), `historySeconds` (60), `longSpanPvs` (0), `longSpanSeconds` (0). Parsed from
`--pvs=`, `--samples-per-second=`, `--seconds-per-bucket=`, `--history-seconds=`,
`--long-span-pvs=`, `--long-span-seconds=`, `--skip-load`, `--help`, with today's values as defaults so
an argument-less run is unchanged.

- The load start is `now − historySeconds`; the query window is the **last** `NUM_SCENARIO_SECONDS`
  of the history, so the scan has history behind it.
- A marker document `{_id: "load", startSeconds, historySeconds, numPvs, ...}` in a
  `benchmarkMetadata` collection records the fixture; `--skip-load` reads it to place the window and
  refuses to run if it is absent. Reading `max(firstTime)` from `buckets` instead was rejected: that
  sort is not index-served across PVs.
- Long-span PVs (`longpv_1..n`) get one bucket each spanning `longSpanSeconds` with one sample per
  minute, recorded through the same updater, so a request mixing them with normal PVs shows the
  max-over-request effect. The samples clients get a `--include-long-span` switch to add them to each
  request.
- A deep fixture at the documented example (`--pvs=200 --samples-per-second=10 --history-seconds=604800`)
  is 121M buckets, too many; the documented example is `--pvs=200 --samples-per-second=10
  --seconds-per-bucket=10 --history-seconds=86400` → 1.7M buckets of 100 samples, ~200 MB, loading in
  a few minutes, with a day of history behind a one-minute window.

## Implementation tasks

### Task 1 — `MongoBucketQueryPlanTest` deep-history fixture and assertions

**File:** `src/test/java/com/ospreydcs/dp/service/query/handler/mongo/client/MongoBucketQueryPlanTest.java`

- Constants: `PV_DEEP = "deeppv_1"`, `DEEP_BASE_SECONDS = BASE_SECONDS − 100_000` (clear of the
  existing PVs' 300 s), `DEEP_BUCKETS = 20_000`, `DEEP_WINDOW_SECONDS = 5`.
- `setUp`: `BucketUtility.createBucketDocuments(DEEP_BASE_SECONDS, 1, 1, "deeppv_", 1, DEEP_BUCKETS)`
  inserted in 5,000-document batches; `client.recordSpan(PV_DEEP, 0L)`.
- `testDeepHistoryScanIsIndependentOfWindowPosition` (D2), `testOverlapResidualIsEvaluatedAfterFetch`
  (D3, on the existing wide-span V1 case), `testMixedSpanRequestBoundsEachClassSeparately` and
  `testMixedSpanCounterfactualSingleBoundWidensTheShortSpanPv` (D4; each class find explained
  individually, asserting its own `[begin − classSpan, end]` interval and `totalKeysExamined ==
  totalDocsExamined == width`), `testSpanClassesAreDecidedPerPv` (D4, second case).
- Extend the class javadoc's fixture paragraph and the `explainV1` helper if the deep window needs
  different constants (it does: begin/end are parameters, not the class constants).

### Task 2 — CLAUDE.md

"Bucket Span Bound Tests" list: describe the deep-history case and the FETCH-stage assertion, and
state that a change making docs examined fewer than keys examined is a planner change to be
understood, not a failure to be silenced.

### Task 3 — Loader parameters and marker

**Files:** `query/benchmark/QueryBenchmarkBase.java`, `common/benchmark/BenchmarkMongoClient.java`

- `LoadParams` record and `parseArgs(String[])` with `--help` text; `loadBucketData(LoadParams)`
  computes `loadStartSeconds`, submits one `InsertTask` per bucket period, writes the marker.
- Long-span buckets built inline (a `SamplingClock` with a 60 s period over `longSpanSeconds`).
- `--skip-load` bypasses `prepareBenchmarkDatabase()` (which drops the database, Background §3) and
  opens a plain `BenchmarkDbClient` only to read the marker.
- `runBenchmark(benchmark, args, ...)` derives `queryStartSeconds = loadStart + historySeconds −
  NUM_SCENARIO_SECONDS`.
- Existing `main()`s pass `args` through.

### Task 4 — Task hierarchy generalization

**File:** `QueryBenchmarkBase.java` (`:242-310`, `:394-420`)

Per D5. `BenchmarkQueryDataStream`, `BenchmarkQueryDataBidiStream`, `BenchmarkQueryDataUnary`, and
`QueryDataResponseObserver` updated for the rename only.

### Task 5 — New benchmark clients

**Files:** new `query/benchmark/BenchmarkQueryTable.java`, `BenchmarkQuerySamples.java`,
`BenchmarkQuerySamplesStream.java`, `BenchmarkQueryBuckets.java`, `BenchmarkQueryBucketsStream.java`

Per D6. Each builds its request from `QueryTaskParams` (`QuerySpec` with `PvNameList` and
`TimeRange`; `queryTable` uses `QueryTableRequest` with `pvNameList` and `TABLE_FORMAT_COLUMN`), awaits
on a latch with `AWAIT_TIMEOUT_MINUTES`, and returns a `QueryTaskResult`. The unary V2 clients loop on
the token inside one task so a task measures one full logical query. Failures print the
`ExceptionalResult` message.

### Task 6 — Documentation

`doc/benchmark-overview.md` §6: extend the client table, add a "Loader options" subsection with the
argument list, the deep-fixture example from D7, the `--skip-load` workflow, and a note that the
query window is the last minute of the loaded history. `doc/release-notes/rel-1.16.0.md`: one line
under a "Benchmarks" heading listing the new clients.

## Out of scope

- A sharded local fixture: the plan walker reads the unsharded explain shape (class javadoc); a
  sharded explain is nested per shard. Pinning the sharded shape is a separate piece of work, and the
  SLAC rehearsal covers it operationally.
- The covered pre-scan alternative for the span cost (#274 D11): only if the customer's `pvStats`
  distribution shows wide spans within classes; the D4 tests are what it would extend.
- Any change to the ingestion benchmarks (Background §4).
- Persisting benchmark results or trend tracking.

## Dependencies and sequencing

- **After #274 PR 1**: Task 1 edits the file whose `bucketSamplesQueryV2` call #274 changes, and the
  samples benchmarks should measure the sliced path, not the one being replaced.
- Task 1–2 and Tasks 3–6 are independent; two PRs if convenient, one is fine.
- The deep fixture adds roughly one second to the shared `dp-test` run; if that is felt in CI, lower
  `DEEP_BUCKETS` to 10,000 (the two window positions stay far apart).
