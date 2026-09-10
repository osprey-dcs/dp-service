# dp-service #232: per-PV max bucket span at ingestion; query lower bound derived from it; startup scan removed

Epic: #257. This is the single implementation ticket for the epic's main objective, fast-tracked
for the next release. Consolidation decisions (re-titling #232, closing #259 and #260, re-scoping
#201) are recorded in the 2026-09-10 comment on #257.

## Overview

Bucket time-range queries carry a lower bound on `firstTime.seconds` so the compound index scan
does not cover each PV's entire history (#197). Today that bound is sized by a single configured
value, `Buckets.maxBucketSpanSeconds`, and is only safe if every stored bucket satisfies it, which
`BucketSpanVerifier` establishes by a full scan of `buckets` at startup. On the SLAC archive
(33.8M buckets, sharded) that scan runs for hours before the gRPC port binds, and the configured
value had to be raised to 3,700,000s to cover four outlier PVs, which widens the bound for every
other PV to a 42-day lookback.

This ticket replaces the configured, globally-verified bound with a per-PV maximum span that
ingestion maintains as it writes:

- **Ingestion** records, per PV, the largest `lastTime.seconds - firstTime.seconds` it has ever
  written, in a new `pvStats` collection, *before* inserting the buckets. A per-process
  high-watermark cache keeps steady-state extra writes at zero.
- **Query** reads the maximum over the PVs a request names and uses that as the bound. The bound
  is still one scalar per query; nothing about the filter shape changes.
- **The startup scan, its marker collection, the process-wide enable flag, and the per-service
  init call are deleted.** `Buckets.maxBucketSpanSeconds` becomes an ingestion-only validation limit.
- **Schema migration v5** seeds `pvStats` from existing buckets in one aggregation pass and drops
  the dead `bucketSpanVerification` collection.

No dp-grpc proto change. `queryPvStats` / `queryProviderStats` stay on aggregation (#201). The
`pvStats` document is designed to take the remaining #201 statistics as further fields on the same
per-request update.

## Background: triage findings

### 1. The scan never measured the maximum span, so there is nothing to "read" yet

`BucketSpanVerifier.findOffendingBucket()` (`src/main/java/com/ospreydcs/dp/service/common/bson/bucket/BucketSpanVerifier.java:236-276`)
matches `$expr: lastTime.seconds - firstTime.seconds > limit` with `$limit: 1`. It answers "does
any bucket exceed the configured limit?", and the bound itself always comes from configuration
(`MongoQueryFilterBuilder.java:158`). A per-PV maximum therefore has to be *captured* before it
can be read, which means one seed pass over existing buckets. That pass belongs in the migration
runner, where v4 (`V4StampColumnDiscriminators`) already established the choreography for a
one-time full scan of `buckets` (`doc/schema-migration.md:269-302`).

### 2. #259 item 1 was a false premise; item 2 is obsolete

`MongoIngestionHandler.init_()` (`src/main/java/com/ospreydcs/dp/service/ingest/handler/mongo/MongoIngestionHandler.java:58-74`)
has never called `verifyBucketSpans()`. The call exists only in `MongoQueryHandler.java:78` and
`MongoAnnotationHandler.java:96`; `git log -S verifyBucketSpans` on the ingestion handler is empty
and the `rel-1.15.0` tag confirms it. The ingestion service has never run the scan, and the option-0
runbook's `DP_BUCKETS_VERIFY_SPANS_ON_STARTUP=false` on ingestion is a no-op. Item 2 (type-tolerant
marker read) has nothing to apply to once the verifier is deleted. #259 was closed on this basis.

### 3. The bound is already a single scalar per query and can stay that way

Every caller passes a begin time into `bucketOverlapsRangeFilter()` and receives one
`firstTime.seconds >= begin - span` predicate: the V1 path via `executeBucketDocumentQuery()`
(`MongoSyncQueryClient.java:49-74`), and the V2 paths via `bucketBaseFilterV2()` (`:552-567`)
and `executeQuerySamplesV2()` (`:423-465`), which `$or` one predicate per retrieval fragment.
Taking the maximum over the request's PVs keeps that shape. No per-PV `$or` branches, so no
interaction with the #203 fragment multiplication.

### 4. #198 is not on the critical path

#198's own follow-up comment measured the total-nanos rewrite at 39,826 -> 39,822 keys examined
for the same query, while narrowing the span assumption from 86,400s to 300s took it to 625. Its
surviving benefits are planner cost and a leaner index. The per-PV value plugs into whichever
predicate #198 later emits (seconds today; nanos with a one-second margin later).

### 5. No plan-shape test exists in the repo

`MongoBucketQueryPlanTest`, referenced in earlier notes, was written on the held #231 branch and
never pushed. `grep -rl explain src/test` finds nothing. The only guards on the bound are
filter-BSON equality tests in `MongoQueryFilterBuilderTest.java:178-290`, which cannot detect an
index-bound regression. Task 10 adds an explain-based test.

### 6. Every bucket in an ingestion batch shares one span

`BucketDocument.columnBucketDocument()` (`BucketDocument.java:91-119`) builds each bucket's
`DataTimestampsDocument` from the same `request.getIngestionDataFrame().getDataTimestamps()`, so
one request yields one `(firstTime, lastTime)` pair and N PV names. The stats update is one span
value applied to N PVs, not N independent computations.

### 7. Where a null cursor is treated as an error

The V2 dispatchers (`QuerySamplesUnaryDispatcher.java:77`, `QuerySamplesStreamDispatcher.java:69`,
`QueryBucketsUnaryDispatcher.java:60`) and the export jobs
(`ExportDataJobAbstractBucketed.java:68`, `ExportDataJobAbstractTabular.java:100`) already treat a
null cursor from the client as an error. `executeQueryTable()` already returns null for an unset
name spec (`MongoSyncQueryClient.java:127`). A failed `pvStats` read can therefore surface as a null
cursor on every path (D8). Task 6 verifies `QueryDataDispatcher`/`QueryTableDispatcher` handle null
the same way before relying on it for V1.

## Design decisions

### D1 — Per-PV storage; one scalar bound per query, the maximum over the PVs named

Storage is per PV (option B2 on #257). At query time the client reads the documents for the
request's PV set and uses the largest value. A query touching only well-behaved PVs gets a tight
bound even on an archive holding a few outliers, which is the SLAC case exactly.

*Rejected:* a single global statistic (B1). Same write and read cost, but every query inherits the
worst PV's span, which on SLAC is the 42-day bound this ticket exists to remove. *Rejected:* a
per-PV `$or` branch in the filter. It multiplies with #203's per-fragment `$or` and changes the
filter shape for no gain over the per-query maximum.

### D2 — A dedicated `pvStats` collection keyed by PV name as `_id`

One document per PV, `_id` = pvName, field `maxBucketSpanSeconds` (long). The default `_id` index
serves both the `$max` upsert and the `$in` read; no additional index. Future #201 statistics are
further fields on this document.

*Rejected:* the user-managed `pvMetadata` collection (#232's original sketch). Its records can be
deleted through the API, and deleting one would silently remove the evidence the bound relies on.
*Rejected:* ObjectId `_id` plus a unique `pvName` index. An extra index to maintain, for nothing.

### D3 — The statistic is the seconds-field difference

`maxBucketSpanSeconds` = max over the PV's buckets of `lastTime.seconds - firstTime.seconds`,
computed from the two `TimestampDocument.seconds` fields, not from nanosecond spans. This is the
exact quantity the bound consumes: for any bucket overlapping `[begin, end)`,
`lastTime.seconds >= beginSeconds`, so `firstTime.seconds >= beginSeconds - spanSeconds`. It is also
what the v5 seed computes with `$subtract`, so ingestion and the seed cannot disagree.

Under #198 a nanos bound is derived as `(maxBucketSpanSeconds + 1) * 1e9`, since the nanos span is
strictly less than one second more than the seconds-field difference.

### D4 — Stats are written before the buckets, and a failed stats write fails the request

Inside `insertBatch`, the `pvStats` update precedes `insertMany`. At every instant, the stored
maximum for a PV is at least the span of every stored bucket for that PV: a stats write that
succeeds without the bucket insert over-states, which errs safe (a bound at least as wide as
needed). A stats write that fails returns an error `IngestionTaskResult` and no buckets are
inserted; the request is reported as an error like any other database failure.

*Rejected:* buckets first, then stats. A query between the two writes could miss the new bucket.
*Rejected:* fire-and-forget stats. An uncovered bucket is a silent wrong answer on every later query.

### D5 — A per-process high-watermark cache gates the write

`ConcurrentHashMap<String, Long>` on the updater. A PV is written only if the request's span
exceeds its cached value or it has no cached value. The cache is advanced only after the bulk write
succeeds, so a cached value is always one this process has stored. Skipping is safe across
processes because `$max` is monotone: a peer can only have raised the stored value. Steady-state
cost for uniform per-PV spans is zero extra writes; first sight of a PV costs one upsert in an
unordered bulk. If the bulk throws, no entry is advanced (a partially applied bulk is re-written
harmlessly next time).

The cache is unbounded and not pre-warmed. PV counts of 10^4 to 10^5 make it megabytes at most;
eviction is safe (a miss just writes) and can be added later without changing semantics.

*Rejected:* an unconditional bulk of N upserts per request. Doubles the write operations per
request against the benchmark's 200-column frames. *Rejected:* pre-warming from `pvStats` at
startup. Saves one bulk per PV per process lifetime, adds a startup read; not worth it for v1.

### D6 — A PV with no `pvStats` document contributes nothing to the bound

After the v5 seed and with an upgraded ingestion service, a PV without a document has no stored
buckets. Such PVs are ignored when taking the maximum; a request naming only such PVs gets a bound
of `beginSeconds`, which is correct for an empty result.

*Rejected:* treating a missing document as "unbounded". One mistyped PV name in a request would
turn the whole query into the multi-minute unbounded scan. *Rejected:* flooring the bound at
`Buckets.maxBucketSpanSeconds`. This would cover buckets written by a not-yet-upgraded 1.15
ingestion process, but it also caps the win at the configured value, which at SLAC is 3.7M seconds.
The rolling-upgrade window is a deployment constraint instead (Dependencies below), the same one
`doc/schema-migration.md:169-184` already states for every migration.

### D7 — No read-side cache; one indexed read per query

The query client reads `pvStats` on every request. A cached value can only be too small once a
longer bucket is ingested, and a too-small bound silently drops that bucket. The read is a `$in` on
`_id` against a collection with one document per PV, sub-millisecond even at V2's 10,000-PV
resolution ceiling.

A consequence worth naming: the recourse for an out-of-band writer (a direct Mongo import that
bypasses ingestion) is one `updateOne` with `$max` on the affected PV's document. No restart, no
rescan, no configuration change. That is why no operator kill switch for the bound is kept (D9).

### D8 — A failed `pvStats` read is a query error, not a silent unbounded scan

If the stats read throws, the client logs it and returns a null cursor, which every dispatcher
already reports as an error (triage 7). *Rejected:* degrading to the unbounded scan. On SLAC that
is a four-minute query that hides a database problem behind slow but "successful" responses; the
bucket read itself would most likely fail the same way a moment later.

### D9 — The verification mechanism is deleted, not disabled

`BucketSpanVerifier`, the `bucketSpanVerification` marker, `verifyBucketSpans()`, the
`Buckets.verifyBucketSpansOnStartup` key, `BucketSpanLimits.isQueryLowerBoundEnabled()` /
`disableQueryLowerBound()`, and the two handler init calls are removed. The disable path existed
because the archive might violate a *configured* claim; with the bound derived from observed data
there is no configured claim to violate, and D7 gives a targeted recourse.

*Rejected:* keeping a `Buckets.applyQueryLowerBound` kill switch. It would be the only remaining
reader of a process-wide static, and its use case is served better by a `pvStats` write.

### D10 — Migration v5 seeds with `$group` + `$merge`, `$max` on match, and drops the marker collection

```js
db.buckets.aggregate([
  {$group: {_id: "$pvName", maxBucketSpanSeconds: {$max: {$subtract: [
      "$dataTimestamps.lastTime.seconds", "$dataTimestamps.firstTime.seconds"]}}}},
  {$merge: {into: "pvStats", on: "_id",
            whenMatched: [{$set: {maxBucketSpanSeconds: {$max: [
                "$maxBucketSpanSeconds", "$$new.maxBucketSpanSeconds"]}}}],
            whenNotMatched: "insert"}}
], {allowDiskUse: true})
```

Idempotent: a re-run recomputes the same maxima and `$max` leaves a larger stored value alone. Safe
against an upgraded ingestion process writing concurrently, for the same reason. One full pass over
`buckets` with per-PV group state, so memory is bounded by PV count. The migration then drops
`bucketSpanVerification`; dropping a nonexistent collection is a no-op, so that step is idempotent
too. Both steps operate on raw `Document`s per the `Migration` contract.

*Rejected:* iterating the group cursor in Java and issuing the upserts client-side. Same effect,
more code, one more place to get the `$max` semantics wrong. *Rejected:* no seed, letting
ingestion populate `pvStats` over time. Every historical bucket would be uncovered until its PV
ingested a bucket at least as long, i.e. possibly never.

Verify the `$merge` pipeline form (`$$new`, `whenMatched` pipeline) against a real `mongo:8.0`
container before relying on it, per the #254 lesson.

### D11 — Pattern queries resolve the regex against `pvStats` ids

`executeQueryTable()`'s `PVNAMEPATTERN` branch (`MongoSyncQueryClient.java:121-125`) has no PV list.
The client runs the same case-insensitive regex against `pvStats._id` and takes the maximum over
the matches. Same one read as the list case, and a tighter bound than a global maximum. V2 pattern
queries already resolve to explicit names (`resolvePvNamesByPattern`, `:273-295`) and use the list path.

### D12 — `Buckets.maxBucketSpanSeconds` stays, ingestion-only

`IngestionValidationUtility` (`:132-146`, `:183-201`) keeps rejecting frames over the limit. The
query side stops reading it. The epic's "ingestion limit must stay below the verified limit"
coupling dissolves, and SLAC can lower the ingestion value back to 86,400 after upgrading without
touching the query deployment.

### D13 — The malformed-bucket diagnostic goes with the scan

`BucketSpanVerifier` also reported buckets missing `dataColumn`/`dataTimestamps`. A malformed
bucket still surfaces as a `DpException` at query time (the dispatcher contract is unchanged), and
locating offenders offline belongs with #258's utility.

## Implementation tasks

### Task 1 — Constants and document class

- `src/main/java/com/ospreydcs/dp/service/common/bson/BsonConstants.java` (after `:74`): add
  `BSON_KEY_PV_STATS_PV_NAME = "_id"` and `BSON_KEY_PV_STATS_MAX_BUCKET_SPAN_SECONDS = "maxBucketSpanSeconds"`.
- New `src/main/java/com/ospreydcs/dp/service/common/bson/pvstats/PvStatsDocument.java`:
  `@BsonId String pvName`, `long maxBucketSpanSeconds`, getter and setter for both (POJO codec
  silent-drop pitfall). Class Javadoc states D2/D3 and that #201's fields extend this document.
- `MongoClientBase.getPojoCodecRegistry()` (`MongoClientBase.java:162`): register `PvStatsDocument`.

### Task 2 — Collection wiring

- `MongoClientBase.java:48`: add `COLLECTION_NAME_PV_STATS = "pvStats"`, and
  `COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY = "bucketSpanVerification"` with a comment that
  nothing initializes it: it is kept only so the emptiness probe still reads a pre-v5 database
  holding only that marker as legacy, and v5 drops it.
- Abstract `initMongoCollectionPvStats(String)`; call it after `:465`; no `createMongoIndexesPvStats`
  (default `_id` index, D2).
- `MongoSyncClient.java` (after `:216`): `mongoCollectionPvStats` field and implementation.
  `MongoAsyncClient.java` (after `:179`): implementation mirroring the sample-status one.
- `MongoClientBase.init()` log line listing collection names: add pvStats.

### Task 3 — Ingestion updater

- New `src/main/java/com/ospreydcs/dp/service/ingest/handler/mongo/client/PvStatsMaxSpanUpdater.java`:
  constructed with `MongoCollection<Document>` for `pvStats`; `ConcurrentHashMap<String, Long>`
  watermark; `void recordSpan(Collection<String> pvNames, long spanSeconds) throws DpException`.
  Filters names by watermark, issues one unordered `bulkWrite` of `UpdateOneModel(eq(_id, pv),
  Updates.max(maxBucketSpanSeconds, span), upsert(true))`, advances the watermark with
  `merge(pv, span, Math::max)` only after the bulk returns. Wraps `MongoException` in `DpException`
  with the exception as cause; advances nothing on failure. Class Javadoc carries D4/D5 verbatim,
  including why a cached value is always one this process stored.
- `MongoSyncIngestionClient.insertBatch()` (`:126-149`): before `insertMany` (`:142`), derive
  `spanSeconds` from `dataDocumentBatch.get(0).getDataTimestamps()` (first/last `getSeconds()`,
  triage 6; empty batch or null timestamps returns an error result), collect `getPvName()` over the
  batch, call `recordSpan`. On `DpException` return `new IngestionTaskResult(true, "error recording
  bucket span statistics: " + msg, null)` and log with the exception object. The updater is a field
  created in `init()` after the collection exists.
- `MongoAsyncIngestionClient.insertBatch()` (`:45`): no stats update; comment that this client is
  test-only (`MongoAsyncIngestionHandlerTest`) and does not maintain `pvStats`, in the same spirit as
  its `runSchemaMigrations()` note.

### Task 4 — Filter builder takes the span as a parameter

- `MongoQueryFilterBuilder.bucketOverlapsRangeFilter()` (`:123-170`): add trailing parameter
  `long maxBucketSpanSeconds`; delete the `isQueryLowerBoundEnabled()` branch (`:149-151`) and the
  `BucketSpanLimits` read (`:158`); reject a negative span with `IllegalArgumentException`; keep the
  saturating `subtractExact` (`:159-165`), which now falls back to omitting only the lower bound.
  Rewrite the Javadoc: the bound is supplied by the caller from `pvStats`, and why it is exact for
  the seconds-field difference (D3). Remove the `BucketSpanLimits` import.

### Task 5 — Query client resolves the span per request

- `MongoSyncQueryClient.java`: add `long resolveMaxBucketSpanSeconds(Collection<String> pvNames)`
  and `long resolveMaxBucketSpanSeconds(Pattern pvNamePattern)`, both `throws DpException`. Read
  `mongoCollectionPvStats` with projection on the span field, return the maximum, `0` when nothing
  matches (D6). Log at debug the resolved span and PV count.
- `executeBucketDocumentQuery()` (`:49`): add `long maxBucketSpanSeconds` parameter and pass it
  through (`:58`). `executeDataBlockQuery()` (`:77`) and `executeQueryData()` (`:90`) resolve by
  name list; `executeQueryTable()` (`:108`) resolves by list or pattern (D11). Each catches
  `DpException`, logs with the exception object, and returns null (D8, triage 7).
- `bucketBaseFilterV2()` (`:552`) gains a span parameter; `executeQueryBucketsV2()` (`:373`),
  `executeQueryBucketsV2Stream()` (`:403`), and `executeQuerySamplesV2()` (`:423`, call at `:439`)
  resolve from `resolvedQuery.getPvNames()` inside their existing try/catch, returning null on failure.
- `MongoQueryClientInterface.java`: delete the `verifyBucketSpans()` default (`:25-39`).

### Task 6 — Verify V1 dispatchers report a null cursor as an error

`QueryDataJob.java:36-38` and `QueryTableJob.java:35-37` pass the cursor straight to
`handleResult()`. Confirm `QueryDataDispatcher`, `QueryDataStreamDispatcher`,
`QueryDataBidiStreamDispatcher`, and `QueryTableDispatcher` send an error response for null (the
unset-name-spec path already produces one); add the check where missing, so a failed stats read
cannot hang the caller's stream.

### Task 7 — Deletions

- Delete `BucketSpanVerifier.java` and `BucketSpanVerifierTest.java`.
- `MongoSyncClient.java`: delete `CFG_KEY_VERIFY_BUCKET_SPANS_ON_STARTUP` (`:34-36`),
  `verifyBucketSpans()` (`:240-281`), and the two imports.
- `MongoQueryHandler.java:75-78`, `MongoAnnotationHandler.java:93-96`: delete the call and comment.
- `BucketSpanLimits.java`: delete `queryLowerBoundEnabled`, `isQueryLowerBoundEnabled()`,
  `disableQueryLowerBound()` (`:91-111`); drop the flag reset from `resetCachedLimitForTesting()`
  (`:120`); rewrite the class Javadoc and the "IMPORTANT deployment note" to say the limit now binds
  ingestion only and the query bound comes from `pvStats`.
- `src/main/resources/application.yml:69-93`: delete `verifyBucketSpansOnStartup`; rewrite the
  `maxBucketSpanSeconds` comment (ingestion-only; the query bound is per PV from `pvStats`; the
  "must be at least the largest stored span" sentence goes). `src/test/resources/application.yml:16-22`:
  delete the key and its comment.

### Task 8 — Migration v5

- New `src/main/java/com/ospreydcs/dp/service/common/mongo/migration/migrations/V5SeedPvStatsMaxBucketSpan.java`:
  D10 pipeline via the driver's `Aggregates.group` / `Aggregates.merge` with `MergeOptions`
  (`whenMatched(PIPELINE)` with the `$set`/`$max` stage, `whenNotMatched(INSERT)`), `allowDiskUse(true)`,
  then `database.getCollection("bucketSpanVerification").drop()`. Javadoc: why idempotent, why
  safe under concurrent ingestion, expected one-time full scan. Wraps `MongoException` in `DpException`.
- `SchemaMigrationRunner.java`: `SCHEMA_VERSION = 5` (`:62`), add to `MIGRATIONS` (`:69`), add
  `COLLECTION_NAME_PV_STATS` to `MANAGED_COLLECTION_NAMES` (`:98`), replace the
  `BucketSpanVerifier` reference (`:109`) with the legacy constant from Task 2, update the Javadoc
  (`:86-96`) that explains the marker's inclusion.
- `SchemaMigrationRunnerTest.java`: `:182-184` and `:332` reference `BucketSpanVerifier`; use the
  legacy constant and drop the class from the reflection list (only `MongoClientBase` remains, and
  the comment explaining the two-class list goes).
- New `V5SeedPvStatsMaxBucketSpanTest` following `V4StampColumnDiscriminatorsTest`: seeds the
  per-PV maximum from mixed-span buckets across several PVs; apply-twice is a no-op; a larger
  pre-existing `pvStats` value survives (models ingestion racing ahead); empty `buckets` yields no
  documents; the legacy collection is dropped and a second run with it absent succeeds.
- `doc/schema-migration.md`: history table row (`:201-207`) and a "Note on version 5": one-time
  full scan with the same waiting-service choreography as v4; post-run check
  `db.pvStats.countDocuments()` against `db.buckets.distinct("pvName").length`; the out-of-band
  writer recourse from D7; update the two prose references to `bucketSpanVerification` (`:71-77`).

### Task 9 — Test framework and existing tests

- `MongoTestClient.java`: add `findPvStats(String pvName)` (retry variant, like `findPvMetadata`
  at `:221`), `findPvStatsNoRetry`, and `upsertPvStatsMaxSpan(String pvName, long seconds)` for
  seeding; and `insertPvStatsDocument`.
- `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()` (`:481-`): after the
  bucket checks, assert `pvStats` holds each PV with `maxBucketSpanSeconds >= lastSeconds - firstSeconds`
  of the request. This is the regression guard for D4 on every ingestion IT.
- `MongoQueryFilterBuilderTest.java`: `:178` rewritten to pass a span and expect the bound;
  `:211` ("omits when disabled") deleted and replaced by a negative-span rejection test; `:244`
  (saturation) rewritten for the parameter.
- `ExportDataBucketSpanIT.java:141`: rename and rewrite. Insert the over-long bucket directly
  (as now), seed `pvStats` for its PV with its span (what v5 would have recorded), and assert the
  export returns it alongside the compliant buckets. Add a second test: with `pvStats` seeded only
  by ingestion, and `Buckets.maxBucketSpanSeconds` set far above the data's spans, the query still
  uses the tight per-PV bound (assert via the plan-shape helper from Task 10, or by inserting an
  out-of-band bucket that starts before `begin - observedSpan` and confirming it is *not* returned,
  which documents the D7 recourse and its need).
- Remove `BucketSpanLimits.resetCachedLimitForTesting()` calls that existed only to reset the flag
  where the limit itself is untouched (`ExportDataBucketSpanIT.java:54,68`).

### Task 10 — Explain-based plan-shape test

New `src/test/java/com/ospreydcs/dp/service/query/handler/mongo/client/MongoBucketQueryPlanTest.java`:
insert a few hundred buckets for two PVs via `MongoTestClient.insertBucketDocument()`, seed
`pvStats`, build the V1 filter exactly as `executeBucketDocumentQuery()` does, run
`find(filter).sort(...).explain(ExplainVerbosity.QUERY_PLANNER)`, and assert the winning plan's
`indexBounds` for `dataTimestamps.firstTime.seconds` has lower bound `beginSeconds - span` and no
`COLLSCAN` stage. Result-level tests cannot catch this class of regression (triage 5).

### Task 11 — Updater unit test

`PvStatsMaxSpanUpdaterTest` against the test database: first sight upserts; a larger span raises the
stored value; a smaller or equal span issues no write (assert via a `MongoCollection` spy counting
`bulkWrite` calls, or via the document's absence of change); two names in one call share one bulk;
watermark is not advanced when the collection throws (use a collection handle on a closed client).

### Task 12 — CLAUDE.md

- Replace "Max Bucket Span Invariant (issue #197)" (`:600-615`) with a "Per-PV bucket span bound
  (issue #232)" section carrying D3, D4, D5, D6, D7, D8 as invariants: stats-before-insert; never
  cache on the read side; missing document means no buckets; out-of-band writers must `$max` the
  PV's document; `Buckets.maxBucketSpanSeconds` is ingestion-only.
- MongoDB Collections list: add `pvStats`. Schema Migration section (`:767-769`): the
  `bucketSpanVerification` mention becomes the legacy constant on `MongoClientBase`.
- Testing Strategy: note the plan-shape test and the ingestion wrapper's `pvStats` assertion.

### Task 13 — Benchmark

Run `BenchmarkIngestDataStream --double-column` (defaults: 7 threads, 20 streams, 1000 rows,
200 columns, 60 s) on `main` and on this branch against the same local Mongo; record throughput
and the `pvStats` document count afterwards in the PR description. The expected result is one bulk
per stream at warm-up and no measurable steady-state difference (D5). If the first-request latency
is visible, pre-warming (rejected in D5) is the fallback, not removing the write.

## Out of scope

- The remaining #201 statistics (`firstDataTimestamp`, `lastDataTimestamp`, `numBuckets`,
  `lastBucket*`, `lastProvider*`) and switching `queryPvStats` / `queryProviderStats` off
  aggregation: **#201**, re-scoped. Counters cannot use the D5 skip; that cost decision lives there.
- Total-nanosecond time fields, planner-cost reduction, the two-field index: **#198**.
- Repairing SLAC's four over-long PVs: **#258**. Under this ticket those PVs keep their 42-day
  bound; every other PV does not.
- A bounded or pre-warmed watermark cache (D5), a per-(PV, era) span, and a database-level span
  validator (#257 "enforce the invariant at the database"): follow-ons if ever needed.
- Any dp-grpc proto change.
- The async Mongo client remains test-only and does not maintain `pvStats` (Task 3).

## Dependencies and sequencing

- **No dependency on dp-grpc, #198, or #258.** Tasks 1–2 first (schema and wiring), then 3
  (write path) and 4–6 (read path) in either order, then 7 (deletions) once nothing references the
  flag, then 8 (migration), then tests and docs. The migration must ship in the same PR as the
  read-path change: a query build that expects `pvStats` against an unseeded archive would treat
  every legacy PV as having no buckets (D6).
- **Deployment order matters once, at upgrade.** Stop all services, or upgrade ingestion first. A
  1.15 ingestion process still writing after the v5 seed produces buckets no statistic covers; an
  upgraded ingestion process running against a not-yet-upgraded query service is harmless. This is
  the same constraint `doc/schema-migration.md` states for every migration.
- **Restart ingestion after any manual edit to `pvStats`** that lowers or removes a value; the
  in-memory watermark would otherwise skip writes the collection no longer reflects. Raising a
  value by hand (the D7 recourse) needs no restart.
- **SLAC after upgrade:** lower `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS` on ingestion back to 86,400;
  remove it from query and annotation (unread). The four outlier PVs stay slow until #258.
