# dp-service #274: querySamples drops PVs on byte-budget trip; time-sliced samples retrieval; outbound flow control

## Overview

Pre-1.16.0 review of the PV time-series query paths found one confirmed silent wrong answer and
four performance/robustness defects, all in the query-side assembly and dispatch code. This ticket
fixes all of them, plus the span-class partition that #276 originally deferred (pulled in on
2026-09-14; #276 is closed as absorbed). The companion #275 adds the deep-history plan-shape test and
the benchmark coverage.

| # | Defect | Fix |
|---|---|---|
| 1 | Unary `querySamples` silently omits every PV the cursor had not reached when the byte budget trips (confirmed, see Background §1) | Time-sliced page retrieval: every slice is drained for all PVs before any row is emitted (D1–D4) |
| 2 | Each unary page drains up to the whole byte budget to emit `pageSize` rows (read amplification) | Same: slices stop once `pageSize` distinct timestamps exist (D2) |
| 3 | `querySamplesStream` materializes the entire window in memory | Same slicing, emitting after each complete slice (D6) |
| 4 | Column-index lookup is an `indexOf` string search per retained sample | Hash-backed index, lookup hoisted out of the sample loop (D9) |
| 5 | Three server-streaming dispatchers call `onNext` with no readiness check | `OutboundReadinessGate` before every streamed `onNext` (D8) |
| 6 | `QueryDataDispatcher` logs a deserialization failure without the exception | Pass the exception (D10) |
| 7 | The #232 span bound is a max over the request's PVs, and every key inside it is a document fetch, so one long-span PV widens every PV's scan (measured, Background §6; originally filed as #276) | Partition the request's PVs into span classes, one find per class, merged in sort order (D11) |

Target: 1.16.0, ahead of the release. Two PRs (see Dependencies and sequencing).

## Background: triage findings

### 1. The byte-budget stop and the PV-major sort do not compose

- `MongoSyncQueryClient.bucketFind()` (`MongoSyncQueryClient.java:211-216`) sorts every bucket
  retrieval `(pvName, firstTime.seconds, firstTime.nanos)` (`bucketSort()`, `:823-828`), a prefix of
  the hinted index (#271). The cursor is therefore **PV-major**: all of PV A's buckets in the window,
  then all of PV B's.
- `TabularDataUtility.addBucketsToTable()` (`TabularDataUtility.java:116-149`) checks the
  accumulated `DataValue` size against `sizeLimit` after each bucket and closes the cursor once it
  is exceeded.
- `QuerySamplesUnaryDispatcher.executeAndDispatch()` (`:86-132`) passes the outgoing message budget
  as that limit, and `emitPage()` (`:139-187`) handles the trip by dropping only the **last**
  assembled timestamp ("the last, possibly-incomplete timestamp"). The count-driven branch
  (`allTimestamps.size() > pageSize`, `:147-150`) ignores `byteBudgetHit` entirely.

Under PV-major order, when the limit trips partway through PV A, PVs B..N have contributed
nothing. Every assembled timestamp is incomplete, not just the last one. The page is emitted with
the later PVs' columns all-unset (indistinguishable from "no sample at this timestamp"), and the
resume token is a timestamp, so the next page starts at the same PV A position and trips the same
way. The later PVs are never read.

Reproduced on 2026-09-14 with a two-PV variant of
`MongoSyncQuerySamplesV2Test.testByteBudgetPagingSeamNoGapNoOverlap` (budget 40 bytes, `spv_a` 10 Hz
and `spv_b` 5 Hz over the 3 s fixture, paged to completion):

| | pages | rows | `spv_a` values | `spv_b` values | error |
|---|---|---|---|---|---|
| expected | | 30 | 30 | 15 | none |
| observed | 4 | 30 | 30 | **0** | none |

The existing seam test is single-PV, which is why it passes. With the production budget
(`GrpcServer.incomingMessageSizeLimitBytes`, 4,096,000) a double `DataValue` serializes to 9 bytes,
so the trip point is ~455k values from the first PV in name order: a window longer than ~7.5 min
at 1 kHz, ~12.6 h at 10 Hz, ~5.3 days at 1 Hz. The #244 plan (D7) records the "last
possibly-incomplete timestamp" assumption, so the design carried the defect from the start.

### 2. Read amplification follows from the same drain

Because retrieval is not `.limit()`-ed and the drain stops only at the budget, each page reads
`min(remaining window, ~4 MB of values)` to emit `pageSize` rows. At the default 10,000 rows and one
double PV that is ~455k values read per 10k emitted. `QueryClient.java:807-809` documents this to
callers ("a small limit costs server work without saving any"); that javadoc changes under D2.

### 3. The stream path has no cap at all

`QuerySamplesStreamDispatcher.executeAndDispatch()` passes `null` as `sizeLimit`
(`QuerySamplesStreamDispatcher.java:286-289`), materializing the whole window. `queryV2MaxResolvedPvCount`
(10,000) bounds the width but nothing bounds the length: one request over a long range exhausts
the query server's heap and takes every other in-flight query with it. `queryTable` errors at the
budget; `queryDataStream`/`queryBucketsStream` iterate their cursor. This is the only uncapped path.

### 4. Per-sample `indexOf`

`TimestampDataMap.getColumnIndex()` (`TimestampDataMap.java`) is `ArrayList.indexOf` over the column
names, and `TabularDataUtility.addColumnsToTable()` calls it at `TabularDataUtility.java:269` inside
the per-timestamp loop, once per retained value. Cost is O(columns) string compares per sample; on a
1,000-PV `querySamples` that is ~500 compares per sample. The registration call at `:240` (and
`:125`) must stay: `getColumnIndex()` is a mutator that determines the emitted column set (#207).

### 5. No outbound flow control

No class under `query/` references `ServerCallStreamObserver`, `isReady()`, or `setOnReadyHandler`.
`QueryDataStreamDispatcher`, `QueryBucketsStreamDispatcher`, and `QuerySamplesStreamDispatcher` call
`onNext` in a loop; gRPC buffers every message a slow client has not consumed. The ingestion
subscription paths already cast to `ServerCallStreamObserver` for `isCancelled()`
(`ingestionstream/handler/monitor/EventMonitor.java:225`), so the cast is established practice.
The three dispatchers are constructed synchronously inside the `MongoQueryHandler.handle*` methods
(`MongoQueryHandler.java:249`, `:299`, and the V1 `handleQueryDataStream`), i.e. on the gRPC thread
before the service method returns, which is where gRPC requires `setOnReadyHandler` to be called.
`queryDataBidiStream` is client-paced (cursor `next`) and is left alone.

### 6. Slice cost is a document fetch per key, so slices must be few

Explain on the local benchmark archive (dp-benchmark, 240k buckets, MongoDB 8.0) with the exact
production filter, sort, and hint: the overlap residual (`lastTime >= begin` and the nanos half of
`firstTime < end`) is evaluated on the `FETCH` stage, and `totalDocsExamined == totalKeysExamined`
in every case tried (span 0 and 60 s, one and five PVs). Every key in `[begin − span, end]` is a
document fetch. Consequence for this ticket: each slice's `find` pays that full range, so the
slicing design must converge to few slices per page (D2), not iterate small fixed slices. And the
bound is a **maximum over the request's PVs** (`resolveMaxBucketSpanSeconds`, `:65-71`), so one
long-span PV in a request widens every other PV's scan to `[begin − longSpan, end]` in fetched
documents. The customer archive has 42-day spans (one bucket per second over 42 days is ~3.6M
fetched-and-discarded documents per short-span PV per find); D11 partitions the request so that
cost is paid only by the PVs that carry it.

### 7. Telemetry integration (#212 landed 2026-09-15, PR #277, after this plan was written)

Every dispatcher now carries a `QueryTelemetry`; each `executeQuery*()` call is timed with
`addDbNanos`, and each cursor is folded in through `recordCursorTime()` in a `finally`, which
recognizes only `TimedMongoCursor`. Consequences for this ticket: the slicer times every slice's
find and folds every slice's cursor (one `db` stage across all slices); the merged span-class
cursor (D11) is wrapped in `TimedMongoCursor` with plain inner cursors, so the CLAUDE.md rule
"every cursor-returning query-client method wraps its cursor in `TimedMongoCursor`" still holds and
the merge overhead is charged to `db`, where the inner cursors' time already lands. Test
constructors gained a `QueryTelemetry` parameter; the new tests pass `new QueryTelemetry(...)`.

### 8. Config shadowing

`src/test/resources/application.yml` shadows the main file (`QueryHandler` block at `:23-26`); every
new key is added to both.

## Design decisions

### D1 — Time-sliced retrieval over all PVs per slice

The page window is retrieved in consecutive time slices `[sliceBegin, sliceEnd)`, each slice a single
`find` over **all** resolved PVs. A slice is either drained completely or discarded, so every
timestamp in an accepted slice is complete across every PV by construction. This is the property the
byte-budget stop violated.

Rejected alternatives:

- **Time-major sort** `(firstTime, pvName)`: makes the trip frontier correct with one cursor, but the
  hinted index cannot serve that sort, so every page pays a blocking `SORT` over the whole window and
  the scan is O(window) per page. Unusable on long windows.
- **Per-PV cursors with a shrinking frontier**: correct and single-pass per PV, but N finds per page,
  a merge, and a budget split across PVs. Too much new machinery for this week.
- **Metadata pre-pass** (projection without column bytes to find the page end): the fetch cost is per
  document regardless of projection (§6), so it is O(window) per page.

### D2 — Adaptive slice length, proportional not doubling

State per page: `sliceNanos`, starting at `QueryHandler.queryV2SamplesInitialSliceSeconds` (new key,
default 60) and never exceeding the remaining window. After a complete slice yielding `r` distinct
timestamps toward a target of `pageSize`:

```
factor = clamp(pageSize / max(r, 1), 1, 16)
sliceNanos = min(sliceNanos * factor, remainingWindowNanos)
```

The proportional step reaches the target in two or three finds for any steady rate (1 Hz: 60 s → 960 s
→ 9,984 s; 10 Hz: 60 s → 960 s → done; 1 kHz: the first slice already exceeds `pageSize`), and an
empty slice grows ×16 so a sparse PV does not cost hundreds of finds. Pure doubling from 60 s would
need eight finds to reach a 10k-row page at 1 Hz, each paying the §6 span scan. The factor floor of 1
means a slice never shrinks except on a budget trip (D3). Slice length is not carried across pages
(the token stays a position; a slice hint in the token is a possible follow-on, noted in Out of
scope).

### D3 — Budget trip: end the page, or halve and retry only when the page would be empty

The drain passes `previousDataSize` cumulatively, so the budget bounds the **page**, exactly as the
emitted message limit requires. On a trip during slice *k*:

- rows from slice *k* are removed from the map (`TimestampMap.removeFrom(sliceBeginSecs, sliceBeginNanos)`,
  new: clears every entry at or after the position; the seconds `TreeMap` makes it a `tailMap`
  operation plus one boundary second);
- if slices 1..k−1 contributed at least one row, the page **ends** here and the resume token is
  `sliceBegin` (the first undrained timestamp);
- otherwise (the page is still empty) the slice is halved and retried from the same begin. A trip on
  a slice one nanosecond wide, or one that admitted zero complete rows at that width, is the
  indivisible-oversized case and produces the same error message the dispatcher emits today.

This keeps today's zero-progress guarantee (every non-error page emits at least one row) and removes
the "drop the last timestamp" branch, which no longer has a meaning.

### D4 — The slice end is a retrieval bound on the fragment list, not a collapsed window (#207)

`AbstractQuerySamplesDispatcher.computeWindowBegin()` keeps its begin-only contract and its comment.
The slice is applied by a new `TimeInterval.clampToWindow(intervals, beginSecs, beginNanos, endSecs, endNanos)`
that intersects **each** resolved fragment with `[sliceBegin, sliceEnd)`; `clampToWindowBegin` becomes
the `end = +∞` special case (kept, delegating, so its existing callers and tests stand). Both the
client's per-fragment `$or` and the dispatcher's retention intervals for the slice come from this one
call, so the #207 single-source invariant is preserved: gaps between fragments inside a slice are
still trimmed at sample granularity. A slice whose clamp is empty (entirely inside a gap) is skipped
without a database call, and `sliceBegin` jumps to the next fragment begin.

### D5 — Status filter resolved per slice

`resolveSampleStatusTimestamps()` gains the slice end (it currently derives its end from the last
clamped fragment); it is called once per slice with the slice's clamped fragments, so the per-PV
timestamp sets are bounded by the slice rather than the whole window. Composition by intersection is
unchanged.

### D6 — The stream dispatcher uses the same slicer and emits after each slice

`QuerySamplesStreamDispatcher` drains slices with the budget as the per-slice limit (always halve and
retry on a trip; there is no page to end), and after each complete slice runs the existing row
chunker over the accumulated rows, then clears the map. Memory is bounded by one slice plus one
chunk; the class note "materializes the full table server-side" is deleted. A `sizeLimit` of `null`
is no longer passed anywhere on the samples paths.

### D7 — One shared slicer, two consumers

The loop (slice sizing, clamp, retrieval, trip handling, gap skipping, exhaustion) lives in
`AbstractQuerySamplesDispatcher` as a package-private `SliceDrain` helper with a callback per
accepted slice. The unary dispatcher's callback checks `distinct timestamps >= pageSize` to stop;
the stream dispatcher's emits. The unary `emitPage()` keeps only its count-driven truncation
(token = first dropped timestamp) plus the trip token from D3.

### D8 — `OutboundReadinessGate`

New `com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate`:

- `static OutboundReadinessGate forObserver(StreamObserver<?> observer)`: when the observer is a
  `ServerCallStreamObserver`, registers `setOnReadyHandler` (signals a lock/condition) and returns a
  gating instance; otherwise returns a no-op gate. Tests that pass plain observers are unaffected.
- `boolean awaitReady()`: returns immediately when ready or no-op; otherwise waits on the condition,
  re-checking `isReady()` and `isCancelled()`, up to `QueryHandler.streamReadyTimeoutSeconds` (new
  key, default 300). Returns `false` on cancellation or timeout.

Each of the three stream dispatchers constructs its gate in its constructor (on the gRPC thread, see
Background §5) and calls `awaitReady()` before every streamed `onNext`. On `false` the dispatcher
closes the cursor, logs at `warn` with the observer id, and returns without further sends: a
cancelled call has no reader, and a timed-out one gets no completion rather than another buffered
message. A slow reader now occupies one of the seven workers instead of the heap; that is the
intended trade and is documented in CLAUDE.md.

### D9 — Hash-backed column index

`TimestampDataMap` keeps `columnNameList` (order is the emitted column order) and adds
`Map<String, Integer> columnIndexByName`; `getColumnIndex()` consults the map and appends to both.
`addColumnsToTable()` resolves an `int[] columnIndexes` once per call, after the registration loop,
and uses it inside the timestamp loop. No behavior change; `TabularDataUtilityTest` already pins
registration order and all-empty columns.

### D10 — Exception object in the log

`QueryDataDispatcher.java:52`: `logger.error(errorMsg, e)`.

### D11 — Span-class partition: one find per class, merged in sort order

`resolveMaxBucketSpanSeconds(Collection)` is replaced on the list-based paths by
`resolveSpanClasses(Collection<String>) → List<SpanClass(List<String> pvNames, long maxSpanSeconds)>`,
reading the same single `$in` on `pvStats._id`. Each PV is assigned to a class by its own span:
class 0 for span ≤ 1 s, class *k* for `2^(k−1) < span ≤ 2^k`; a PV with no document is class 0 (D6
semantics unchanged: bound at `begin`). A class's bound is the **actual maximum span within the
class**, not the class ceiling. Every PV's scan is therefore within 2× of its own ideal, and the
number of finds equals the number of non-empty classes: at most ~22 for spans up to a few months,
one to three in practice.

Retrieval methods build one `find` per class (same overlap filter, sort, and hint as today, with the
class's own PV list and bound), open every cursor eagerly inside the existing `try`/`catch` (so the
null-cursor contract of `MongoSyncQueryClientMissingIndexTest` holds for every class), and return a
`MergedBucketCursor implements MongoCursor<BucketDocument>` that yields the smallest head by
`(pvName, firstTime.seconds, firstTime.nanos)`. Because classes hold disjoint PV sets and each cursor
is already in that order, the merge is a head comparison across at most a handful of cursors. The
V2 keyset seek and the unary `limit(pageSize + 1)` are applied to every class find; the consumer
stops after `pageSize + 1` merged documents as before.

**Single-class fast path**: when every PV lands in one class, the method returns that class's cursor
directly, with a filter byte-identical to today's. The plan test's existing assertions therefore keep
pinning the common case, and the partition is exercised only by the mixed-span cases (#275 D4).

Unchanged: the V1 `queryTable` pattern path (`executeQueryTable`, `PVNAMEPATTERN`) has no PV list and
keeps its single regex find with the pattern's maximum span; converting it would replace the bucket
regex with a `$in` over `pvStats` matches, which changes what a PV lacking a `pvStats` document
returns. It is a V1 method with a documented full-PV-walk cost and is left as is.

Rejected alternatives:

- **Per-PV bounds**: one find per distinct span value, unbounded find count.
- **A rooted `$or` with one branch per class in a single find**: MongoDB's subplanner can plan a
  rooted `$or` branch by branch, but with the hint and the `(pvName, firstTime)` sort it would need
  a `SORT_MERGE`, and #271 measured this planner losing bounds inside `$or` branches. A merge in the
  client is deterministic and needs no planner behavior to be re-verified per server version.
- **Covered pre-scan then fetch by `_id`**: removes the wasted fetches entirely, but is a second round
  trip on every query including single-span ones, and more code; it stays as the fallback option if
  the customer's span distribution turns out to be wide within classes (see #275 D4's test, which
  would be extended).

## Implementation tasks

### Task 1 — `TimeInterval.clampToWindow`

**File:** `query/handler/model/TimeInterval.java` (after `clampToWindowBegin`, `:162`)

Add `clampToWindow(List<TimeInterval>, long beginSecs, long beginNanos, long endSecs, long endNanos)`
(begin = max, end = min, drop empty); reimplement `clampToWindowBegin` as the `Long.MAX_VALUE, 0`
end. Move the #207 single-source javadoc to the new method and reference it from the old.
Tests in `TimeIntervalTest`: end clamp, fragment entirely past the end dropped, fragment straddling
both edges, gap-only slice yields empty.

### Task 2 — `TimestampMap.removeFrom` and the hash-backed index

**Files:** `common/model/TimestampMap.java`, `common/model/TimestampDataMap.java`,
`common/utility/TabularDataUtility.java`

- `TimestampMap.removeFrom(long seconds, long nanos)`: remove all entries at or after the position;
  return the number removed (used by the trip path and asserted in tests).
- `TimestampDataMap`: add the name→index map per D9.
- `TabularDataUtility.addColumnsToTable()` (`:227-280`): build `columnIndexes` after the registration
  loop at `:238-241`; replace the lookup at `:269`.

### Task 3 — Client: slice-bounded samples retrieval

**Files:** `query/handler/mongo/client/MongoQueryClientInterface.java`,
`query/handler/mongo/client/MongoSyncQueryClient.java`

- `executeQuerySamplesV2(ResolvedQuery, long beginSecs, long beginNanos, long endSecs, long endNanos)`;
  `bucketSamplesQueryV2` takes the clamped list from `clampToWindow`. The package-private builder keeps
  its role for the plan-shape test.
- `resolveSampleStatusTimestamps(ResolvedQuery, long beginSecs, long beginNanos, long endSecs, long endNanos)`
  clamps the same way (D5).
- Update the fake clients: `QuerySamplesDispatcherNullCursorTest.NullCursorClient`, and the
  `bucketSamplesQueryV2` call in `MongoBucketQueryPlanTest` (signature only; #275 extends that test
  afterwards).

### Task 4 — `AbstractQuerySamplesDispatcher`: the slicer

**File:** `query/handler/mongo/dispatch/AbstractQuerySamplesDispatcher.java`

- Constructor gains `long initialSliceNanos`; `MongoQueryHandler` reads
  `QueryHandler.queryV2SamplesInitialSliceSeconds` and passes it; the existing test constructors
  gain the parameter (tests choose small slices to force multi-slice pages on the 3 s fixture).
- `retentionIntervals(resolvedQuery, begin, end)` built from `clampToWindow`.
- `statusRetentionFilter(...)` per slice (D5).
- `SliceDrain` (package-private): fields `resolvedQuery`, `mongoClient`, `tableValueMap`,
  `byteBudget`, `sliceNanos`, `windowEnd` (= last fragment end), `cursorSecs/Nanos` (next slice
  begin), `dataSize`. Method `Outcome drainNext()` returning one of `ACCEPTED(rowsInSlice)`,
  `BUDGET_TRIP_PAGE_END(resumeAt)`, `EXHAUSTED`, `OVERSIZED(timestamp)`, with a `retryOnTrip` flag
  (stream mode) that halves instead of ending. Null cursor from the client stays an error, as today.
- Slice sizing per D2; gap skipping per D4.

### Task 5 — `QuerySamplesUnaryDispatcher`

**File:** `query/handler/mongo/dispatch/QuerySamplesUnaryDispatcher.java`

Replace the single `executeQuerySamplesV2` call and `addBucketsToTable` (`:86-117`) with the slicer
loop: accept slices until `collectTimestamps(map).size() >= pageSize`, budget-trip page end, or
exhaustion. `emitPage()` keeps the count-driven truncation; the byte-driven branch (`:151-171`) is
replaced by "token = resumeAt from the trip". Update the class javadoc (drain-then-truncate paragraph).

### Task 6 — `QuerySamplesStreamDispatcher`

**File:** `query/handler/mongo/dispatch/QuerySamplesStreamDispatcher.java`

Slicer loop in stream mode; after each accepted slice, run the existing chunk loop (`:303-358`) over
the accumulated rows and clear the map; `onCompleted` after exhaustion. Delete the memory note in the
class javadoc.

### Task 7 — `OutboundReadinessGate` and wiring

**Files:** new `common/grpc/OutboundReadinessGate.java`;
`query/handler/mongo/dispatch/QueryDataStreamDispatcher.java`,
`QueryBucketsStreamDispatcher.java`, `QuerySamplesStreamDispatcher.java`;
`query/handler/mongo/MongoQueryHandler.java` (timeout config)

Per D8. The gate is a constructor-time field; `awaitReady()` precedes
`QueryServiceImpl.sendQueryDataResponse(...)` in `QueryDataStreamDispatcher.handleResult_()` (both the
intermediate and residual sends), `emitChunk()` in `QueryBucketsStreamDispatcher`, and `emit()` in
`QuerySamplesStreamDispatcher`.

### Task 8 — `QueryDataDispatcher` log

`QueryDataDispatcher.java:52` per D10.

### Task 9 — Configuration

Both `application.yml` files, `QueryHandler` block: `queryV2SamplesInitialSliceSeconds` (60) and
`streamReadyTimeoutSeconds` (300), with comments; `MongoQueryHandler` constants and getters.

### Task 10 — Tests

- `MongoSyncQuerySamplesV2Test`:
  - `testByteBudgetPagingCompletesEveryPv`: the two-PV repro from Background §1 (budget 40, paged to
    completion, 30 and 15 values respectively, no all-unset column on any page).
  - `testStreamByteBudgetCompletesEveryPv`: stream analog.
  - `testSmallSliceMultiSlicePageIsCompleteAndOrdered`: initial slice 1 s over the 3 s two-PV fixture,
    page size 10,000 → one page, 30 rows, both columns complete, no token.
  - `testSliceGrowsProportionally` (package-private slicer driven directly): 1 Hz fixture, initial
    1 s, pageSize 20 → slices 1 s, 16 s, done; assert the find count through a counting client.
  - `testBudgetTripEndsPageAtSliceBegin`: trip on the second slice → page holds slice 1 only, token =
    slice 2 begin, next page resumes there with no gap or duplicate.
  - `testGapOnlySliceSkipsDatabaseCall` on the #207 `spanpv` fixture with two fragments.
  - `testStatusFilterAppliesPerSlice` on the `sspv` fixture with a 2 s initial slice.
  - Existing `testByteBudgetPagingSeamNoGapNoOverlap`, the oversized-row tests, and the stream chunk
    test must still pass unchanged (any correct paging satisfies them).
- `QuerySamplesDispatcherNullCursorTest`: adapt the fake client; the classification pinned there is
  unchanged.
- `TimeIntervalTest`, `TabularDataUtilityTest` (a `removeFrom` test on `TimestampMap`; the column
  order tests as-is).
- New `OutboundReadinessGateTest`: a minimal `ServerCallStreamObserver` subclass whose `isReady()` is
  flipped from another thread; assert `awaitReady()` blocks and then returns true, returns false on
  `isCancelled()`, returns false after the timeout, and is a no-op for a plain `StreamObserver`.
- `MongoSyncQueryBucketsV2Test.runStream` with a not-ready-then-ready observer: all chunks arrive, and
  no `onNext` was observed while `isReady()` was false.

### Task 12 — Span-class partition

**Files:** `query/handler/mongo/client/MongoSyncQueryClient.java`, `MongoQueryClientInterface.java`,
new `query/handler/mongo/client/MergedBucketCursor.java`, new `SpanClass` record (nested in the
client or its own file)

- `resolveSpanClasses()` per D11, beside `resolveMaxBucketSpanSeconds()` (which the pattern path and
  the plan test keep using). The negative-span clamp and `warn` at `:96-113` apply per document.
- `spanClassFinds(Bson perClassNameFilter, ...)`: a package-private builder returning
  `List<FindIterable<BucketDocument>>`, one per class, for `MongoBucketQueryPlanTest` to `explain()`
  individually (#275 Task 1).
- Callers converted: `executeDataBlockQuery` (`:219-240`), `executeQueryData` (`:243-267`),
  `executeQueryTable` `PVNAMELIST` arm (`:285-289`), `executeQueryBucketsV2` (`:549-573`),
  `executeQueryBucketsV2Stream` (`:596-621`), `executeQuerySamplesV2` (`:624-659`, which under Task 3
  is per slice). Each opens all class cursors inside its existing `try`/`catch`, closing any already
  opened on failure, and returns the merged cursor or the single cursor.
- `MergedBucketCursor`: `hasNext`/`next`/`close` over the head comparison; `tryNext`, `available`,
  `getServerCursor`, `getServerAddress` delegate to the first cursor or return conservative values.
  `close()` closes every underlying cursor.
- Tests:
  - `SpanClassTest` (no database): class assignment at the boundaries (0, 1, 2, 3, 4, 5 s → classes
    0, 0, 1, 2, 2, 3), per-class max is the actual max, missing PV → class 0, single class →
    single-find path.
  - `MergedBucketCursorTest` (no database, list-backed fake cursors): interleaved PV names across
    three cursors come out in `(pvName, firstTime)` order; empty cursors; `close()` closes all.
  - `MongoSyncQueryClientMissingIndexTest`: add a mixed-span request so the multi-class open path is
    pinned to the null-cursor classification too.
  - `MongoBucketQueryPlanTest`: the mixed-span cases live in #275 Task 1 (they assert each class find's
    own bound); this ticket only adapts the existing calls.

### Task 11 — Documentation

- `CLAUDE.md`, "Per-PV Bucket Span Bound": replace the "maximum over the request's PVs" sentences
  with the span-class partition (D11): the bound is per class, the finds are merged in sort order,
  the single-class path is byte-identical, and the pattern path is the documented exception.
- `CLAUDE.md`, a new subsection under "Query API V2" / after "querySamples Fragment Clamp Invariant":
  samples retrieval is time-sliced because the cursor is PV-major and a byte-bounded partial drain
  has no complete timestamp; every slice is all-PVs-or-nothing; `clampToWindow` is the #207 single
  source; the stream path is bounded by one slice; the readiness gate trades heap for worker
  occupancy. Record the §6 measurement (residual on FETCH) in the "Per-PV Bucket Span Bound" list.
- `doc/release-notes/rel-1.16.0.md`: a "span-class partition" paragraph under the #232 section (query
  cost for a short-span PV no longer depends on the longest-span PV named alongside it), and a
  "querySamples paging fix" section (behavior: pages are now
  complete across PVs; a page may hold fewer than `limit` rows when the budget ends it; the resume
  token is the first undrained timestamp) and a note on the two new config keys.
- `QueryClient.java:805-809` and `:1354-1361`: replace the "drains buckets until the byte budget
  trips" wording with the slice semantics; a small `limit` now does save work.
- `plan/tickets/244/plan.md` D7 is historical and is left as written.

## Out of scope

- The covered pre-scan alternative for the span cost (D11, rejected alternatives): only if the
  customer's `pvStats` distribution shows wide spans *within* classes.
- The V1 `queryTable` pattern path keeps a single find with the pattern's maximum span (D11).
- Carrying the converged slice length in the page token (would make page 2+ a single find in the
  common case). Follow-on if the benchmark in #275 shows the two-to-three finds per page matter.
- Flow control on `queryDataBidiStream` (client-paced) and on the annotation `exportData` stream.
- Paging for `queryTable` (it errors at the budget, which is not a wrong answer).
- The deep-history plan-shape test and benchmark work: #275.

## Dependencies and sequencing

- **PR 1**: Tasks 1–6, 8–12 (samples correctness, amplification, stream bound, hotspot, log,
  span-class partition, docs). This is the release-blocking part. Within it, Task 12 lands before
  Task 3–6 so the slicer calls the partitioned client from the start.
- **PR 2**: Task 7 and its tests (flow control). Independent of PR 1 except for the shared
  `QuerySamplesStreamDispatcher` file; rebase.
- **#275 follows PR 1**: it extends `MongoBucketQueryPlanTest`, whose `bucketSamplesQueryV2` call
  changes signature in Task 3 and whose mixed-span cases explain the per-class finds from Task 12,
  and its `querySamples` benchmarks should measure the sliced path.
- No dp-grpc change: the proto, token `Kind`s, and wire status values are untouched.
