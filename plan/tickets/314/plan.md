# dp-service #314: Query service stops executing jobs after OutOfMemoryError

## Overview

A customer evaluating `rel-1.16.0` ran a few concurrent long-range `querySamplesStream` requests.
The query service ran out of heap, every worker thread that saw the error died, and the service
then accepted and queued requests for 33 hours without executing any of them.

This ticket delivers two things, in two PRs:

1. **Availability (PR 1).** A job that throws cannot cost a handler its worker, and a JVM that can
   no longer be trusted exits so its supervisor restarts it. This applies to all four services.
2. **Bounded memory by design (PR 2).** The samples paths (`querySamples`, `querySamplesStream`)
   stop guessing a time slice and buffering it in a pivot map. They read buckets once, in time
   order, and merge them into rows as they arrive.

PR 2 is in this ticket rather than a follow-on because PR 1 alone does not make the customer's
workload usable (finding 6).

## Background: triage findings

All line references are to `main` at `fd806c9`. Items marked *measured* were run during triage;
the reproduction tests are on this branch.

### 1. A job that throws an `Error` ends its worker for good — confirmed

`QueueHandlerBase.executeJob()` catches only `Exception` (`QueueHandlerBase.java:110`), and
`QueryV2Job.execute()` only `RuntimeException` (`QueryV2Job.java:54`). An `Error` therefore leaves
`QueueWorker.run()`. The workers are long-running `Runnable`s submitted once
(`QueueHandlerBase.java:172-177`); the pool does start a replacement thread, but it waits on the
pool's own empty task queue, not on `requestQueue`, so it never runs a job.

*Measured:* `QueueHandlerBaseWorkerSurvivalTest.testWorkerSurvivesAnError` fails on `main` with the
incident's `Exception in thread "pool-N-thread-1"` signature; the `RuntimeException` twin passes.

Nothing exits the JVM or fails a probe. The image runs a bare `java -jar` (`Dockerfile:64`) with no
heap or OOM flags, and the reporter ran without `-XX:+ExitOnOutOfMemoryError` at the time of the
incident; they added it afterwards. That is consistent with the evidence: with the flag, the first
JVM-raised OOM would have exited the process, and neither the one-by-one worker loss nor the
shared-instance self-suppression in finding 2 could have occurred.

### 2. "Self-suppression not permitted" hides the real cause — confirmed

`SliceDrain.retrieveSlice()` closes its cursor with try-with-resources
(`AbstractQuerySamplesDispatcher.java:414`). When `close()` rethrows the JVM's pre-allocated
`OutOfMemoryError` instance, `addSuppressed` is asked to suppress an exception with itself and
throws `IllegalArgumentException`, which is what reached the error response.

### 3. Any `CommandListener` makes the driver copy every reply — confirmed

`MongoSyncClient.java:67` registers `DpMongoCommandListener`. In driver 5.4.0,
`LoggingCommandEventSender.sendSucceededEvent` calls
`ResponseBuffers.getResponseDocument(..., RawBsonDocumentCodec)` whenever a listener is present
(read from the bytecode), and `bucketFind()` sets no `batchSize`
(`MongoSyncQueryClient.java:336`).

*Measured:* reading 80 MB of documents allocated 161 MB without the listener and 321 MB with it.
The copy is transient: one reply-sized allocation per in-flight command.

### 4. The trigger is slice growth, not a leading gap specifically

The ticket attributes the heap use to the page budget counting wire bytes. That is correct but is
not sufficient: with `pageSize=100` at a steady rate the stream emits after every slice and holds
about one page of rows.

What unbounds it is `growSlice()` (`AbstractQuerySamplesDispatcher.java:473`). After a slice of
`r` rows the length is multiplied by `clamp(pageSize / max(r, 1), 1, 16)`, and it shrinks only by
halving after a budget trip. A stretch with few or no samples therefore inflates the slice, and the
dense data after it is read at the inflated length, up to the full byte budget.

*Measured* (`MongoSyncQuerySamplesV2Test`, 200 buckets, 20,000 samples at 1 Hz, `pageSize=100`,
window starting two years before the data):

| Byte budget | Buckets read before first message | Total buckets read | Retrievals |
|---|---|---|---|
| 4,096,000 (production) | 200 (whole fixture) | 200 | 6 |
| 40,000 | 2,261 | 2,454 | 70 |

The reporter has since said the PVs have data from before the range start (Archiver import), so
their case was not an empty *leading* stretch. Any quiet stretch inside the range has the same
effect. Whether their PVs are change-driven is an open question on the issue (see Open questions).

### 5. Heap per buffered row is 24–34 times the wire size — measured

The byte budget is the gRPC wire limit, read from `GrpcServer.incomingMessageSizeLimitBytes`
(`MongoQueryHandler.java:30`), and is compared with `DataValue.getSerializedSize()`. The rows sit in
`TreeMap<Long, TreeMap<Long, TreeMap<Integer, DataValue>>>`.

*Measured:* one full budget of double samples (455,000 rows) retains 139 MB at 1 Hz and 98 MB at
1 kHz: 216–305 bytes per row. Seven workers at a full budget is 0.7–1.0 GB.

### 6. Capping the buffer makes the service safe but not usable

In steady state the slice settles at 50–100 rows when `pageSize=100`, so the stream issues about one
`find` per page of rows, and each `find` re-decodes every bucket it overlaps. A two-year stream of
millions of samples is tens of thousands of queries. The incident log shows `queueMs` of 22–42 s
with all seven workers saturated before the crash, against a client deadline of about two minutes.

A row cap would force every request back to that regime. This is arithmetic from the code and the
incident log, not a measurement on the customer's data; PR 2 carries the measurement.

### 7. A time-ordered bucket stream is available from the shipped index — measured

Sorting by `(firstTime.seconds, firstTime.nanos)` instead of `(pvName, firstTime)`, with the
production overlap filter and the `BUCKET_QUERY_INDEX_KEYS` hint, MongoDB 8.0 explodes the
`pvName $in` into one index scan per PV and merges them:

| Deployment | PVs | Plan | Blocking sort |
|---|---|---|---|
| Unsharded | 3 | `FETCH < SORT_MERGE < IXSCAN ×3` | No |
| Unsharded | 100 | `SORT_MERGE` | No |
| Unsharded | 101+ | `SORT` | Yes |
| Two shards, hashed `pvName` key | 12 | `SHARD_MERGE_SORT` over per-shard `SORT_MERGE` | No |
| Two shards, ranged `(pvName, firstTime.seconds)` key, each PV split across shards | 12 | same | No |

Keys examined equalled documents returned in every streaming case. The threshold of 100 follows from
`internalQueryMaxScansToExplode` (200) and the overlap filter contributing two scans per PV. Adding
`pvName` as a trailing sort key defeats the plan (blocking `SORT`), so the tie-break must be done in
the service.

Not yet checked: the same plan with a multi-fragment `$or` (configuration selector). PR 2's first
task is the plan-shape test that pins it.

### 8. Cancellation is noticed only at emit — confirmed

`isCancelled()` is consulted only inside `OutboundReadinessGate.awaitReady()`, called from
`emitChunk`. The slice drain reads and decodes in between.

## Design decisions

### D1. Workers catch `Throwable`; a fatal `Error` exits the process

The worker loop (`QueueWorker.run()`) catches `Throwable`, around the queue poll as well as
`executeJob()`. For anything except a fatal error the worker logs and takes the next job, as it does for exceptions today. A `VirtualMachineError` other than `StackOverflowError`
is fatal: the handler logs it and calls `Runtime.halt(3)`, the status HotSpot uses for
`-XX:+ExitOnOutOfMemoryError` (checked on the JDK 21 runtime), so a supervisor sees one value for
either mechanism. The halt is in a `finally` after the log call, so a second OOM while logging
cannot prevent it.

`QueueHandlerBase` is shared, so this applies to ingestion and annotation as well as query (agreed
2026-11-19). For ingestion an exit loses requests acked on enqueue and not yet written, as the JVM
flag in D2 or any other crash would; a service that keeps acking after an OOM is worse.

- *Why exit:* the incident's OOM also killed the Mongo driver's cluster-monitor thread. A JVM that
  has thrown `OutOfMemoryError` on arbitrary threads cannot be assumed to be working, and the
  supervisor restart is seconds. This is the #254 rule: a service that cannot do its job must not
  appear to be serving.
- *Why `halt`, not `System.exit`:* `exit` runs the shutdown hooks, and `QueueHandlerBase.fini()`
  waits for the workers, one of which is the caller. `-XX:+ExitOnOutOfMemoryError` skips hooks too.
- *Why spare `StackOverflowError`:* it is local to the job's thread and the stack is unwound by the
  time it is caught.
- *Rejected:* keep serving after OOM. It turns a restart into an undefined state.

The exit is behind an overridable hook so the test can observe it without ending the test JVM.

### D2. The image sets `-XX:+ExitOnOutOfMemoryError` through `JAVA_TOOL_OPTIONS`

D1 covers only an OOM thrown inside a job. One thrown on a gRPC or driver thread needs the JVM flag.
`ENV JAVA_TOOL_OPTIONS` rather than the `ENTRYPOINT`, because operators start the other three
services by overriding the command with `java -cp`. An operator who sets the variable replaces the
default; the runbook says so.

### D3. The samples paths read buckets once, in time order, and merge

`SliceDrain`, the adaptive slice length, and the pivot map leave the samples paths. In their place:

- **Retrieval.** One `find` per (span class, PV group), sorted `(firstTime.seconds, firstTime.nanos)`
  over the whole remaining window of every fragment, merged in the service into one time-ordered
  bucket stream. A PV group holds at most 50 PVs — half the measured threshold, since the limit is a
  server parameter and a planner detail.
- **Merge.** Each bucket read becomes an *open bucket*: its decoded column plus a position, advanced
  past samples before the window begin. A min-heap over the open buckets yields samples in timestamp
  order. The **watermark** is the `firstTime` of the next unread bucket. Every sample strictly before
  the watermark is known to be complete across PVs, because no unread bucket can hold an earlier one.
  Rows are emitted up to the watermark; then the next bucket is read.
- **Output.** Rows go straight into the outgoing `ColumnTable` builders. The byte budget and the row
  limit are checked per row, against what is actually being built, so nothing is discarded or
  re-read. The indivisible-oversized-row error is unchanged.
- **Unary paging.** The same merge, stopped at `pageSize` rows or the budget; the token is the next
  row's timestamp.

Memory is the open buckets (those overlapping the current position, typically one per PV), one
cursor batch per group, and one outgoing chunk.

*Rejected:* patching the slice sizing (proportional shrink, a probe for the next non-empty time).
It narrows the bad cases but keeps the guess, the discard, and the per-slice re-query.
*Rejected:* one cursor per PV. Ten thousand resolved PVs would be ten thousand cursors.

### D4. Invariants the merge must carry over

- **#207 fragment trim.** Retention is per sample against the resolved fragments via
  `TimeInterval.clampToWindowBegin()`, the same source the database filter uses. No collapsed
  window.
- **Column registration.** Every resolved PV gets a column, all-unset where it has no samples.
- **#232 / #274 span bound.** Each find carries its span class's bound, resolved once per request
  through the existing `SpanClassHolder`; nothing is cached across requests.
- **Failure classification.** Every cursor open returns the null cursor on a driver error, and a
  mid-iteration failure is wrapped as `DpException`, so neither can hang the caller's stream.
- **Duplicate samples.** Two buckets of one PV holding the same timestamp resolve as today: the
  bucket later in `(firstTime, lastTime, _id)` order wins.
- **Status selector.** Matching timestamps are resolved in bounded windows as the merge position
  advances, replacing the per-slice resolution. A failed read stays an error.

### D5. Bucket cursors set a batch size; the command listener stays

With several cursors open per request, the driver default (101 documents, then up to 16 MiB per
batch) is too much per cursor. `bucketFind()` sets `batchSize`, sized so a request's open batches
stay within a fixed document count. That also shrinks the listener's reply copy (finding 3).

The listener stays: it is the only source of `db.client.operation.duration` (#212), and after D3 its
cost is transient garbage of one small batch. *Fallback, not planned:* a second listener-free
`MongoClient` for bucket reads, which `TimedMongoCursor` already times.

### D6. Cancellation is checked per bucket

The merge loop asks the readiness gate whether the call is cancelled before reading each bucket, and
abandons through the existing `markAbandoned()` path.

### D7. The cursor is closed in a guarded `finally`

Replaces the try-with-resources (finding 2). A `RuntimeException` from `close()` is logged and
dropped, so it cannot replace the throwable in flight. An `Error` from `close()` propagates: it is
either the same pre-allocated instance already in flight or a fatal error that should supersede it,
and with no `addSuppressed` call there is nothing to fail. Applies to the new merge loop.

## Implementation tasks

### PR 1 — availability

- `common/handler/QueueHandlerBase.java`: catch `Throwable` in `executeJob`; classify fatal errors;
  add the overridable fatal-error hook that halts (D1). Update the class javadoc.
- `Dockerfile`: `ENV JAVA_TOOL_OPTIONS="-XX:+ExitOnOutOfMemoryError"` (D2).
- `AbstractQuerySamplesDispatcher.java`: guarded close in `retrieveSlice` (D7), since PR 1 ships
  before the slice code is removed.
- `QueueHandlerBaseWorkerSurvivalTest`: already on the branch; add a case asserting the fatal hook
  fires for `OutOfMemoryError` and does not for `StackOverflowError`.
- `MongoSyncQuerySamplesV2Test`: a cursor that throws one `OutOfMemoryError` instance from `next()`
  and again from `close()` must surface that instance, not "Self-suppression not permitted". The
  two leading-gap tests written during triage fail until PR 2, so they are kept on the
  `issue-314-merge` branch and are not part of PR 1.
- `doc/running.md`: operator section on the exit behaviour and `JAVA_TOOL_OPTIONS`, written for
  both image and non-image launches; `NEXT.md` section.
- `CLAUDE.md`: the worker-survival invariant.

### PR 2 — time-ordered merge

1. `MongoBucketQueryPlanTest`: pin the time-ordered plan (no `SORT` stage, one `IXSCAN` per PV) for
   a single fragment, a multi-fragment `$or`, and a mixed span-class request, at the group size.
   **This gates the rest:** if the multi-fragment case does not stream, D3's retrieval changes.
2. `MongoSyncQueryClient.java`: time-ordered samples query builder and cursor open, per span class
   and PV group, with `batchSize` (D5); a time-ordered merging cursor beside `MergedBucketCursor`.
   `MongoQueryClientInterface`: the new method, replacing the six-argument slice overload.
3. New merge class under `query/handler/mongo/dispatch/`: open buckets, heap, watermark, retention,
   status windows (D3, D4).
4. `QuerySamplesStreamDispatcher`, `QuerySamplesUnaryDispatcher`, `AbstractQuerySamplesDispatcher`:
   drive the merge; remove `SliceDrain`, `SliceOutcome`, `growSlice`, the seeded pivot map.
5. Remove `QueryHandler.queryV2SamplesInitialSliceSeconds` from both `application.yml` files and the
   docs.
6. Tests: the two leading-gap tests on the branch become acceptance tests (re-expressed as "each
   bucket is read once" and "first message after a bounded read"); port the existing
   `MongoSyncQuerySamplesV2Test` cases, replacing the slice-count assertions; add quiet-then-dense,
   overlapping-bucket, wide-bucket-beside-narrow, more-than-one-PV-group, and cancel-mid-stream
   cases; `MongoSyncQueryClientMissingIndexTest` for the new open path.
7. Measurement: a `QueryBenchmark` scenario shaped like the incident (one PV, long range, tens of
   thousands of buckets, several concurrent streams, `-Xmx512m`), run before and after, with peak
   heap and time to completion recorded in the PR.
8. `CLAUDE.md`: replace "querySamples Retrieval Is Time-Sliced" with the merge invariants;
   `NEXT.md`.

## Out of scope

- **`queryTable` and tabular export** still buffer in the pivot map (`QueryTableDispatcher.java:205`,
  `ExportDataJobAbstractTabular.java:136`). They use the size limit as a hard ceiling, so the heap is
  bounded at about one budget per worker (finding 5), but it is the same multiplier. Epic.
- **Handler admission control and health:** skipping jobs whose client has gone at dequeue,
  `RESOURCE_EXHAUSTED` on saturation instead of blocking gRPC threads on a queue of one, a gRPC
  health service, and an error response for a job whose throwable the worker swallows. Epic.
- **API changes:** packed typed columns in `ColumnTable`, a per-PV series stream, server-side
  reduction for plotting. Epic.
- **Separate outgoing message-size key.** `MongoQueryHandler.java:30` reuses the incoming key. Not a
  cause here. Epic.

The epic is osprey-dcs/data-platform#121, with #314 as a sub-issue. Its other children are
#319 (admission control), #320 (error response for an escaped throwable), #321 (tabular export),
#322 (outgoing size key), and osprey-dcs/dp-grpc#176–#178 (packed columns, per-PV series,
reduction). The gRPC health service is the existing #293, which stays under its own epic.

## Dependencies and sequencing

- PR 1 has no dependencies and merges first. There is one release, after PR 2 (agreed 2026-11-19):
  the reporter has run with `-XX:+ExitOnOutOfMemoryError` since the incident, so PR 1 alone changes
  little for them.
- PR 2 does not depend on PR 1 and does not need a dp-grpc change or a schema migration.
- PR 2 task 1 blocks tasks 2–8.
- Neither PR blocks on the reporter's answers. They affect only the shape of the benchmark fixture
  in task 7.

## Open questions

Asked on the issue, 2026-11-19, unanswered when this was written: whether the PVs are change-driven
with quiet periods, the typical bucket shape, the heap limit at the time of the incident, whether
`pageSize=100` is required, and the client deadline.
