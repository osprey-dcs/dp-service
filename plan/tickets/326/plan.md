# dp-service #326: do not send an error or reject to a streaming client that has cancelled

Follow-on to #324, from the review of PR #325.

## Overview

The three server-streaming query responders (`queryDataStream`, `queryBucketsStream`,
`querySamplesStream`) consult `OutboundReadinessGate` before every data send and, since #324,
before the empty-result send. Their error and reject sends did not: each marked the outcome,
called `onNext()` with the exceptional response, then `onCompleted()`, whether or not the client
was still there.

This ticket makes those sends check for cancellation first. When the client has cancelled, nothing
is sent and the call is not completed. The outcome is unchanged: the request is still counted as
`error` or `reject`.

For operators and clients nothing observable changes: a cancelled client was never going to read
the message, and the metrics are the same. What the ticket delivers is the rule, applied
uniformly and pinned by tests, that a responder does not send to or complete a call that has no
reader. No proto change, no schema migration, no configuration.

## Background: triage findings

### 1. Nine send sites, not eight

The issue as filed listed eight. The ninth is the null-cursor error in
`QueryDataAbstractDispatcher.handleResult()`, which `queryDataStream` reaches through its base
class and which the unary and bidi `queryData` dispatchers share. The issue has been updated.

As of `8dfa4cf`:

- `AbstractQuerySamplesDispatcher.sendError()` and `sendReject()`, reached on the stream path from
  `sendOversizedRowError()` (`QuerySamplesStreamDispatcher.java:76`) and `sendDrainFailure()`
  (`:108`). Shared with `QuerySamplesUnaryDispatcher`.
- `QueryBucketsStreamDispatcher.java:83` (null cursor), `:115` (bucket build failure), `:138`
  (indivisible oversized bucket).
- `QueryDataStreamDispatcher.java:146` (deserialization failure or oversized bucket).
- `QueryDataAbstractDispatcher.java:62` (null cursor), shared with unary and bidi.

### 2. What gRPC does with a send on a cancelled call

Read from the bytecode of `ServerCalls$ServerCallStreamObserverImpl` in grpc-stub 1.65.1, the
version on this classpath:

- `onNext()` throws `StatusRuntimeException` ("call already cancelled. Use
  ServerCallStreamObserver.setOnCancelHandler() to disable this exception") when the call is
  cancelled, **no cancel handler is registered**, and the call is **server-streaming or bidi**:
  the test is `cancelled && serverStreamingOrBidi`, and `cancelled` is set only when there is no
  handler to run.
- The gate registers a cancel handler (#274), so on the three streaming responders `onNext()` does
  not throw.
- A unary call never throws: `asyncUnaryCall` builds its observer with `serverStreamingOrBidi`
  false. (The first version of this plan said a unary error send throws; the PR #327 review
  corrected it from the bytecode.)
- `queryDataBidiStream` is bidi and registers no cancel handler, so a send on a cancelled bidi
  call does throw, out of the dispatcher. The outcome was already marked, so the metric is right;
  the cost is a stack trace in the log for a client that simply went away.

Not measured against a running server: what the transport does with the message and the
`close(Status.OK)` after a cancel on a call that does have a handler. The tests here use
`FakeServerCallStreamObserver`, which records every `onNext()` regardless.

### 3. The merge already covers most of the samples path

`SampleRowMerger.nextRow()` asks about cancellation at the top of every pass (#314), and a
cancelled merge is `abandoned`. An exceptional send on the samples stream sees a cancelled client
only when the cancel arrives after that check: during the bucket read of the pass that then
fails, or before the merge exists (the null cursor, thrown by `openMerger()`).

## Design decisions

### D1. Check cancellation, do not wait for readiness

The sends check `isCancelled()` and do not call `awaitReady()`.

Rationale: an exceptional response is one small message, so buffering it for a slow reader costs
nothing that flow control exists to prevent. `awaitReady()` would hold a worker for up to
`QueryHandler.streamReadyTimeoutSeconds` (300) to deliver it, and on a timeout a client that is
slow but alive would get no error at all and wait on an open stream.

Rejected: `awaitReady()`, for consistency with the empty-result send (#324). That send is a data
message on the normal path; this one is the only report of a failure.

### D2. The outcome stays `error` or `reject`

A request that failed and then lost its client is counted as `error` (or `reject`), not
`abandoned`. The mark stays ahead of the send, where it was.

Rationale: `abandoned` means the service did nothing wrong and the response is incomplete because
of the client. Here the service did fail, or the client did send something wrong, and that is
true whoever is left to be told. Relabelling would hide real failures behind disconnects, which
are common exactly when a query is failing slowly.

Rejected: mark after the send and `markAbandoned()` on a cancelled client, the #324 order. This
is the deliberate asymmetry with #324: there a refused send of a normal result has no other
classification; here the request already has one.

### D3. One helper per dispatcher family

Each family gets a single send helper that marks, checks, and sends, so a new error site cannot
skip the check:

- samples: the existing `sendError()`/`sendReject()` in `AbstractQuerySamplesDispatcher`;
- buckets stream: a new private `sendError()`;
- `queryData`: a new protected `sendError()` on `QueryDataAbstractDispatcher`.

The samples and `queryData` helpers are shared with dispatchers that have no gate, so they use
the static `OutboundReadinessGate.isCancelled(observer)`, which registers nothing and returns
false for a plain `StreamObserver`. The buckets stream uses its gate.

Consequence, accepted: the unary samples dispatcher, and the null-cursor path of unary and bidi
`queryData`, also skip the send on a cancelled call. On a unary call the skip is a no-op (no throw
to avoid, finding 2); on the bidi null-cursor path it avoids the throw. No behavior a client can
observe changes.

Each helper logs the skipped send at debug, so a log can be read against a client report of
receiving no error.

Rejected: an overridable hook so only the stream dispatchers check. It would add a seam and keep
the bidi null-cursor throw for no benefit.

## Implementation tasks

- `AbstractQuerySamplesDispatcher`: private `clientCancelled()`; `sendError()` and `sendReject()`
  return after marking when it is true.
- `QueryBucketsStreamDispatcher`: private `sendError(String)`; the three sites call it.
- `QueryDataAbstractDispatcher`: protected `sendError(String)`; the null-cursor site calls it.
- `QueryDataStreamDispatcher`: the error branch calls `sendError()`.
- Tests, each asserting no message, no completion, and the outcome, and each verified failing
  with the main-source change reverted:
  - `MongoSyncQuerySamplesV2Test`: `testStreamRejectIsNotSentToACancelledClient` (non-scalar PV)
    and `testStreamErrorIsNotSentToACancelledClient` (oversized row). The test client gains an
    `afterSamplesBucketRead` hook so the cancel can arrive during a read (finding 3).
  - `QuerySamplesDispatcherNullCursorTest`: stream and unary null-cursor cases.
  - `MongoSyncQueryBucketsV2Test`: `testStreamErrorIsNotSentToACancelledClient` (oversized
    bucket). The three sites share the helper; this is the one a fixture produces.
  - `QueryDataStreamDispatcherCancelledErrorTest` (new, no database): null cursor and
    deserialization failure, each with a control whose client is present.
- Docs: `CLAUDE.md` (Outbound Flow Control) and `doc/metrics.md` (`error`/`reject` are counted
  whether or not the client was still there). No `NEXT.md` section: nothing a user or operator
  can observe changes.

## Out of scope

- **Resolution-stage rejects** (`completeRejectedResolution`) and **RPC-layer validation rejects**
  in `QueryServiceImpl`: sent on the gRPC thread before any dispatcher or gate exists, when the
  call has only just arrived. No ticket.
- **The other error sends of the unary and bidi `queryData` dispatchers**
  (`QueryDataDispatcher.java:88`, `QueryDataBidiStreamDispatcher.java:144`): not streaming
  responders in the sense of #274; `queryDataBidiStream` is client-paced and ungated by design.
  The bidi one still throws on a cancelled call (finding 2). No ticket.
- **Normal sends of `queryDataBidiStream`**, which throw on a cancelled call for the same reason.
  No ticket.

## Dependencies and sequencing

Depends on #324 (merged, PR #325) only for the test patterns. Blocks nothing, and nothing blocks
on it.
