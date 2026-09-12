# Plan: QueryClient wrappers for the Query API V2 (issue #244)

- **Ticket**: [osprey-dcs/dp-service#244](https://github.com/osprey-dcs/dp-service/issues/244)
- **Consumer**: [osprey-dcs/dp-desktop-app#39](https://github.com/osprey-dcs/dp-desktop-app/issues/39)
  task 6. `DpApplication` reaches gRPC exclusively through `ApiClient`'s typed clients and never
  touches a stub or channel, so this is a hard architectural blocker for that task, not a
  convention.
- **Companion proto fix**: [osprey-dcs/dp-grpc#149](https://github.com/osprey-dcs/dp-grpc/issues/149)
  — correct the `ConfigurationSelector` empty-criteria comment
  (see [Design decision D1](#d1--the-empty-configurationselector-comment-is-the-defect-not-the-server)).
  **Resolved** by dp-grpc PR #151 (`f753cfd`), which also documents the `pvSelector.metadataQuery`
  match-all asymmetry noted in Background §2.
- **Status**: triaged 2026-09-12 against dp-service `aafb3e3` and dp-grpc `6dfff3f`, by reading the
  resolver, the four V2 dispatchers and the existing client/test layers. **Implemented** 2026-09-12;
  see [Implementation notes](#implementation-notes) for the two places the implementation departed
  from the plan.

## Overview

The Query API V2 methods (`queryBuckets`, `queryBucketsStream`, `querySamples`,
`querySamplesStream`) are implemented server-side and exercised by ITs, but the
`com.ospreydcs.dp.client` convenience layer has **no V2 support at all** — `QueryClient` wraps only
the V1 `queryTable`, `queryPvStats` and `queryProviders`. This ticket adds wrappers for all four V2
methods, with params types that model `QuerySpec`'s full selector surface, and result classes
following the `ApiResultBase` convention.

It delivers, for the first time in this repo, a `src/main` builder for a V2 request. Everything that
exists today is test-only and minimal (see [Background §3](#3-the-only-v2-request-builders-today-are-test-only-and-pvnamelist-only)).

## Background / triage findings

The ticket is AI-generated and reads as a draft. Its premises hold, with one correction already
posted to the issue and three further findings below that change the work.

### 1. Column metadata is inert on the samples path — confirmed

The issue body says:

> `ResultRepresentation`: default to plain `DataColumn`s with column metadata included

This is wrong, and was corrected in the issue's triage comment. Verified independently here:

- `ResolvedQuery.isExcludeColumnMetadata()` has exactly **one** call site repo-wide,
  `AbstractQueryBucketsDispatcher.java:30`. No samples dispatcher reads it.
- The sample column builders only ever call `.setName(...)` (`AbstractQuerySamplesDispatcher.java:157`)
  and `.addDataValues(...)` (`:185`). There is no `setMetadata` call anywhere under
  `service/query`.
- `QuerySamplesUnaryDispatcher.java:42` documents it: *"excludeColumnMetadata (Q8) is inert — the
  tabular path carries no column metadata."*

The flag is therefore **not merely defaulted-on; it does nothing** on the samples path. It *is*
functional on the buckets path (`BucketDocument.java:238,253`), which is one reason the buckets
wrappers are worth having in the same ticket.

`useSerializedColumns`, by contrast, is genuinely honored on both samples paths
(`AbstractQuerySamplesDispatcher.java:192-204`).

### 2. `configurationSelector` and `pvSelector.metadataQuery` treat empty criteria oppositely

Two selectors on the same `QuerySpec`, opposite semantics for an empty criteria list:

| Selector | Empty criteria list | Source |
|---|---|---|
| `configurationSelector` | **REJECT** — `"configurationSelector.criteria list must not be empty"` | `QueryV2Resolver.java:346` |
| `pvSelector.metadataQuery` | **match-all** PV metadata, intersected with archive existence | `MongoSyncQueryClient.java:434-436`; resolver does not reject (`QueryV2Resolver.java:246-254`) |

The `configurationSelector` reject **contradicts the proto**, which says at `query.proto:335-337`:

> If no criteria are supplied, the selector matches nothing and the query returns an empty result
> (omit ConfigurationSelector entirely to query the full TimeRange unconditionally).

The divergence is resolved in favor of the server — see [D1](#d1--the-empty-configurationselector-comment-is-the-defect-not-the-server).
It is load-bearing for this ticket regardless of which way it is settled, because `AnnotationClient`'s
established idiom is *"a null or empty field contributes no criterion"* — applied naively to
`configurationSelector`, that idiom builds a **rejected** request rather than an omitted filter.

### 3. The only V2 request builders today are test-only and pvNameList-only

`QueryTestBase.java:156-196` has three static helpers:

```java
buildV2QuerySpecPvNameList(List<String> pvNames, long beginSecs, long beginNanos, long endSecs, long endNanos)
buildQueryBucketsRequest(QuerySpec, int limit, String pageToken, boolean useSerialized, boolean excludeMetadata)
buildQuerySamplesRequest(QuerySpec, int limit, String pageToken, boolean useSerialized)
```

They live in `src/test`, so they are unreachable from `src/main` and cannot serve the client layer.
Their coverage gaps also show what a real builder must add: only the `pvNameList` arm is supported;
nothing builds `pvNamePattern` or `metadataQuery`; nothing sets `configurationSelector` or
`sampleStatusSelector`; and `buildQuerySamplesRequest` has no `excludeMetadata` parameter at all, so
there is currently no way to send `excludeColumnMetadata=true` on a samples request from the tests.

This ticket does **not** migrate the ITs onto the new builders; see [Out of scope](#out-of-scope).

### 4. `getQueryChannel()` returns a type too narrow for `QueryClient`

`GrpcIntegrationQueryServiceWrapper.java:45` declares `public Channel getQueryChannel()` —
`io.grpc.Channel`, not `ManagedChannel`. `QueryClient`'s constructor (via `ServiceApiClientBase`)
takes `ManagedChannel`. The base class already exposes `getChannel()` returning `ManagedChannel`
(`GrpcIntegrationServiceWrapperBase.java:60-62`), and the annotation wrapper's equivalent
(`GrpcIntegrationAnnotationServiceWrapper.java:107`) already returns `ManagedChannel`.

So the new IT must either call `getChannel()` or the accessor's return type must be widened. Widen it
— the narrow type is an inconsistency with the annotation wrapper, not a deliberate restriction.

### 5. The unary and streaming oversized-row messages are not identical

Both dispatchers error on a single row exceeding the byte budget, but with different text:

- Unary (`QuerySamplesUnaryDispatcher.java:163-168`): `"... exceeds the outgoing message size limit
  (" + byteBudget + " bytes); narrow the PV set or time range"`
- Streaming (`QuerySamplesStreamDispatcher.java:140-148`): includes the measured row size —
  `"... (" + rowBytes + " > " + byteBudget + " bytes); ..."`

A test asserting one string will not match the other. Assert on a common substring, or assert each
separately.

Note the guidance in both messages is only half right for a consumer: narrowing the **time range
cannot help**, because the row is a single instant. Only reducing the PV count does. The wrapper
javadoc should say so rather than echoing the server's phrasing.

## Design decisions

### D1 — The empty-`configurationSelector` comment is the defect, not the server

**Decision**: leave `QueryV2Resolver` unchanged; correct `query.proto:335-337` in dp-grpc to document
the reject. The wrapper omits an all-empty `configurationSelector` entirely rather than emitting one.

**Rationale**: three semantics are defensible for an empty criteria list, and the proto picked the
one with the worst failure mode.

- **Reject** (what the server does) makes a mis-built selector loudly distinguishable from a
  well-formed selector that matched no activations. The resolver documents exactly this at
  `QueryV2Resolver.java:322-331`, mirroring V1 `queryProviders`, and it is pinned by
  `QueryV2ResolverTest.testConfigSelectorEmptyCriteriaListRejected`.
- **Match nothing** (what the proto says) means an unfilled criteria list silently returns zero
  rows — the same class of silent wrong answer the `nonBlank()` guard exists to prevent (#243),
  merely inverted: #243 guards against a blank filter silently returning *everything*, this would
  have a blank filter silently return *nothing*.
- **Match-all** would follow #245's invariant for the annotation queries, but means an unfilled
  selector silently *widens* the query, and would require changing both proto and server.

Rejected alternatives: implementing "match nothing" per the proto as written, and adopting #245's
match-all. Both change working, tested server behavior in order to honor a comment that was never
implemented; neither has a consumer asking for it.

**Consequence for the wrapper**: the "no criterion" idiom is safe here *only because* the builder
drops the whole selector when it has no usable criteria. Emitting an empty `ConfigurationSelector`
message would turn an omitted filter into a rejected request. This is the same reasoning
`buildQueryPvMetadataRequest` records at `AnnotationClient.java:1959-1961`, and it must be carried in
a comment at the config-selector build site.

> **Superseded in part by R1 (see Review revisions).** This paragraph is correct for a caller who
> supplied *no* criteria, and that case still drops the selector. It is wrong for a caller who
> supplied criteria that all turned out blank: dropping there widens the query to the whole time
> range rather than omitting a filter. Those two cases are now distinguished.

### D2 — `PvSelector` is modeled as a sealed interface, not a flat record

**Decision**:

```java
public sealed interface PvSelectorParams
        permits PvNameListSelector, PvNamePatternSelector, PvMetadataSelector {}

public record PvNameListSelector(List<String> pvNames) implements PvSelectorParams {}
public record PvNamePatternSelector(String pattern) implements PvSelectorParams {}
public record PvMetadataSelector(
        TextMatch pvName,
        TextMatch aliases,
        List<String> tagsAnyOf,
        List<AttributeCriterion> attributes) implements PvSelectorParams {}
```

**Rationale**: `PvSelector` is a strict proto oneof — the resolver rejects `SELECTOR_NOT_SET`
(`QueryV2Resolver.java:107`) and, by construction, only one arm can be set. A flat record with three
nullable fields makes an invalid combination representable, and the existing precedent for resolving
it is `buildQueryTableRequest` (`QueryClient.java:162-172`), which silently prefers `pvNameList` over
`pvNamePattern` when both are set. Silent-preference is the wrong default for a strict oneof: a
caller that populates two arms gets a query it did not ask for, with no diagnostic.

A sealed interface makes the wrong combination fail to compile. This departs from `AnnotationClient`'s
flat-record idiom, but that idiom models criteria which genuinely *are* all-optional and ANDed; this
is a mutually exclusive choice, which is a different shape.

Rejected alternatives: flat record with first-set-wins (reproduces the `buildQueryTableRequest`
hazard); flat record throwing `IllegalArgumentException` on ambiguity (a runtime failure where a
compile-time one is available).

**Note**: `PvMetadataSelector` reuses `AnnotationClient.TextMatch` and `AnnotationClient.AttributeCriterion`
rather than redeclaring them. The proto types are distinct
(`PvSelector.MetadataQuery.Criterion.PvNameCriterion` vs
`QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion`) and deliberately so — the proto
comment at `query.proto:264-268` says the duplication is intentional so each stays self-documenting.
But the *client-side* value types are structurally and semantically identical, and duplicating them
would give callers two `TextMatch` types to keep straight. See [D3](#d3--textmatch-attributecriterion-and-nonblank-move-to-a-shared-location).

### D3 — `TextMatch`, `AttributeCriterion` and `nonBlank()` move to a shared location

**Decision**: promote `TextMatch`, `AttributeCriterion`, `nonBlank()` and `isBlankKey()` out of
`AnnotationClient` into a shared home usable by both clients — a new
`com.ospreydcs.dp.client.criteria` package (or `ClientCriteria` utility class; either is acceptable,
decide at implementation). `AnnotationClient` keeps type aliases or re-exports so its ~2,000 lines of
existing call sites are untouched in this ticket.

**Rationale**: `nonBlank()` is the single source for the #243 invariant — *"a blank string in a prefix
or contains criterion is a silent match-all, not a no-op"* — and that invariant applies verbatim to
`PvMetadataSelector`, which resolves through the same `MongoQueryFilterBuilder.nameMatchFilter()`
(`QueryV2Resolver.java:290-300`). Copying the helper into `QueryClient` would create exactly the drift
a shared helper exists to prevent, and CLAUDE.md states the guard's whole point is that there is
"deliberately no weaker helper to reach for."

Rejected alternative: duplicating `nonBlank()` into `QueryClient`. It is four lines, which is what
makes the duplication tempting and what makes the drift invisible when one copy is fixed.

**Constraint**: the promotion must not change `AnnotationClient`'s public API shape, or it becomes a
breaking change for dp-desktop-app, which consumes `AnnotationClient.TextMatch` today.

### D4 — Four wrappers, phased, in one PR

**Decision**: one ticket, one PR, four ordered phases:

1. Shared params types (`PvSelectorParams` hierarchy, `QuerySpecParams`, D3's promotion) and the
   shared `QuerySpec` builder.
2. `querySamples` (unary) — params, result, observer, wrapper.
3. `querySamplesStream` — stream observer, accumulating result.
4. `queryBuckets` + `queryBucketsStream` — bucket result class and observers.

**Rationale**: all four share the `QuerySpec` builder, which is the bulk of the work; splitting them
across tickets means either duplicating it or landing a shared type with one consumer. Phasing keeps
each commit independently reviewable. The consumer (dp-desktop-app#39 task 6) needs only phase 2, but
is unblocked by the whole PR landing at once either way.

Rejected alternative: shipping samples in #244 and filing a separate buckets ticket. It would unblock
the consumer marginally sooner at the cost of a second round of review on shared types.

### D5 — The streaming wrappers accumulate, and their `nextPageToken` is always empty

**Decision**: `querySamplesStream` and `queryBucketsStream` return the same result types as their
unary counterparts, with the stream's messages accumulated into one payload and `nextPageToken`
fixed to `""`.

**Rationale**: this is exactly the `sendQuerySampleStatusesStream` precedent
(`AnnotationClient.java:2912-2940`), whose comment reads *"streaming is fire-and-consume: the buckets
are the accumulated result of the whole stream and there is no continuation token."* V2 streaming has
the identical contract — the resolver rejects a `pageToken` on a streaming call
(`QueryV2Resolver.java:114`) and the dispatchers emit an empty token on every message.

**Consequence**: the params type is shared between unary and streaming, but `pageToken` must be
**dropped, not forwarded**, when building a streaming request — forwarding a non-empty token produces
a guaranteed reject. This is a footgun the params type cannot prevent (the field is legitimately
present for the unary call), so the streaming builder drops it and the javadoc says so.

### D6 — Samples and buckets get separate result classes

**Decision**: `QuerySamplesApiResult` (carrying `ColumnTable` + `nextPageToken`) and
`QueryBucketsApiResult` (carrying `List<DataBucket>` + `nextPageToken`). Both extend `ApiResultBase`
with the three-constructor shape (`(boolean,String)`, `(boolean,String,ApiResultStatus)`,
`(payload, nextPageToken)`), `nextPageToken` null-coalesced to `""`.

**Rationale**: the payloads are unrelated types; a single result class would carry two mutually
exclusive nullable fields. `QuerySampleStatusesApiResult` is the shape to copy.

**On accumulation across streamed messages**: a streamed samples response delivers successive
`ColumnTable` pages, each with its own `timestampList`. Accumulating them into *one* `ColumnTable`
requires concatenating the timestamp axis and appending to each column by name — and every page
carries a column for every resolved PV (Q9 seeds all columns), so the column set is stable across
pages. The accumulation must nonetheless merge **by column name**, not by index, and must reject a
page whose column set differs, rather than silently mis-aligning values against timestamps. That is
the one piece of genuine logic in phase 3 and it needs its own unit test.

### D7 — The javadoc carries the behaviors the proto does not

The params and wrapper javadoc must state, because none of these is discoverable from the proto and
each has already cost a consumer design time:

- **Column metadata is never returned by `querySamples`/`querySamplesStream`** — not defaulted-on,
  absent. Use `queryBuckets` or `queryPvMetadata`. (`excludeColumnMetadata` is accepted on the samples
  params only for symmetry, and is documented as inert.)
- **`limit` counts timestamps/rows, not buckets.** Unset/0 → `queryV2DefaultPageSize` (10,000);
  over-max → **silently clamped** to `queryV2MaxPageSize` (100,000), never rejected.
- **A page is bounded by `min(limit rows, byte budget)`.** The byte budget is
  `GrpcServer.incomingMessageSizeLimitBytes` (4,096,000 default).
- **A small `limit` costs server work without saving any** — the Mongo retrieval is not `.limit()`-ed
  (`MongoSyncQueryClient.java:622-626`); the server drains buckets until the byte budget trips, then
  truncates. Callers should generally leave `limit` unset.
- **The byte budget can overshoot by up to one bucket** — it is measured on `DataValue.getSerializedSize()`
  only (`TabularDataUtility.java:272`), excluding the timestamp list, per-column framing and names, and
  the response envelope. Recommend raising the client's `maxInboundMessageSize`.
- **A single oversized row is a hard ERROR, not a page**, and **narrowing the time range cannot help** —
  only reducing the PV count does (see [Background §5](#5-the-unary-and-streaming-oversized-row-messages-are-not-identical)).
- **Page tokens encode a position only** — URL-safe Base64 of `"1|S|<seconds>|<nanos>"` for samples,
  and `"1|B|<len>|<pvName>|<seconds>|<nanos>"` for buckets (`PageToken.encode`). Nothing binds a token
  to the query that produced it beyond a `Kind` check (`QueryV2Resolver.java:124-129`), which
  separates bucket tokens from sample tokens but not one `QuerySpec` from another — so replaying a
  token against a different spec of the same kind yields well-formed but semantically wrong results.
  **Do not reuse a token across queries.**
- **Malformed tokens are rejected here, unlike the annotation queries.** Three behaviors exist across
  the API surface: `querySamples`/`queryBuckets` reject; `querySampleStatuses` rejects;
  `queryPvMetadata`/`queryConfigurations`/`queryConfigurationActivations` **silently reset to page 1**.
  A shared client-side paging helper must not assume one.
- **`sampleStatusSelector` is rejected on the bucket-oriented methods** (`QueryV2Resolver.java:165`).
  The samples params expose it; the buckets params **must not** — see [D8](#d8--the-buckets-params-omit-samplestatusselector-rather-than-forwarding-it).
- **Non-scalar PV rejection is data-driven, not pre-flight** (#194 still open). `TabularDataUtility`
  throws mid-assembly and the dispatcher rejects, discarding everything assembled. So a non-scalar PV
  with **no buckets in the requested window passes silently**, and the same PV set can succeed on one
  page and reject on the next. Only the first offending PV is named, and which one depends on bucket
  iteration order.
- **Result shape**: columns are bare PV names, sorted ascending and deduped; every resolved PV gets a
  column even with zero data in the window; exactly one `DataValue` per `timestampList` entry per
  column; missing samples are an **unset `DataValue` oneof** (pass through untouched); there is **no
  timestamp column** — the axis lives only in `ColumnTable.timestampList`.
- **A selector resolving to more than `queryV2MaxResolvedPvCount` (10,000) is rejected.**

### D8 — The buckets params omit `sampleStatusSelector` rather than forwarding it

**Decision**: `QuerySpecParams` does not carry `sampleStatusSelector`. The samples params type adds
it; the buckets params type does not.

**Rationale**: the resolver rejects the combination outright (`QueryV2Resolver.java:162-168`), so a
field that can only ever produce a rejection should not be offered. This is the same reasoning as D2 —
prefer making the invalid request unrepresentable over validating it at runtime.

**Consequence**: `QuerySpecParams` is the shared core (`timeRange`, `pvSelector`,
`configurationSelector`) and each method's params type composes it with the fields that method
accepts. This also keeps the `ResultRepresentation` asymmetry honest: `excludeColumnMetadata` is
meaningful on buckets and inert on samples.

## Implementation tasks

### Phase 1 — shared params and the `QuerySpec` builder

**`client/criteria/` (new package)** — D3
- Move `TextMatch`, `AttributeCriterion`, `nonBlank()`, `isBlankKey()` out of `AnnotationClient`,
  carrying their javadoc verbatim (it is the #243 invariant's documentation).
- `AnnotationClient` re-exports or aliases so no existing call site changes.

**`QueryClient`** — new nested types
- `sealed interface PvSelectorParams` with the three records (D2).
- `record QuerySpecParams(TimeRange-ish begin/end, PvSelectorParams pvSelector, List<ConfigurationCriterion> configurationCriteria)`.
  Follow `QuerySampleStatusesParams` (`AnnotationClient.java:2832-2841`) for the timestamp fields'
  shape.
- `record ConfigurationCriterion(...)` modeling the five arms
  (`configurationNameAnyOf`, `clientActivationIdAnyOf`, `categoryAnyOf`, `tagsAnyOf`, `attributes`),
  following `QueryConfigurationActivationsParams` (`:2156`).
- `static QuerySpec buildQuerySpec(QuerySpecParams)` — the shared core. Must:
  - reject nothing itself; all validation is the server's.
  - apply `nonBlank()` to every criterion value list.
  - **omit `configurationSelector` entirely when it has no usable criteria** (D1), with the comment
    explaining why an empty one would be a rejection rather than an omitted filter.
  - dispatch the `PvSelectorParams` arm with an exhaustive `switch` over the sealed interface.

**`GrpcIntegrationQueryServiceWrapper`** — widen `getQueryChannel()` to `ManagedChannel`
(Background §4).

### Phase 2 — `querySamples` (unary)

- `record QuerySamplesParams(QuerySpecParams querySpec, SampleStatusSelectorParams sampleStatus, int limit, String pageToken, boolean useSerializedColumns)`.
- `record SampleStatusSelectorParams(String domain, List<String> layers, List<Integer> statusCodes, Mode mode)`.
- `static QuerySamplesRequest buildQuerySamplesRequest(QuerySamplesParams)` — `limit` set only when
  `> 0`, `pageToken` only when non-blank (the `AnnotationClient.java:2865-2870` idiom).
- `result/QuerySamplesApiResult` (D6).
- `QuerySamplesResponseObserver extends ApiResponseObserverBase<QuerySamplesResponse>`.
- `sendQuerySamples(QuerySamplesRequest)` / `querySamples(QuerySamplesParams)` — the canonical
  three-method shape.
- Javadoc per D7.

### Phase 3 — `querySamplesStream`

- `QuerySamplesStreamResponseObserver` — accumulates, merging **by column name** with a hard failure
  on a differing column set (D6).
- `sendQuerySamplesStream` / `querySamplesStream` — **drop `pageToken`** when building the request
  (D5), and document why.
- Unit test for the accumulation merge specifically.

### Phase 4 — `queryBuckets` + `queryBucketsStream`

- `record QueryBucketsParams(QuerySpecParams querySpec, int limit, String pageToken, boolean useSerializedColumns, boolean excludeColumnMetadata)`
  — no `sampleStatusSelector` (D8).
- `result/QueryBucketsApiResult` (D6).
- Unary and stream observers; the four methods.
- Javadoc notes that `excludeColumnMetadata` **is** functional here, unlike on samples.

### Testing — `QueryClientIT` (new, `integration/query/`)

Mirror `PvMetadataClientIT` / `SampleStatusClientIT`: class-level doc stating it covers the *client
wrapper* (request building, success payloads, streaming accumulation, rejection surfacing via
`ApiResultBase.resultStatus`), with server behavior covered by `QueryV2GrpcIT`.

Build-only tests (no server needed):
- each `PvSelectorParams` arm reaches the right `PvSelector` oneof case;
- an all-blank `TextMatch` in `PvMetadataSelector` emits **no criterion** — assert on
  `getCriteriaCount() == 0`, **not** by expecting an error (the #245 lesson recorded in CLAUDE.md:
  `PvMetadataClientIT.testQueryPvMetadataBlankCriteriaEmitsNoCriterion` had to be rewritten for
  exactly this);
- an empty `configurationCriteria` list omits `configurationSelector` entirely (D1) — assert
  `!request.getQuerySpec().hasConfigurationSelector()`;
- `limit`/`pageToken` omitted when unset/blank;
- the streaming builders emit **no** `pageToken` even when the params carry one (D5).

Round-trip tests against the running query service:
- unary single page; multi-page paging seam via `nextPageToken`;
- missing values arrive as unset `DataValue` oneof;
- streaming accumulation equals the unary result for the same spec;
- a rejection surfaces as `ApiResultStatus.REJECT` with the server's message verbatim;
- `sampleStatusSelector` on a buckets request is unrepresentable (compile-level, D8) — instead assert
  the resolver's reject is reachable through the samples wrapper for a blank domain.

Pinned failure modes (the two the consumer ticket flags):
- **non-scalar PV reject** — including the data-driven footgun: a non-scalar PV with no buckets in
  the window passes silently. Pin both halves, so the behavior has a guard if #194 changes it.
- **oversized-row error** — assert on a substring common to both dispatcher messages, or assert each
  separately (Background §5).

### dp-grpc — the proto comment fix (D1)

Tracked as [osprey-dcs/dp-grpc#149](https://github.com/osprey-dcs/dp-grpc/issues/149). Replace
`query.proto:335-337`:

> If no criteria are supplied, the selector matches nothing and the query returns an empty result
> (omit ConfigurationSelector entirely to query the full TimeRange unconditionally).

with wording that documents the reject: an empty criteria list is a malformed request and is
rejected; omit `ConfigurationSelector` entirely to query the full `TimeRange` unconditionally. Note
in the dp-grpc change that this documents existing server behavior — no service change accompanies
it.

### Issue body update

Post to #244, or edit the body:
- correct the column-metadata claim (already covered by the triage comment; fold into the body so a
  reader of the body alone is not misled);
- record the expanded scope: four wrappers, not one;
- record the `configurationSelector` divergence and its resolution (D1), referencing dp-grpc#149;
- note the `pvSelector.metadataQuery` empty-criteria asymmetry (Background §2), which is unchanged
  and deliberate but undocumented anywhere today.

## Out of scope

- **Migrating the existing ITs onto the new builders.** `QueryTestBase`'s V2 helpers stay; converting
  `QueryV2GrpcIT` and `MongoSyncQuerySamplesV2Test` to the client layer would mix a server-behavior
  test with a client-wrapper test. Worth a follow-up ticket once the wrappers are proven.
- **Fail-fast non-scalar rejection** — #194. This ticket documents and pins the current data-driven
  behavior; if #194 lands, the javadoc note and one test simplify.
- **Revisiting `useSerializedColumns` on samples** — #195. The flag is exposed because the server
  honors it; no performance claim is made.
- **Paging `queryProviders`/`queryPvStats`** — needs a proto change, tracked as #265.
- **A shared client-side paging helper.** D7 documents three different malformed-token behaviors
  across the API surface; unifying them is its own ticket.
- **`ConfigurationSelector` semantics change.** D1 settles this as a comment fix; changing the server
  to "match nothing" or "match-all" is explicitly not done.

## Dependencies and sequencing

- **Blocks**: dp-desktop-app#39 task 6, and nothing else in that ticket — tasks 3, 4, 5 and #36 are
  already unblocked.
- **Blocked by**: nothing. All four V2 server methods are implemented and tested.
- **Independent of**: #194 and #195 (both touch `querySamples` behavior; neither gates this work in
  either direction). #265 (proto paging for V1 methods) is unrelated.
- **The dp-grpc comment fix is independent** of the dp-service work and can land in either order —
  it changes no generated code, only a comment.
- **Phases 2, 3 and 4 each depend on phase 1** and are otherwise independent of each other; phase 4
  could be dropped without affecting 2 or 3 if the PR needs to shrink.

## Implementation notes

Two departures from the plan as written, both settled by what the code turned out to say.

### D3's re-export was unnecessary

The plan constrained the `TextMatch`/`AttributeCriterion` promotion not to change
`AnnotationClient`'s public API shape, because dp-desktop-app was believed to consume
`AnnotationClient.TextMatch`. It does not — a grep of that repo finds no reference to either type
(it uses `SavePvMetadataParams`, `QuerySampleStatusesParams` and the other params records, none of
which mention them). Java has no type alias and a record cannot be subclassed, so honoring the
constraint would have required a wrapper type or moving the declarations onto
`ServiceApiClientBase`, which conflates a channel holder with criteria value types.

With no cross-repo consumer the clean move is available: the canonical declarations live in
`com.ospreydcs.dp.client.criteria`, `AnnotationClient` imports them (its ~60 unqualified call sites
are untouched), and the four `AnnotationClient.TextMatch` / `AnnotationClient.AttributeCriterion`
references in `PvMetadataClientIT` and `ConfigurationClientIT` were updated to the new names.
`nonBlank()`/`isBlankKey()` are static-imported, so every existing call site reads exactly as before.

### The oversized-row test is not reachable from an IT

The plan's testing section asked `QueryClientIT` to pin the oversized-row error alongside the
non-scalar reject. The byte budget is a **dispatcher constructor argument**
(`new QuerySamplesUnaryDispatcher(observer, byteBudget)`), which only a test constructing the
dispatcher directly can set — an IT going through `QueryServiceImpl` gets the configured production
value, and producing a genuinely oversized row against it would need a fixture far larger than the
rest of the suite. The behavior is already pinned at the dispatcher level by
`MongoSyncQuerySamplesV2Test.testUnaryOversizedSingleTimestampErrors` and
`testStreamIndivisibleOversizedRowErrors`, including the differing message text that Background §5
flagged. Adding a production-side test hook to reach it through the client layer would be a worse
trade than leaving it covered where it is covered; the wrapper javadoc still carries the behavior,
including the correction that narrowing the time range cannot help.

### What landed

- `client/criteria/`: `TextMatch`, `AttributeCriterion`, `ClientCriteria` (`nonBlank`, `isBlankKey`).
- `QueryClient`: the `PvSelectorParams` sealed hierarchy, `QuerySpecParams`,
  `ConfigurationCriterion`, `SampleStatusSelectorParams`, `QuerySamplesParams`,
  `QueryBucketsParams`; `buildQuerySpec` and the six request builders (including the two stream
  variants that drop `pageToken`); four observers; the eight `sendXxx`/`queryXxx` methods.
- `result/`: `QuerySamplesApiResult`, `QueryBucketsApiResult`.
- `QuerySamplesStreamAccumulationTest` (10 tests) and `QueryClientIT` (22 tests), all passing.
- `GrpcIntegrationQueryServiceWrapper.getQueryChannel()` widened to `ManagedChannel`.
- CLAUDE.md records the invariants that outlive the ticket.

## Review revisions (PR #270)

Three findings from the PR review, all fixed in the review-fix commit. Each is recorded here
because each one *reverses* something the plan or the first implementation had decided, and the
reasoning for the reversal is not recoverable from the code alone.

### R1 — D1's selector drop was right for the wrong scope, and silently widened the query

**Finding**: D1 concluded "the wrapper omits an all-empty `configurationSelector` entirely rather
than emitting one", and the first implementation applied that unconditionally — including when the
caller *had* supplied criteria that all turned out blank. `QueryV2Resolver.resolveIntervals()`
treats an absent selector as "the whole query range is the single retrieval interval", so that
caller's query silently widened from "only while configuration X was active" to the entire time
range, returning strictly more data than they asked for with no diagnostic.

This is the #243 failure mode with its sign flipped, and D1 did not notice the inversion because
every other application of the "no criterion contributes no filter" idiom in the client layer
narrows toward correctness: a blank prefix would have matched *everything*, so dropping it is the
safe direction. `configurationSelector` is the one place where dropping is the unsafe direction.

**Fix**: `buildQuerySpec` now distinguishes the two reasons the selector can come out empty.
Null/empty `configurationCriteria` still drops it — no restriction was asked for, and D1's
reasoning holds intact for that case. A non-empty list yielding no usable criterion instead emits
the empty selector for the server to reject, matching how `PvNameListSelector` already treats an
all-blank name list. A list containing only `null` entries counts as "not requested": a null
criterion is a caller bug distinct from a filled-in-but-blank field, and rejecting on something the
caller never typed would be unhelpful.

**Tests**: `testBuildQuerySpecOmitsUnrequestedConfigurationSelector` keeps the two drop cases;
`testBuildQuerySpecEmitsEmptySelectorForUnusableConfigurationCriteria` pins the new reject case.
The old `testBuildQuerySpecOmitsEmptyConfigurationSelector` pinned the widening as intended
behavior and was split into those two.

### R2 — `ConfigurationCriterion` documented the multi-arm hazard instead of preventing it

**Finding**: D2 made `PvSelectorParams` a sealed interface precisely so that a multi-arm oneof
cannot be resolved by silent preference order, citing `buildQueryTableRequest` as the anti-pattern.
`ConfigurationCriterion` is also a proto oneof, and `buildConfigurationCriterion` did exactly what
D2 rejected: returned on the first populated arm in declaration order, with the record javadoc
merely documenting it. Documenting a hazard is not preventing it, and the PR had set the bar.

**Fix**: `buildConfigurationCriterion` evaluates every arm, counts how many are populated, and
returns null unless exactly one is — so a multi-arm criterion is dropped rather than half-honored,
and R1's logic then emits the empty selector so the request is rejected. The record javadoc now
explains why this oneof is enforced at build time while `pvSelector` is enforced at compile time:
`configurationCriteria` is a *repeated* field, so a sealed hierarchy would cost five permitted
records plus a wrapper and force every caller to build a heterogeneous list, for a criterion that
is usually one name list. The rule is identical in both places; only the enforcement point differs.

**Test**: `testBuildQuerySpecRejectsMultiArmConfigurationCriterion`.

### R3 — the fragmented serialized-column table looked assembled

**Finding**: under `useSerializedColumns` the server puts every column in `serializedDataColumns`
and leaves `dataColumns` empty, so `QuerySamplesStreamResponseObserver`'s by-name merge is inert and
the accumulated table holds per-page column *fragments* against a fully concatenated timestamp axis.
D7 covered this in the observer javadoc ("callers should deserialize per page"), but a caller who
does not read it gets back a structurally valid `ColumnTable` whose columns do not line up with its
axis — a wrong answer rather than an error, which is the category this PR otherwise refuses to leave
to documentation.

**Fix**: the observer counts pages contributing serialized columns and exposes
`isSerializedColumnsFragmented()`; `QuerySamplesApiResult` carries it as
`serializedColumnsFragmented`, set only by the streaming path. False for the unary method, for the
non-serialized representation, and for a single-page stream — all of which are directly consumable.
The existing three-arg constructor is retained as a delegating two-arg form, so no call site outside
`sendQuerySamplesStream` changed.

**Tests**: `testSinglePageSerializedColumnsAreNotFragmented` and
`testDataColumnStreamIsNeverFragmented` pin the false cases (the flag must not push callers away
from a good result); `testSerializedColumnsAreConcatenated` gained the true assertion.
