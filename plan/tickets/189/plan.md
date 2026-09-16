# Implementation Plan — Query API V2 handling (dp-service #189)

**Status:** open questions resolved (§7); ready for detailed design / implementation
**Issue:** https://github.com/osprey-dcs/dp-service/issues/189
**Parent:** osprey-dcs/data-platform#82
**Proto contract:** dp-grpc PR #124 / issue #123 (merged; dp-grpc 1.15.0 already the dp-service dependency, V2 stubs present in the jar)
**Target module version:** dp-service 1.15.0

---

## 0. Decisions summary (all §7 questions resolved)

| # | Question | Decision |
|---|----------|----------|
| Q1 | `querySamples` unary paging | Stateless timestamp-advanced re-query; `limit` a soft cap (never split a timestamp); opaque token keeps (b) cached-state migration open |
| Q2 | `queryBuckets` unary paging | Keyset (seek) on `(pvName, firstTimeSecs, firstTimeNanos)`; no tiebreaker (composite `_id` proves uniqueness); **follow-up ticket:** standardize MLDP paging on keyset |
| Q3 | Config fragmentation × paging | Compound `$or` overlap filter; position-only token; best-effort re-resolution. **Cross-cutting principle:** all V2 paging is stateless, best-effort, position-only |
| Q4 | Non-scalar PV in `querySamples` | Reject (scalar-only). v1: detect at assembly (catch `DpException`, enrich w/ PV name); **deferred ticket:** early reject via cheap type lookup (NOT the stats aggregation) |
| Q5 | `useSerializedColumns` on samples | Implement but do not optimize; symmetry only, no perf claim; empty `encoding`. Real optimization on `queryBuckets`. Revisit on real usage (no ticket yet) |
| Q6 | Annotation filter reuse | Extract shared neutral helpers in **common**; behavior-preserving (existing annotation tests pass unchanged); promote `"aliases"`/`"attributes."` literals to `BsonConstants` |
| Q7 | Paging limits & byte guard | Byte guard becomes page boundary (emit token), not error; zero-progress guard; single indivisible oversized item still errors; silent max clamp; defaults TBD via config |
| Q8 | `excludeColumnMetadata` | `queryBuckets`: include free, suppression is new work. `querySamples`: inert — no column metadata (matches V1 drop); rejected "most-recent-bucket wins" (false/page-flickering provenance) |
| Q9 | `ColumnTable` ordering/presence | Sorted by PV name; every resolved PV gets a column (all-unset if no data). Resolution must produce the concrete PV **name list**, not just a filter |
| Q10 | Regex safety & PV-set bounds | Configurable max resolved-PV-count (reject); catch-and-reject invalid regex (**fixes latent V1 `queryTable` bug**); complexity guard deferred |
| Q11 | `metadataQuery` PVs with no data | Intersect metadata-matched names with archive existence (`distinct` on buckets); drop never-ingested PVs; keeps Q9's all-unset-column rule clean |

**Follow-up tickets (filed):**
- **#193** — standardize MLDP paging on keyset + migrate annotation metadata queries (Q2).
- **#194** — `querySamples` fail-fast non-scalar reject before retrieval, via a cheap PV-type
  lookup (Q4).
- **#195** — revisit `useSerializedColumns` on `querySamples` once real usage data exists (Q5;
  tracking ticket, only if warranted).

---

## 1. Goal and scope

Implement the four Query API V2 RPCs in dp-service, in a **single phase**, reusing
(and where useful, evolving) the existing query subsystem rather than building the
greenfield `planner/retrieval/formatter` package tree from the original sketch:

- `queryBuckets` (unary) / `queryBucketsStream` (server-streaming)
- `querySamples` (unary) / `querySamplesStream` (server-streaming)

Full `QuerySpec` surface **except** `SampleStatusSelector` (reserved field 4):

- `timeRange` (required)
- `pvSelector` — `pvNameList`, `pvNamePattern`, `metadataQuery` (all in scope)
- `configurationSelector` (in scope)
- `executionOptions` (paging: `limit`, `pageToken`)
- `resultRepresentation` (`excludeColumnMetadata`, `useSerializedColumns`)

Out of scope: sample-status filtering; annotation integration; aggregation/resampling;
Arrow/Parquet; the V1 row-map table format (V2 standardizes on column-oriented `ColumnTable`).

---

## 2. How V2 maps onto the existing subsystem

The existing V1 pipeline (verified against the code) is:

```
QueryServiceImpl (gRPC entry, validation entry point)
  → QueryHandlerInterface.handleQueryXxx()      [MongoQueryHandler: build dispatcher+job, enqueue]
     → QueryXxxJob.execute()                      [HandlerJob: call client, hand cursor to dispatcher]
        → MongoSyncQueryClient.executeQueryXxx()  [shared bucket retrieval]
        → QueryXxxDispatcher.handleResult(cursor) [format cursor → response(s), stream/close]
```

Key facts established from the code that anchor the design:

- **Shared retrieval already exists.** `MongoSyncQueryClient.executeBucketDocumentQuery(columnNameFilter, startSecs, startNanos, endSecs, endNanos)` is the single overlap-query core used by both the data path and the table path. Overlap predicate (half-open `[begin,end)` bucket selection):
  `bucket.firstTime < endTime AND bucket.lastTime >= beginTime`.
  Returns an **unbounded** `MongoCursor<BucketDocument>` sorted ascending by `(pvName, firstTimeSecs, firstTimeNanos)`. **No limit / no paging today** — chunking is done downstream by message size.
- **Sample assembly + trimming already exists.** `QueryTableDispatcher` + `TabularDataUtility.addBucketsToTable(...)` assemble overlapping buckets into a `TimestampDataMap` **trimmed to the exact half-open `[begin,end)` window** (`addColumnsToTable` skips samples outside the range), and `columnTableResultFromMap` already produces exactly the V2 `ColumnTable` shape: a union `TimestampList` + one `DataColumn` per PV + an **unset `DataValue`** where a PV has no sample at a union timestamp. `querySamples` is the direct evolution of this path.
- **Bucket → response conversion already exists.** `BucketDocument.dataBucketFromDocument(doc, querySpec)` builds a `DataBucket`, and the stored column document polymorphically attaches either a `DataColumn` or a `SerializedDataColumn` (`ColumnDocumentBase.addColumnToBucket`). Boundary buckets are returned **whole** (no trimming) in the data path — exactly V2 `queryBuckets` semantics.

Sketch concept → existing realization:

| Sketch concept          | Existing dp-service realization                                   |
|-------------------------|-------------------------------------------------------------------|
| Query Planner           | validation (`QueryHandlerUtility`) + a new V2 resolution step     |
| ExecutionPlan           | a new internal resolved-query object (see §4)                     |
| Bucket Retrieval Engine | `executeBucketDocumentQuery` (unchanged core)                     |
| Bucket Formatter        | evolve `QueryData*Dispatcher` bucket-building                     |
| Sample Formatter        | evolve `QueryTableDispatcher` column-table assembly + trimming    |

**Decision:** follow the established pattern; do **not** introduce a parallel `planner/retrieval/formatter` package tree. Add V2 methods alongside V1 in the same packages, sharing retrieval and reusing tabular assembly.

---

## 3. Component work (proposed)

### 3.1 gRPC entry — `QueryServiceImpl`

Override the four generated V2 stubs. Each: log → validate+resolve (§4) → delegate to handler.
Add V2 response helpers mirroring the V1 static builders:
`queryBucketsResponse*` / `querySamplesResponse*` for `Reject` / `Error` / success / empty.
**Rejections and errors are `ExceptionalResult` in the response `oneof`** (`RESULT_STATUS_REJECT` / `RESULT_STATUS_ERROR`), never a transport gRPC status. Empty result is an **empty payload**, not an `ExceptionalResult`.

### 3.2 `QueryHandlerInterface` / `MongoQueryHandler`

Add:
- `handleQueryBuckets(request, obs)` / `handleQueryBucketsStream(request, obs)`
- `handleQuerySamples(request, obs)` / `handleQuerySamplesStream(request, obs)`
- a V2 validation/resolution entry (`resolveQueryV2(...)` returning the resolved-query object or a `ResultStatus` error)

`MongoQueryHandler` builds the right dispatcher + job and enqueues, as today.

**Shared-vs-duplicated jobs:** the four methods share one `QuerySpec`, one retrieval path, and differ only in the final formatter and streaming/unary behavior. Prefer **one `QueryV2Job`** (like `QueryDataJob` serves unary+stream+bidi by injected dispatcher) parameterized by an injected V2 dispatcher, over four near-duplicate jobs. Bucket-vs-sample and unary-vs-stream become dispatcher variants.

### 3.3 Planning / validation / resolution (the "planner")

A shared step — a helper, not a new subsystem — turning `QuerySpec + ExecutionOptions`
into a resolved internal query. Responsibilities:

1. **Validate invariants** (§6).
2. **Resolve `PvSelector` → concrete PV name set:**
   - `pvNameList` → the list directly.
   - `pvNamePattern` → regex over PV name (as V1 `queryTable` PVNAMEPATTERN already does).
   - `metadataQuery` → run PV-metadata criteria against the `pvMetadata` collection, collect matching `pvName`s. **Reuses annotation metadata-query filter logic (see §5).**
3. **Resolve `ConfigurationSelector` → effective retrieval intervals:**
   - Match `ConfigurationActivation` records by the non-temporal criteria (name, clientActivationId, category→`internalCategory`, tags, attributes).
   - Union their `[startTime,endTime)` intervals (endTime null = open-ended).
   - Intersect the union with `QuerySpec.timeRange` → a **possibly fragmented** set of sub-intervals.
   - No criteria supplied but selector present → matches nothing → empty result. Selector omitted entirely → full `timeRange`.
4. **Validate/normalize paging** (`limit` default when 0; `pageToken` decode; streaming rejects non-empty token).

Output: a resolved-query object (the "ExecutionPlan") carrying: resolved PV filter, the effective interval list, page size, decoded continuation state, and the representation flags.

### 3.4 Retrieval

Reuse `executeBucketDocumentQuery`. Two wrinkles to fold into the retrieval/paging layer (not the formatters):

- **Configuration fragmentation:** retrieval may span multiple sub-intervals. Either issue one overlap query per sub-interval and merge, or build a compound Mongo filter (`$or` of per-interval overlap predicates) in a new `executeQueryV2(...)` client method. **Open question — see §7.**
- **Paging:** the current cursor is unbounded. V2 needs bounded, resumable pages. **This is the central design problem — see §7.**

### 3.5 Formatting

- **Bucket formatter** (`queryBuckets` / `queryBucketsStream`): evolve the `QueryData*Dispatcher` bucket-building. Emit `DataBucket`s, boundary buckets **whole**. Honor `useSerializedColumns` (already polymorphic in `addColumnToBucket` — needs a flag to force the serialized form regardless of how the column was stored; **confirm feasibility, §7**). Honor `excludeColumnMetadata`.
- **Sample formatter** (`querySamples` / `querySamplesStream`): evolve `QueryTableDispatcher.columnTableResultFromMap` to emit the V2 `ColumnTable` (`TimestampList` + `DataColumn` per PV, unset `DataValue` for missing samples), **trimmed** to `[begin,end)` (already done by `TabularDataUtility`). Drop the row-map format. Honor `useSerializedColumns` by serializing the assembled `DataColumn`s into `serializedDataColumns` (populate exactly one of the two lists). Honor `excludeColumnMetadata`.

### 3.6 Paging semantics (from proto)

- **Unary** (`queryBuckets`/`querySamples`): `limit` = max result objects per page (buckets / timestamps-rows resp.); `pageToken` continues from prior `nextPageToken`; empty `nextPageToken` == last page. No `hasMore`, no `totalCount`. Continuation token is opaque.
- **Streaming** (`*Stream`): `limit` = per-message chunk size; **fire-and-consume** — server streams to completion, `nextPageToken` empty on every message, inbound `pageToken` **must be empty** (non-empty → `ExceptionalResult`, do not silently return page 1).

---

## 4. The resolved-query / ExecutionPlan object

New internal (non-proto) class, e.g. `query.handler.model.QueryV2Request` (name TBD), holding:

- resolved PV set / PV name filter (`Bson`)
- effective retrieval intervals: `List<TimeRange-ish>` (fragmented)
- page size (`limit`, normalized)
- decoded continuation state (see §7 for shape)
- `useSerializedColumns`, `excludeColumnMetadata`
- result mode enum: BUCKET vs SAMPLE; UNARY vs STREAM

This is the stable seam between resolution and retrieval, matching the sketch's ExecutionPlan intent without a new package tree.

---

## 5. Reusing PV-metadata and configuration-activation query logic

Confirmed from the annotation subsystem: the criterion→`Bson` mapping for
`queryPvMetadata` (`MongoSyncAnnotationClient.executeQueryPvMetadata`) and
`queryConfigurationActivations` (`executeQueryConfigurationActivations`) is **inline,
not factored**, and is coupled to the V1 annotation request proto types. V2 uses
**different** proto types (`PvSelector.MetadataQuery.Criterion`,
`ConfigurationSelector.Criterion`) with the same semantics.

**Proposed refactor (low-risk, mechanical):** extract the criterion→`Bson` mapping into
package-visible static helpers that accept **neutral inputs** (name exact/prefix/contains
lists, tag lists, attribute key+values, time instants) and return `Bson`, so both the V1
annotation clients and the V2 planner call the same code. Two literals to promote to
`BsonConstants` during extraction so V1/V2 stay in sync:
- the `"aliases"` field literal in `executeQueryPvMetadata`
- the `"attributes."` map-key prefix literal in `executeQueryConfigurationActivations`

Alternative (if the refactor is deemed too invasive for this phase): duplicate the mapping
for the V2 types with a `// TODO: unify with annotation filter logic` marker. **Recommend
extraction** — it is small and prevents drift. **Open question §7** on how far to go.

`ConfigurationActivation` interval fields confirmed: `Instant startTime`, `Instant endTime`
(null = open-ended), denormalized `String internalCategory`. Category criterion filters on
`internalCategory`.

---

## 6. Validation invariants (handlers must enforce; proto3 cannot)

- **PvSelector presence:** unset `PvSelector`, or set `PvSelector` with unset `selector` oneof → `ExceptionalResult` (reject).
- **Exactly-one-criterion:** each `PvSelector.MetadataQuery.Criterion` and each `ConfigurationSelector.Criterion` must have exactly one arm set.
- **TimeRange presence/order:** reject unset `timeRange`, unset endpoints, or `endTime <= beginTime`. (V1 `validateDataQueryTimeRange` is the model — reuse/adapt. Note its existing TODO for a max-range cap.)
- **Column list exclusivity:** `ColumnTable` populates exactly one of `dataColumns` / `serializedDataColumns`, driven by `useSerializedColumns`.
- **Streaming paging:** non-empty `pageToken` on a streaming call → `ExceptionalResult`; emit empty `nextPageToken` on every streamed message.
- **Reserved field 4** (`sampleStatusSelector`): no handling needed (it's reserved; cannot be set).

---

## 7. Open questions / concerns to resolve BEFORE implementation

These are the design decisions the ticket leaves open (or that surfaced from the code) and
that materially affect the implementation. **Nothing in the ticket is set in stone; these
need answers first.**

### Q1 — `querySamples` unary paging by timestamp. **RESOLVED**
**Decision: option (a) — stateless timestamp-advanced re-query.** The continuation token
encodes the last-emitted timestamp; each page re-runs the overlap query with `beginTime`
advanced to that timestamp and assembles until `limit` rows are produced. Re-reads the
boundary buckets that straddle a page cut each page — accepted.

**`limit` is a soft cap:** page boundaries fall *between* union timestamps so all PV values
for a given timestamp stay together; a page may slightly overshoot `limit` rather than split
a timestamp. Never split a timestamp across pages.

**Migration path to (b) (server-side cached cursor/state) is open** if we hit performance
issues in the wild. This is safe precisely because the token is **opaque by contract**:
moving to (b) is a pure server-side change — no proto change, no client change, no token
break. A future (b) implementation can still accept an (a)-style timestamp token as a
cache-miss fallback and rebuild state from it.

**Implementation constraint to preserve the migration path:** keep the token's *meaning*
("resume at timestamp T") decoupled from its *encoding*, and let no client-visible behavior
depend on boundary re-reads being cheap. Then (b) is a drop-in.

Background (why this is the hard problem): the current sample path (`QueryTableDispatcher`)
**materializes the entire union table in memory in one shot** (bounded only by the ~4MB
message-size limit, above which it errors). There is no timestamp cursor today; (a) adds
resumable, bounded-memory paging without server state.

### Q2 — `queryBuckets` unary paging unit and token. **RESOLVED**
**Decision: keyset (seek) paging.** A page is the next `limit` buckets in the existing
retrieval sort order `(pvName, firstTimeSecs, firstTimeNanos)`. The opaque token encodes the
last-emitted `(pvName, firstTimeSecs, firstTimeNanos)` tuple; the next page adds a
`> that tuple` filter on the same compound sort key. O(1) seek via the sort index, no
re-scan, stable under concurrent inserts. Chosen over skip/offset for stability and to match
the Q1 sample-paging seek model (both V2 unary paths use the same seek-token model).

**No separate tiebreaker required — uniqueness is enforced by the primary key.** The bucket
`_id` is the derived composite `pvName + "-" + firstTimeSeconds + "-" + firstTimeNanos`
(`BucketDocument.generateBucketsFromRequest`, ~line 99). So `(pvName, firstTime)` is unique
by construction (a collision would collide on `_id`), and a bare `>`-tuple keyset filter
cannot skip or double-emit at a page boundary.

**Implementation note (do not "optimize" the token into an `_id`-string seek):** because
`_id` == `pvName-secs-nanos` concatenated, it is tempting to seek on `_id > lastId`. That is
**wrong** — string comparison mis-orders numeric components (`"pv-9-0" > "pv-10-0"` as
strings), diverging from the compound numeric sort. Key the token on the structured tuple and
seek on the compound sort `(pvName, firstTimeSecs, firstTimeNanos)`.

**Follow-up ticket to file (paging convention divergence):** V2 unary methods use keyset;
the existing `queryPvMetadata()` and `queryConfigurationActivations()` use Base64 skip/offset;
paging is also about to be added to `queryAnnotations()`. File a ticket to **standardize MLDP
paging on keyset** and migrate the annotation metadata queries (skip/offset is O(offset) and
unstable under concurrent writes). Acceptable to ship V2 divergent short-term; not acceptable
to leave the divergence unrecorded or to let `queryAnnotations()` inherit skip/offset by
default.

### Q3 — Configuration fragmentation × paging interaction. **RESOLVED**
**Decision: single compound `$or` overlap filter.** When `ConfigurationSelector` resolves to
multiple disjoint sub-intervals (union of matching activations' intervals ∩ `timeRange`),
retrieval issues **one** Mongo query whose time predicate is an `$or` of the per-fragment
overlap predicates, AND-ed with the PV filter. Fragments are disjoint and time-ordered, so
the existing sort `(pvName, firstTimeSecs, firstTimeNanos)` (buckets) / timestamp order
(samples) remains globally monotonic across fragments — therefore **the paging token stays
position-only** (the same keyset tuple / resume-timestamp as Q1/Q2); it does **not** need to
encode a fragment index. If the `$or` proves index-unfriendly in practice we can switch to
per-fragment queries internally without touching the token (opaque by contract).

**Best-effort re-resolution on resume (accepted).** The token does not freeze the resolved
fragment list. Each page re-resolves `ConfigurationSelector` from the (unchanged) request;
if activations changed between pages the fragment set may shift. This is the same
opaque-token, best-effort stability posture as Q1/Q2 — see the cross-cutting principle below.

**Query-construction note (correctness):** the keyset seek predicate wraps the *whole*
fragment `$or` — the page-N filter is `AND(pvFilter, $or(fragmentOverlaps...), >keysetTuple)`.
The `>keysetTuple` seek must **not** be distributed into each `$or` branch. Index-friendliness
of the `$or`-of-ranges is the caveat that would motivate the per-fragment fallback above.

### Cross-cutting paging principle (Q1 + Q2 + Q3): stateless, best-effort, position-only tokens
All V2 paging is uniform: the token is a **pure position marker**; each page **re-runs
resolution + retrieval** from the unchanged request and seeks to the token position. No
server-side state and **no frozen snapshot** of the resolved plan. Consequence: concurrent
writes (new buckets, changed activations) between pages are best-effort — a paginated query
is not a point-in-time snapshot. This is one rule to document and test once, not three
special cases. (Escalation path if this proves insufficient: Q1's option (b) server-side
cached state, which *would* give snapshot stability — a pure server-side change, no token
break.)

### Q4 — Sample path with non-scalar columns. **RESOLVED**
**Contract (locked): `querySamples` supports scalar PVs only — reject on any non-scalar PV.**
A resolved PV set (esp. via `pvNamePattern` / `metadataQuery`) may include array/binary/
image/struct PVs. If any resolved PV is non-scalar, `querySamples` returns an
`ExceptionalResult` (no partial table) directing the caller to `queryBuckets`. `queryBuckets`
itself has no such restriction (returns whole buckets of any type).

**Mechanism v1 (this phase): detect at assembly time — zero added cost.**
`TabularDataUtility.addBucketToTable` already throws `DpException` on the first non-scalar
column (only `ScalarColumnDocumentBase` and legacy `DataColumnDocument` convert to
`DataColumn`). The V2 sample dispatcher **catches** that `DpException` and turns it into a
clean, specific `ExceptionalResult`. Retrieval was happening regardless, so there is no extra
cost; the only downside is the reject arrives after retrieval has begun, not before.

**Required change to the shared exception (correctness of the error message):** the current
`DpException` includes only the column *type* (`getClass().getSimpleName()`) and uses
export-specific wording ("cannot be exported to tabular format (CSV/Excel/etc)"), which is
misleading for a query. The PV name **is** available at the throw site
(`bucket.getPvName()` / `columnDocument.getName()` in `TabularDataUtility.addBucketToTable`,
~line 74). Plan: enrich the shared exception to carry the **PV name + type** in neutral
wording, and let the **query dispatcher** translate the caught `DpException` into the
`querySamples`-specific message ("...PV `<name>` is `<type>`; use queryBuckets"). The export
framework keeps its own phrasing. (Shared utility — don't rewrite its message for one caller.)

**Mechanism v2 (deferred — ticket): fail-fast reject before retrieval.** Rejecting *during
resolution* would need a per-PV type probe. **The PV-stats path is NOT it** — it is a heavy
Mongo aggregation (`$match→$project→$sort→$group` by pvName with `$last` accumulators) and
running it just to learn one bit of type info per PV is far too expensive. Defer until a
**cheap** type lookup exists (per-PV type cache, or a covered `.find().limit(1)` reading a
single bucket's `dataType` per PV). Contract is fixed now; the early-reject optimization is a
deliberate future step gated on that cheap lookup.

### Q5 — `useSerializedColumns` for `querySamples`. **RESOLVED**
**Decision: implement on the sample path, but do NOT optimize and make NO performance claim
(option 1).** Build the `DataColumn`s as normal, serialize each to bytes into
`serializedDataColumns`, populate **exactly one** of `dataColumns` / `serializedDataColumns`,
preserve the unset-oneof missing-value encoding. Leave `SerializedDataColumn.encoding` empty
(there is no meaningful per-column encoding for an assembled sample column — no struct/image
schema, just `DataValue`s). It is present for **API symmetry** with the bucket path, not as a
speed optimization. Document it as such.

**Why not a performance win here (unlike `queryBuckets`):** sample columns are **assembled
fresh** at query time (union axis + synthesized missing-value gaps across buckets) — they
never existed as stored bytes, so there is **no deserialize→reserialize round trip to avoid**.
`useSerializedColumns=true` *adds* an explicit `.toByteString()` step (more server work), and
gRPC serializes the response either way, so the wire bytes are ~identical and the net effect
is a wash at best. The theoretical client-side lazy-parse/Arrow benefit is hypothetical for
the target Python consumer, which materializes values for analysis anyway.

**Contrast — `queryBuckets` (where the flag IS useful):** the stored column is already
serialized bytes in the bucket document; `useSerializedColumns=true` passes those bytes
through, skipping the deserialize-then-gRPC-reserialize round trip. Keep it as a real
optimization there. (Confirm during implementation whether forcing the serialized form for a
bucket stored in a *typed/legacy* form requires on-the-fly serialization vs. passthrough.)

**Do NOT implement the sample-side flag as if it were a tuned fast-path** — that is how a
dubious feature gets entrenched. **No ticket filed yet by decision**: wait for real-world
usage. If telemetry shows nobody sets `useSerializedColumns` on `querySamples`, that is the
evidence to deprecate it in the next API version — data, not a hunch.

### Q6 — How far to refactor annotation filter logic (§5). **RESOLVED**
**Decision: (a) extract to shared neutral helpers.** Pull each criterion→`Bson` mapping out of
`MongoSyncAnnotationClient` into package-visible static helpers taking **neutral inputs**
(exact/prefix/contains lists, tag lists, attribute key+values, `Instant`s) returning `Bson`,
called by **both** the V1 annotation clients and the V2 planner. One implementation, no drift.
Annotation client + its tests are in scope for this phase (accepted). (Option (c) — collapsing
the parallel proto criterion types — is rejected: the proto deliberately duplicated them to
stay self-documenting; extraction unifies the *mapping*, not the types.)

**Where the helpers live — common, not annotation.** The V2 planner is in the *query*
subsystem; the source logic is in *annotation*. Do **not** make query depend on annotation.
Put the neutral helpers in a **common** location (e.g. `common/mongo/` filter-builder) so both
subsystems depend on `common` (arrows point the right way: annotation → common, query →
common).

**Acceptance bar — the extraction must be behavior-preserving (a pure move):**
1. Extract; wire the existing annotation client methods to call the new helpers so they emit
   **byte-identical `Bson`** to today.
2. **Existing annotation tests must pass unchanged** — that is the regression proof. If an
   existing test's *assertions* (not just mechanical relocation) need to change, the extraction
   altered behavior — **stop and investigate**.
3. Only then add the V2 callsite and **new** tests for the V2 criterion types.

**Promote the two literals to `BsonConstants` (correctness, not tidiness):** today the
annotation code uses raw `"aliases"` and `"attributes."` string literals. If V2 referenced a
`BsonConstants` value that did not *exactly* match, the two APIs would silently query different
fields. Extraction forces both onto the same constant — that is the point.

**Security-sensitive note:** the mapping includes user-supplied regex handling
(`Pattern.quote` on prefix/contains). Two drifting copies of regex-escaping logic is exactly
the class of bug a single source prevents.

### Q7 — `limit = 0` default page size, max clamp, and byte-guard interaction. **RESOLVED**

**Byte guard becomes a page boundary, not an error.** A page is bounded by BOTH `limit`
(count) AND the ~4MB message-size guard (`GrpcServer.incomingMessageSizeLimitBytes`). A page
ends when **either** `limit` items are reached **or** adding the next item would exceed the
byte budget — whichever comes first — and **both** cases emit a normal page with a
`nextPageToken` (for unary) / continue the stream (for streaming). This replaces the V1 sample
path's "assembled table > 4MB → error" with "→ paginate." This is the single most important
behavioral change paging introduces.

**Zero-progress guard (get this exactly right, or clients loop forever):** every page must
emit **at least one** item. Rule: always include the first item of a page; end the page
*before* adding an item that would overflow the byte budget, but only *after* at least one item
is already in the page.

**Indivisible-oversized item still errors (the one exception).** If a *single* indivisible
item exceeds the byte budget — one `DataBucket`, or one union timestamp's full cross-PV row —
it cannot be paged out of (the next page would face the same item and make zero progress →
infinite loop), so it must still return an `ExceptionalResult`. This matches existing behavior
(`QueryDataStreamDispatcher` errors on a single oversized bucket) and stays.

**Max `limit` clamp — silent.** A configured server max caps `limit`; a request exceeding it
is **silently clamped** (not rejected) to keep page row-counts predictable rather than letting
`limit` be bounded only by the byte guard (which varies wildly with data width).

**Defaults — TBD, conservative, config-driven.** Default page size (when `limit==0`) and the
max clamp are config keys; separate values for buckets (count) vs sample rows vs streaming
chunk. Numbers deferred (start conservative, tune later). **New config keys → update BOTH
`src/main/resources/application.yml` and `src/test/resources/application.yml`** per the
test-config shadowing convention.

### Q8 — `excludeColumnMetadata` default-include semantics. **RESOLVED**
Flag is inverted (default false = include; true = suppress), deliberately, to dodge the
proto3 "can't default a bool to true" footgun. The two paths differ (verified in code):

**`queryBuckets`: include is free, suppression is the new work.** `dataBucketFromDocument`
already restores `columnMetadata` onto the bucket column via
`ColumnDocumentBase.applyMetadataToProto()`, so the default (include) is the existing
behavior. Only `excludeColumnMetadata=true` needs new code: clear/skip `metadata` on each
emitted `DataColumn`/`SerializedDataColumn` (post-step on the built bucket, or a flag threaded
into `addColumnToBucket`).

**`querySamples`: does NOT carry column metadata; `excludeColumnMetadata` is INERT (option 4).**
Confirmed in code: the tabular/sample path builds columns via
`ScalarColumnDocumentBase.toDataColumn()` (name + values only — it does **NOT** call
`applyMetadataToProto`, unlike `toProtobufColumn()`), so V1 **already drops** `ColumnMetadata`
on the table path. V2 keeps that: `querySamples` returns aligned values only, and the flag has
no effect there (documented).

**Why not "most-recent-bucket wins" (rejected option 1):** a sample-query column is a
**synthesized artifact** — the union of N buckets' samples re-gridded onto a shared timestamp
axis with fabricated missing-value gaps. It corresponds to no single real ingested column.
Stamping one bucket's `ColumnMetadata` on it makes a **provenance claim that isn't true**
(the metadata describes one of N buckets but is attached to a column drawn from all N). Worse
**under paging**: metadata would be reconciled per page from whichever buckets that page
touched, so the *same PV's column could carry different metadata on page 1 vs page 2* — a
field clients expect to be a stable column property becomes page-dependent. Authoritative,
un-synthesized provenance is available on `queryBuckets`; that is where it belongs. This is
consistent with the Q5 stance (representation flags mean less on the synthesized sample path).

### Q9 — Column ordering / presence in `ColumnTable`. **RESOLVED**
**Order: (a) always sorted by PV name.** Deterministic, stable across pages, matches V1's
`TreeMap`-derived ordering, and it is the only well-defined order when the selector is
`pvNamePattern` / `metadataQuery` (which give no caller ordering to honor). One rule for all
selectors. (Rejected (b) "follow pvNameList order when explicit" — a conditional,
selector-dependent contract with two behaviors to document/test.)

**Presence: every resolved PV gets a column — even if entirely missing (all-unset) across the
page.** This makes both the column *set* and column *order* identical on every page, which is
what makes paginated sample results coherent (page 2's column *k* is the same PV as page 1's).

**Implementation consequence (a real change, not free):** today the sample path derives its
columns from `tableValueMap.getColumnNameList()` — i.e. from the buckets that came back — so a
PV with zero samples in range produces **no** column. To honor this decision the column set
must be **seeded from the resolved PV list up front**, and any PV with no data emitted as an
all-unset `DataValue` column of the correct length.

- **Resolution must produce the concrete PV name LIST, not just a Mongo filter.** For
  `pvNameList` the list is the set. For `pvNamePattern` / `metadataQuery`, resolution must
  materialize the matched PV **names** (so the assembler can seed all-missing columns for names
  that have no buckets in the time range). A naive implementation that flows the pattern
  straight into the retrieval filter would lose the names — avoid that.
- **Interaction with Q4 (non-scalar reject) — consistent, no conflict:** a PV with no buckets
  in range yields an all-unset column (its type is never observed, which is fine — unset
  values carry no type). Q4's non-scalar reject fires only for PVs that actually return a
  non-scalar bucket during assembly. The two rules do not contradict.

### Q10 — Regex safety and PV-set size bounds. **RESOLVED**

**Max resolved-PV-count cap (configurable, reject-when-exceeded, default TBD conservative).**
`pvNamePattern` / `metadataQuery` can resolve to thousands of PVs. Combined with Q9 ("every
resolved PV gets a column"), a broad selector produces an enormous column set — and note
**row/bucket paging (Q7) does NOT bound column count**: every sample page carries one column
per resolved PV, so a 50k-PV pattern puts 50k (mostly-empty) columns on *every* page. The cap
on resolved PV count is the protection against this blow-up (row paging cannot catch it).
Exceed → `ExceptionalResult` telling the caller to narrow the selector. New config key →
update BOTH `application.yml` files.

**Regex: catch-and-reject invalid patterns.** Validate that a `pvNamePattern` compiles;
`PatternSyntaxException` → clean `ExceptionalResult` reject. Put this in the shared validation
(Q6 extraction) so it applies to V2 and back-fills V1.

**This is a latent V1 bug we will fix, not just V2 hygiene (verified in code):**
`QueryHandlerUtility.validateQueryTableRequest` checks only that `pvNamePattern` is
**non-blank** — it never verifies the pattern compiles. The `queryTable` retrieval path
`Pattern.compile(...)` at `MongoSyncQueryClient.java:113` has **no try/catch**, so an invalid
pattern throws `PatternSyntaxException` in the handler worker thread instead of returning a
clean rejection. (Note `MongoSyncQueryClient.java:256` *does* guard a different path, so the
handling is inconsistent today.) Moving regex validation into shared code fixes the V1
`queryTable` path as a side benefit.

**Explicit regex-complexity/length guard — deferred (future hardening).** A
catastrophic-backtracking / overly-broad regex evaluated by Mongo against every PV name is a
ReDoS-flavored perf foot-gun, but a good complexity guard is hard to define. The max-resolved-
PV-count cap plus Mongo's own limits blunt the worst outcomes; note as possible future work,
not this phase.

### Q11 — `metadataQuery` PVs with no buckets: all-unset column, or drop? **RESOLVED**
**Decision: (b) — intersect metadata-matched names with archive existence.** A `metadataQuery`
resolves against the `pvMetadata` collection; the resulting name set is then intersected with
PV names that actually have buckets (one cheap `distinct` on the buckets collection over the
matched set, the `executeQueryPvExistence` primitive). "Resolved PV" therefore uniformly means
"a PV with data in the archive." Q9 stays clean (an all-unset column means the PV *has* buckets
but none *in range*); never-ingested PVs named only by a metadata record are dropped, avoiding
phantom empty columns from stale/typo'd metadata. Cost: one extra `distinct` during metadata
resolution. (Rejected (a) include-phantom-columns — surfaces metadata bugs as confusing empty
columns; (c) two-behavior split — unnecessary precision.)

**Original question (for context):**
The two `PvSelector` resolution sources have different reach (verified in code):
- `pvNameList` / `pvNamePattern` resolve against **PV names present in the buckets collection**
  (pattern via `distinct(pvName, regex)`), so a resolved PV always has *some* bucket somewhere.
- `metadataQuery` resolves against the **`pvMetadata` collection** — a metadata record can name
  a PV that was **never ingested** (no buckets at all in the archive).

Given Q9 ("every resolved PV gets a column, even if no data *in range*"), what about a PV with
**no buckets at all**?
  - **(a) Include an all-unset column** — literal Q9 ("you asked for the PV via metadata; here
    it is, empty"). Consistent rule, but a metadata typo / stale record yields phantom empty
    columns the caller may not expect.
  - **(b) Drop PVs with zero buckets in the archive** — resolution intersects the metadata-
    matched names with names that actually have buckets (a cheap `distinct` on the buckets
    collection over the matched set, like `executeQueryPvExistence`). `queryBuckets` returns
    only real buckets; `querySamples` only builds columns for PVs that could ever have data.
  - **(c) Distinguish "no buckets in range" (all-unset column, per Q9) from "no buckets at all"
    (drop)** — the most precise, but requires both the metadata match and the existence check,
    and two documented sub-behaviors.

**Recommendation:** **(b)** — intersect metadata-matched names with archive existence (one cheap
`distinct`), so "resolved PV" always means "a PV with data in the archive." Keeps Q9's rule
intact (all-unset = has buckets, none in range) and avoids phantom columns from metadata records
with no data. Cost: one extra `distinct` on the buckets collection during metadata resolution.
**Confirm.**

---

## 8. Testing plan

Follow existing structure (`QueryTestBase`, `QueryServiceImplTest`, integration tests under
`test/integration/.../query`). At minimum:

- Each of the 4 RPCs, unary and streaming.
- Each `PvSelector` form (list / pattern / metadata) and `ConfigurationSelector`
  (single/multiple criteria; interval union + intersection; fragmented ranges).
- Half-open boundary: `queryBuckets` whole boundary buckets vs. `querySamples` trimming.
- Union-axis alignment with multi-rate PVs and missing values (unset `DataValue`).
- Paging: multi-page unary continuation; empty `nextPageToken` on last page; streaming
  chunking with rejected non-empty `pageToken`.
- Validation rejections for every §6 invariant.
- `useSerializedColumns` for both bucket and sample results.
- `excludeColumnMetadata` include/suppress.
- Empty-result-is-empty-payload (not `ExceptionalResult`).
- (Pending Q4) non-scalar PV in a sample query.

Test-config note: per project convention, `src/test/resources/application.yml` shadows the
main config — update **both** if new config keys (e.g. V2 default page sizes) are added.

---

## 9. Proposed implementation order (single phase, internal sequencing)

All seven steps have detailed designs in §11 (cross-referenced below).

1. **Filter-logic extraction** (Q6) — enables PV-metadata / config resolution reuse. → **§11.1**
2. **Resolution/planner helper + resolved-query object** — PV + config + paging resolution, with validation (§6). → **§11.15**
3. **`queryBuckets` unary** — simplest formatter; establishes keyset paging (Q2) + `$or` fragmentation (Q3) end-to-end. → **§11.2**
4. **`queryBucketsStream`** — fire-and-consume variant of #3. → **§11.4**
5. **`querySamples` unary** — paging-aware assembly + timestamp paging (Q1); the substantive step. → **§11.3**
6. **`querySamplesStream`** — fire-and-consume variant of #5 (assemble-once, row-chunk). → **§11.5**
7. **Wiring + service impl + response helpers + tests** throughout. → **§11.6**

(Buckets first because sample paging (Q1) is the riskiest piece and benefits from the
retrieval/paging plumbing being proven on the simpler bucket path first. Step 1 is
self-contained and independently mergeable.)

---

## 10. Notes / assumptions

- dp-grpc 1.15.0 with V2 stubs is already the dp-service dependency (verified in the local
  `.m2` jar and `pom.xml`); no dp-grpc regeneration needed.
- V1 methods (`queryData*`, `queryTable`, stats, providers) remain unchanged for backward compat.
- Exception logging: follow the #191 convention — pass the exception object as the final
  `logger` argument to capture stack traces.
- "Dispatcher" in this subsystem means **result formatter** (cursor → response), not a router.

---

## 11. Detailed step designs

### 11.1 Step 1 — Extract shared filter-builder helpers (Q6)

**Goal:** one implementation of the criterion→`Bson` mapping, called by the V1 annotation
clients today and the V2 planner in step 2, with **byte-identical** behavior for the V1 paths.

#### 11.1.1 Exact current state (verified in code)

Two inline mappings in `MongoSyncAnnotationClient`:

- `executeQueryPvMetadata` (~L556-671): a `for`/`switch` over
  `QueryPvMetadataRequest.QueryPvMetadataCriterion` building `List<Bson> filterList`; arms:
  - `PVNAMECRITERION` (L564-582): exact→`in(field, exact)`; each prefix→`regex(field, "^"+Pattern.quote(p))`; each contains→`regex(field, ".*"+Pattern.quote(c)+".*")`; combine `size()==1 ? get(0) : or(list)`. Field = `BSON_KEY_PV_METADATA_PV_NAME` (`"pvName"`).
  - `ALIASESCRITERION` (L584-602): **identical** exact/prefix/contains logic, field = raw literal `"aliases"`.
  - `TAGSCRITERION` (L604-608): `in(BSON_KEY_TAGS, values)`.
  - `ATTRIBUTESCRITERION` (L610-619): `mapKey = BSON_KEY_ATTRIBUTES + "." + key`; empty values→`exists(mapKey)` else `in(mapKey, values)`.
  - empty `filterList` → `exists(BSON_KEY_PV_METADATA_PV_NAME)` (match-all guard); else `and(filterList)`.
- `executeQueryConfigurationActivations` (~L1102-1164): `for`/`switch` over the activation
  criterion; arms `TIMESTAMPCRITERION`, `TIMERANGECRITERION` (temporal — **V1-only**, not in
  V2 selector), `CONFIGURATIONNAMECRITERION`, `CLIENTACTIVATIONIDCRITERION`,
  `CATEGORYCRITERION` (field `BSON_KEY_ACTIVATION_INTERNAL_CATEGORY`), `TAGSCRITERION`,
  `ATTRIBUTESCRITERION` (raw literal `"attributes." + key`). Empty → `new Document()` (match-all).

Literals to promote to `BsonConstants`:
- `"aliases"` — **no constant exists.** Add `BSON_KEY_PV_METADATA_ALIASES = "aliases"`. Used in
  3 places in `executeQueryPvMetadata` (L589/592/596) **and** in `findPvMetadataByNameOrAlias`
  (L681) — update all four.
- `"attributes." + key` in the activation attributes arm (L1153/1155) → use
  `BSON_KEY_ATTRIBUTES + "." + key`. This is a **no-op byte change** (the metadata arm already
  builds the same string from the constant), which is exactly the behavior-preserving property
  the Q6 acceptance bar wants.

#### 11.1.2 Proposed helper API (neutral inputs, in `common`)

New class, e.g. `common/mongo/MongoQueryFilterBuilder.java` (package-visible statics; name TBD).
It must NOT reference any proto request type — callers unpack proto into neutral inputs:

```
// name/alias exact+prefix+contains → OR-combined single Bson, or null if all empty.
// prefix/contains are literal substrings (Pattern.quote applied inside); field is the Bson key.
static Bson nameMatchFilter(String field, List<String> exact, List<String> prefix, List<String> contains)

// tags: in(BSON_KEY_TAGS, values)   (caller passes the value list; empty list handling per current behavior)
static Bson tagsFilter(List<String> values)

// attribute key(+optional values): empty values → exists("attributes."+key), else in("attributes."+key, values)
static Bson attributeFilter(String key, List<String> values)
```

Note on `nameMatchFilter` returning `null` when all three lists are empty: preserves the
current "only add to filterList if nameFilters non-empty" behavior — callers do the
`if (f != null) filterList.add(f)` themselves (keeps the helper pure and side-effect-free).

The temporal arms (`TIMESTAMPCRITERION` / `TIMERANGECRITERION`) are **V1-only** (excluded from
the V2 `ConfigurationSelector` by proto design). Extract them too for consistency (so the whole
activation switch is uniform) but they are not called by V2:

```
static Bson activationContainsInstantFilter(Instant ts)      // lte(start,ts) AND (endMissing OR gt(end,ts))
static Bson activationOverlapsRangeFilter(Instant s, Instant e) // lt(start,e) AND (endMissing OR gt(end,s))
```

`activationOverlapsRangeFilter` is also the natural building block for the **V2 Q3 `$or`
fragment overlap** (each config sub-interval → one overlap predicate) — a bonus reuse that
argues for extracting it now.

#### 11.1.3 Rewrite of the two V1 clients

Each `switch` arm becomes: unpack the proto criterion → call the helper → add to `filterList`.
The empty-`filterList` match-all guards, the `and(filterList)`, pagination, sort, and result
marshalling **stay in the client unchanged**. Example (PvName arm):

```
case PVNAMECRITERION -> {
    final var c = criterion.getPvNameCriterion();
    final Bson f = MongoQueryFilterBuilder.nameMatchFilter(
            BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, c.getExactList(), c.getPrefixList(), c.getContainsList());
    if (f != null) filterList.add(f);
}
```

#### 11.1.4 Acceptance / verification (the Q6 bar)

1. Extraction is a pure move — V1 clients emit byte-identical `Bson`.
2. **Existing annotation tests (`PvMetadataIT`, `ConfigurationIT`, and any client/unit tests
   for these two queries) pass UNCHANGED.** If an existing test's *assertions* need to change,
   the extraction altered behavior — stop and investigate. (Mechanical relocation of a test is
   fine; assertion changes are the red flag.)
3. Add **new** unit tests for `MongoQueryFilterBuilder` directly (each helper, incl. the
   `null`-when-empty case, `Pattern.quote` escaping of regex-special substrings, single-vs-OR
   combination, attribute key-only vs key+values).
4. `BSON_KEY_PV_METADATA_ALIASES` added; all four `"aliases"` sites updated; activation
   attributes arm switched to the constant.

#### 11.1.5 Files touched

- **New:** `common/mongo/MongoQueryFilterBuilder.java` (+ its unit test).
- **Edit:** `common/bson/BsonConstants.java` (add `BSON_KEY_PV_METADATA_ALIASES`).
- **Edit:** `annotation/handler/mongo/client/MongoSyncAnnotationClient.java`
  (`executeQueryPvMetadata`, `executeQueryConfigurationActivations`,
  `findPvMetadataByNameOrAlias` alias-literal only).
- **Unchanged behavior; run:** `PvMetadataIT`, `ConfigurationIT`.

#### 11.1.6 Scope guard

This step does **not** touch the query subsystem or any V2 proto type — it is purely the
annotation-side extraction + constants. V2 wiring begins in step 2 (resolution) and consumes
these helpers. Keeping step 1 self-contained means it can merge independently and its
behavior-preserving property is trivially reviewable.

### 11.15 Step 2 — Resolution / planner + resolved-query object

**Goal:** turn a validated `QuerySpec + ExecutionOptions + ResultRepresentation` into an
internal resolved-query object that steps 3 and 5 consume. This is the sketch's "planner +
ExecutionPlan," realized as a helper + a plain object (no new package tree, per §2).

#### 11.15.1 The resolved-query object

New non-proto class, e.g. `query/handler/model/ResolvedQuery.java` (name TBD). Fields:

- `List<String> pvNames` — the **concrete, resolved PV name list** (Q9 requires the list, not a
  filter — see 11.15.2). Sorted ascending (Q9 column order derives from this).
- `List<Interval> retrievalIntervals` — effective, possibly-fragmented intervals (Q3); each a
  `(beginSecs,beginNanos,endSecs,endNanos)` half-open range. Single element = the whole
  `timeRange` when no `ConfigurationSelector`.
- `int pageSize` — normalized `limit` (Q7: default when 0, silently clamped to max).
- `KeysetPosition pageStart` — decoded from `pageToken`; null on first page. Shape differs by
  result mode: buckets = `(pvName, firstTimeSecs, firstTimeNanos)` (Q2); samples =
  `(epochSeconds, nanos)` resume-timestamp (Q1). Model as a small sealed/variant type or two
  fields; the codec (11.15.4) owns encoding.
- `boolean useSerializedColumns`, `boolean excludeColumnMetadata` (Q5/Q8).
- `ResultMode mode` (BUCKET | SAMPLE), `boolean streaming` — set by the calling handler, not
  parsed from the request.

Immutable; built by the resolver; carried into the `QueryV2Job` and dispatcher.

#### 11.15.2 PvSelector resolution → concrete name list (Q9)

Resolution MUST produce a concrete `List<String>` (not just a `Bson` filter), because Q9
requires a column for every resolved PV even when it has no buckets in range.

- **`pvNameList`** → the list verbatim (deduped, sorted). No DB round-trip.
- **`pvNamePattern`** → resolve against **PV names present in the buckets collection**:
  `mongoCollectionBuckets.distinct(BSON_KEY_PV_NAME, regex(BSON_KEY_PV_NAME, pattern))`. This is
  the cheap primitive already used by `executeQueryPvExistence` (L247-249) — `distinct` on the
  indexed `pvName`, no sort/group. Compile the regex with a **try/catch → reject** (Q10). Apply
  the resolved-PV-count cap (Q10) to the distinct result size.
- **`metadataQuery`** → resolve against the **`pvMetadata` collection** using the step-1 shared
  filter helpers (Q6), project the matching `pvName`s, **then intersect with archive existence**
  (Q11 decision (b)): `distinct(BSON_KEY_PV_NAME, in(pvName, matchedNames))` on the buckets
  collection — drop names that have no buckets at all. New client method
  `resolvePvNamesByMetadata(criteria) → List<String>` performs the metadata match + existence
  intersect. Apply the count cap to the final (intersected) set.

**Q11 (RESOLVED, §7):** metadata-matched names are intersected with archive existence, so
"resolved PV" always means "a PV with buckets in the archive." A resolved PV with no buckets
*in the query range* still gets an all-unset column (Q9); a PV named only by a metadata record
with no buckets at all is dropped.

#### 11.15.3 ConfigurationSelector resolution → intervals (Q3)

- No `ConfigurationSelector` → single interval = the whole `QuerySpec.timeRange`.
- Present with **no criteria** → matches nothing → **empty result** (per proto).
- Present with criteria → query `configurationActivations` with the step-1 shared filter helpers
  (non-temporal arms only: name, clientActivationId, category→`internalCategory`, tags,
  attributes), collect matching activations' `[startTime, endTime)` intervals (endTime null =
  open-ended → clamp to `timeRange.endTime`), **union** them, **intersect** with
  `timeRange` → the fragmented `retrievalIntervals`. Empty intersection → empty result.
- Interval math (union of possibly-overlapping activation intervals, then intersect with the
  query range) is pure Java on sorted `Instant` pairs — a small, unit-testable helper. Reuses
  `activationOverlapsRangeFilter` (step 1) only if we choose to pre-filter activations by the
  query range in Mongo (optional optimization; correctness comes from the Java intersect).

#### 11.15.4 Page-token codec (Q1/Q2/Q3 — opaque, position-only)

One small codec, e.g. `query/handler/paging/PageToken.java`:

- Encodes/decodes the `KeysetPosition` to/from an opaque Base64 string.
- **Meaning decoupled from encoding** (Q1 migration constraint): the token carries a position
  ("resume after bucket tuple" / "resume at timestamp"), never server state; a future cached-
  state impl (Q1 option b) can still accept it.
- **Position-only** (Q3): no fragment index — the compound `$or` keeps the global sort monotonic
  so a single position suffices across fragments.
- Malformed inbound token → `ExceptionalResult` reject (do not silently treat as first page).
- Streaming calls: non-empty inbound token → reject (Q7/proto); resolver enforces this given
  `streaming=true`.

#### 11.15.5 Validation placement (§6 invariants)

Single entry `resolveQueryV2(request, mode, streaming) → ResolvedQuery | ResultStatus error`,
called from `QueryServiceImpl` before delegating to the handler (mirrors V1's
validate-then-handle). Validates, in fail-fast order: PvSelector presence + exactly-one arm;
exactly-one-criterion per metadata/config criterion; TimeRange presence + `endTime > beginTime`;
paging (streaming-token rule, limit normalize/clamp); regex compile (Q10); then performs
resolution (which can itself reject: count cap, empty config match). Regex + criterion
validation live in the step-1 shared code where possible so V1 benefits (Q10).

#### 11.15.6 Files touched

- **New:** `query/handler/model/ResolvedQuery.java`, `query/handler/paging/PageToken.java`,
  interval-union/intersect helper (+ unit tests for token codec and interval math).
- **Edit:** `query/service/QueryServiceImpl.java` (`resolveQueryV2` entry + §6 validation).
- **Edit:** `MongoQueryClientInterface` / `MongoSyncQueryClient` (+ `resolvePvNamesByPattern`,
  `resolvePvNamesByMetadata`, `resolveConfigurationIntervals` — or a single
  `resolvePvNames`/`resolveIntervals` pair).
- **Consumes:** step-1 shared filter helpers.

#### 11.15.7 Scope note

Step 2 has **no user-visible RPC** — it is exercised only through steps 3/5. Its unit tests
(token codec, interval math, regex reject, count cap) stand alone; its resolution methods are
integration-tested via `queryBuckets` in step 3. This matches the sketch's "establish the
architecture before handlers" intent without a no-op phase.

### 11.2 Step 3 — `queryBuckets` unary (end-to-end paging + fragmentation + resolution)

**Why this is step 3 (not the sample path first):** buckets are discrete and the whole plumbing
we most fear — keyset paging (Q2), `$or` config fragmentation (Q3), selector resolution to a
concrete PV name list (Q9), the representation flags (Q5/Q8) — can be proven here on the
*simpler* formatter (whole buckets, no trimming, no union axis). `querySamples` (step 5) then
reuses the same resolution + retrieval + paging, changing only the formatter.

Depends on step 2 (resolution/planner + resolved-query object). This subsection assumes step 2
produced: a concrete PV **name list** (Q9), the effective retrieval **intervals** (Q3), the
normalized **page size** (Q7), the decoded **keyset position** (Q2), and the representation
flags. Where step 2's boundary is fuzzy, noted inline.

#### 11.2.1 gRPC entry + wiring (mirrors V1)

- `QueryServiceImpl.queryBuckets(QueryBucketsRequest, StreamObserver<QueryBucketsResponse>)`:
  log → `resolveQueryV2(...)` (validate + resolve; §6 invariants) → on error send
  `ExceptionalResult` (reject/error) and return → else `handler.handleQueryBuckets(resolved, obs)`.
- Add `handleQueryBuckets` to `QueryHandlerInterface`; `MongoQueryHandler` builds a
  `QueryBucketsUnaryDispatcher` + the shared `QueryV2Job` (§3.2 — one job, injected dispatcher),
  enqueues.
- Add V2 response helpers to `QueryServiceImpl`:
  `queryBucketsResponse{Reject,Error}`, `queryBucketsResponse(BucketQueryResult)`,
  `queryBucketsResponseEmpty` (empty `dataBuckets`, **not** an `ExceptionalResult` — empty is a
  normal payload).

#### 11.2.2 Retrieval — keyset + `$or` fragmentation (new client method)

New `MongoSyncQueryClient.executeQueryBucketsV2(resolvedQuery)` returning a bounded
`MongoCursor<BucketDocument>` (NOT the unbounded V1 cursor). Filter =

```
AND(
  pvNameFilter,                              // in(BSON_KEY_PV_NAME, resolvedPvNameList)
  $or( overlap(frag_i) for each interval ),  // Q3; single interval → no $or wrapper
  keysetSeek                                 // Q2; absent on first page
)
sort ascending (BSON_KEY_PV_NAME, BSON_KEY_BUCKET_FIRST_TIME_SECS, BSON_KEY_BUCKET_FIRST_TIME_NANOS)
.limit(pageSize + 1)                          // +1 probe row to detect next page (as annotation paging does)
```

- **`overlap(frag)`** is the **bucket** overlap predicate (`firstTime < fragEnd AND lastTime >=
  fragStart`), currently inlined in `executeBucketDocumentQuery`. **DECISION (binding, not
  optional): factor it into a shared `MongoQueryFilterBuilder.bucketOverlapsRangeFilter(interval)`
  helper and have BOTH V1 `executeBucketDocumentQuery` and the V2 `$or` build from it** — a single
  source for the predicate, consistent with the step-1 extraction rationale (real shared logic with
  a drift hazard, unlike the trivial exact-match `in(...)` arms which are deliberately left
  inline). Do NOT duplicate the two-line predicate.
  - **Caution — this is NOT `activationOverlapsRangeFilter`.** Step 1's
    `activationOverlapsRangeFilter` operates on the **activation** fields (`startTime`/`endTime`, the
    `configurationActivations` collection) and is consumed by step-2 *config interval resolution*.
    The bucket path needs a **separate** helper over the **bucket** fields
    (`BUCKET_FIRST_TIME_*`/`BUCKET_LAST_TIME_*`) with the V1 bucket semantics
    (`firstTime < end` strict, `lastTime >= start` inclusive). Same shape, different fields and
    different collection — keep them distinct helpers.
- **`keysetSeek`** — seek strictly after the last-emitted `(pvName, firstTimeSecs, firstTimeNanos)`
  tuple `(P, S, N)`, matching the compound sort. Lexicographic-tuple `>`:

  ```
  OR(
    gt(pvName, P),
    AND(eq(pvName, P), gt(firstTimeSecs, S)),
    AND(eq(pvName, P), eq(firstTimeSecs, S), gt(firstTimeNanos, N))
  )
  ```

  **Do NOT** seek on `_id > lastIdString` (Q2 note: string ordering of `pvName-secs-nanos`
  mis-orders numeric components). No tiebreaker needed — composite `_id` proves
  `(pvName, firstTime)` uniqueness (Q2).
- **`$or` wraps the whole fragment set; the keyset seek is ANDed at top level, NOT distributed
  into each `$or` branch** (Q3 correctness note).

**Index check — RESOLVED (verified in code, no new index needed):**
`MongoClientBase.createMongoIndexesBuckets()` (L174-179) already creates a compound index on
`(BSON_KEY_PV_NAME, BUCKET_FIRST_TIME_SECS, BUCKET_FIRST_TIME_NANOS, BUCKET_LAST_TIME_SECS,
BUCKET_LAST_TIME_NANOS)`. This is a **prefix-perfect** match for the V2 sort/seek (leading 3
columns) and also covers the overlap predicate's `firstTime <` / `lastTime >=` conditions.
Residual: the `$or`-of-fragment ranges rely on Mongo index-union across branches — the
favorable case (all branches are ranges within the same compound index), but **verify the
`$or` plan uses the index under realistic fragment counts** (this is the trigger for the
per-fragment-query fallback of Q3 if it degrades). Downgraded from blocker to load-time verify.

#### 11.2.3 Formatter — `QueryBucketsUnaryDispatcher`

Loop the cursor building `DataBucket`s (reuse/adapt `BucketDocument.dataBucketFromDocument`;
note its current `QuerySpec` param is **vestigial/unused** — the V2 variant should drop it and
instead take the representation flags). For each bucket:

- **Byte-budget page-ender (Q7):** track running serialized size; end the page before adding a
  bucket that would exceed the budget, but only after ≥1 bucket is in the page (zero-progress
  guard). A single bucket larger than the whole budget → `ExceptionalResult` (indivisible
  oversized; matches V1 `QueryDataStreamDispatcher`).
- **`limit` (count) page-ender (Q7):** the `pageSize + 1` fetch means: if the cursor yields a
  `pageSize+1`-th bucket, drop it and emit a `nextPageToken` from the last *kept* bucket's
  `(pvName, firstTimeSecs, firstTimeNanos)`. Whichever of count/byte fires first ends the page.
- **`nextPageToken` (Q2):** encode the last-emitted keyset tuple, opaque (Base64 of a small
  struct). Empty token == last page.
- Empty cursor → `queryBucketsResponseEmpty` (empty payload).

#### 11.2.4 Representation flags on the bucket path

- **`excludeColumnMetadata` (Q8):** default include is free (`addColumnToBucket` →
  `applyMetadataToProto` already carries metadata). For `true`, clear `metadata` on the emitted
  column. Thread the flag into the V2 `dataBucketFromDocument` variant (post-clear on the built
  `DataValues` column, or a metadata-suppressing build path).
- **`useSerializedColumns` (Q5) — bigger than it looks; needs a sub-decision.**
  Verified: `addColumnToBucket` emits the **stored typed form** (e.g. `setDoubleColumn`) and
  consults **no flag**; there is **no V1 precedent** for query-side emit-as-serialized (the V1
  `QueryDataRequest.QuerySpec` proto comment mentions such a flag but the field/impl never
  existed — the query subsystem only ever *passes through* columns that were **stored**
  serialized via `SerializedDataColumnDocument`). So:
  - **Stored-serialized column** → pass through as `SerializedDataColumn` (already works, free).
  - **Typed/scalar-stored column + `useSerializedColumns=true`** → **new work**: serialize the
    typed column to a `SerializedDataColumn` payload (pattern exists:
    `SerializedDataColumn.newBuilder().setName(...).setPayload(typedColumn.toByteString())`, cf.
    `ingest/benchmark/SerializedDataColumnBuilder` L42-45 and `DataColumnDocument.toSerializedDataColumn`).
  - **The `encoding` field defines a client contract.** A typed column serialized this way needs
    an `encoding` string telling the client how to parse the payload (e.g. `"proto:DoubleColumn"`).
    This is a **new client-facing convention** with no V1 precedent — the Python/Java client must
    know to decode per-encoding. **Open sub-decision:** (i) define and document the `encoding`
    scheme for each typed column now, or (ii) scope V2 `useSerializedColumns` on `queryBuckets`
    to **pass-through-only** for this phase (honor it for stored-serialized columns; for typed
    columns either ignore the flag and emit typed, or reject) and defer typed-serialization until
    a client actually needs it.
    **DECIDED: (ii) pass-through-only.** Honor `useSerializedColumns` for columns that were
    **stored** serialized (emit the stored `SerializedDataColumn`); for typed/scalar-stored
    columns the flag has **no effect** — emit the typed column as normal (do not fabricate an
    `encoding` scheme, do not reject). Document the limitation. Consistent with the Q5 "no
    speculative serialized machinery" stance; revisit with real usage. **No typed-column
    `encoding` contract is defined in this phase.**

#### 11.2.5 Validation (§6 invariants exercised here first)

`queryBuckets` is where the shared `resolveQueryV2` validation lands first: PvSelector presence
+ exactly-one selector arm; exactly-one-criterion per metadata/config criterion; TimeRange
presence + `endTime > beginTime`; streaming-token rule N/A (unary); regex compile-check (Q10,
via step-1 shared validation); resolved-PV-count cap (Q10). Empty-result-is-empty-payload.

#### 11.2.6 Files touched

- **Edit:** `query/service/QueryServiceImpl.java` (override `queryBuckets`, V2 response helpers,
  `resolveQueryV2` entry).
- **Edit:** `query/handler/interfaces/QueryHandlerInterface.java` (+ `handleQueryBuckets`).
- **Edit:** `query/handler/mongo/MongoQueryHandler.java` (build dispatcher + `QueryV2Job`, enqueue).
- **New:** `query/handler/mongo/job/QueryV2Job.java` (shared; injected dispatcher).
- **New:** `query/handler/mongo/dispatch/QueryBucketsUnaryDispatcher.java`.
- **Edit:** `query/handler/mongo/client/MongoSyncQueryClient.java` (+ `executeQueryBucketsV2`;
  optional single-interval overlap-predicate helper).
- **Edit/New:** V2 `dataBucketFromDocument` variant taking representation flags (drop vestigial
  `QuerySpec` param).
- **New:** resolution/planner helper + resolved-query object (from step 2; this step consumes it).
- **Tests:** `queryBuckets` unary — happy path, keyset multi-page continuation + empty last-page
  token, `$or` fragmentation (multiple config sub-intervals), whole-boundary-buckets (no
  trimming), byte-budget page split + indivisible-oversized error, all §6 rejections,
  `excludeColumnMetadata` include/suppress, `useSerializedColumns` pass-through (per 11.2.4 (ii)).

#### 11.2.7 Open sub-tasks surfaced by this step

1. ~~Confirm buckets-collection index coverage~~ **DONE** — existing compound index
   (`MongoClientBase` L174-179) is a prefix-perfect match; no new index. Remaining: load-time
   verify that the `$or`-of-fragment plan uses the index (per-fragment fallback trigger).
2. ~~Decide `useSerializedColumns` scope on `queryBuckets`~~ **DONE** — pass-through-only
   (11.2.4): honor for stored-serialized columns; no effect for typed columns; no `encoding`
   contract this phase.
3. ~~Confirm the step-2 boundary~~ **DONE** — step 2 (11.15.1/11.15.2) produces the concrete
   sorted PV **name list** in `ResolvedQuery.pvNames`; step 3 uses it as the `in(...)` filter,
   step 5 uses it to seed all columns (Q9).

### 11.3 Step 5 — `querySamples` unary (timestamp paging + column seeding + non-scalar reject)

**The substantive step.** Reuses step 2 resolution wholesale; the retrieval overlap query and
the trimming + sparse-fill assembly already exist (`TabularDataUtility`,
`QueryTableDispatcher.columnTableResultFromMap`). The genuinely new work is a **paging-aware
assembly loop** (Q1), **column seeding from the resolved PV list** (Q9), the **non-scalar
reject** (Q4), and the representation flags (Q5/Q8).

#### 11.3.1 Why the existing assembly must change (verified in code)

`TabularDataUtility.addBucketsToTable` (L22-49) **drains the entire cursor** into a
`TimestampDataMap` and treats the size limit as an **error trigger**
(`sizeLimitExceeded=true` → `QueryTableDispatcher` errors). Both behaviors are incompatible
with V2:

- **Cursor order vs. paging axis.** Buckets arrive sorted `(pvName, firstTime)`, **not** by
  timestamp. A single union timestamp T receives contributions from buckets scattered
  throughout the cursor, so "row T is complete" is not known until the cursor is drained **for
  the page's time window**. Therefore you **cannot** stop mid-cursor at `pageSize` rows — the
  page's timestamp set must be bounded by the **re-query window**, not by early cursor exit.
  This is precisely why Q1 chose timestamp-advanced re-query: it is not merely the token design,
  it dictates the loop.
- **Size limit must page, not error (Q7).** Replace "drain + error if > limit" with "assemble a
  bounded page + emit token."

`TimestampDataMap` is a nested sorted map (`second → nano → {colIndex→value}`), so distinct
timestamps are naturally time-ordered and countable; the **trimming** predicate
(`addColumnsToTable` L116-121, half-open `[begin,end)`) and the **sparse-fill**
(`columnTableResultFromMap`, unset `DataValue` for missing) are reused verbatim.

#### 11.3.2 The paging-aware assembly loop (Q1)

Per unary call (one page):

1. Resolve (step 2) → PV list, intervals, `pageStart` timestamp (from token; = `timeRange.begin`
   on first page), `pageSize`, flags.
2. Run the overlap query for the **page window** — `[pageStart, endTime)` intersected with the
   config `$or` fragments (Q3) — over the resolved PV list.
3. Assemble into a `TimestampDataMap` with trimming to `[pageStart, endTime)`.
4. **Bound the page to ~`pageSize` distinct timestamps (soft cap, never split a timestamp —
   Q1)** and to the byte budget (Q7). See 11.3.3 for *how* the window is bounded — this is the
   real sub-decision.
5. `nextPageToken` = the timestamp **immediately after** the last row emitted (so the next page's
   `[pageStart', endTime)` resumes with no gap and no overlap). Empty token = last page.

#### 11.3.3 Window-sizing: how to land ~`pageSize` rows — **DECIDED: (A) drain-then-truncate**

Because a page window must be drained fully before rows are final, we cannot exactly hit
`pageSize`. Options:

- **(A) Drain-then-truncate.** Query `[pageStart, endTime)` with a **bucket** `limit` heuristic
  (fetch enough buckets to likely cover ≥ `pageSize` timestamps), assemble, then keep the first
  `pageSize` distinct timestamps and set the token to the next one. Risk: hard to pick a bucket
  count that yields ≥ `pageSize` timestamps for irregular/multi-rate PVs; may under- or
  over-fetch.
- **(B) Assemble-until-count, then finalize the boundary timestamp.** Stream the cursor
  accumulating into the map; once the map holds `pageSize` distinct timestamps, note the largest
  timestamp Tb, then **keep draining only buckets whose window still overlaps Tb** to complete
  row Tb (soft cap: never split Tb), and stop. Token = timestamp after Tb. More precise; needs
  care because later buckets in `(pvName,firstTime)` order can still contribute to Tb.
- **(C) Time-slice heuristic.** Advance `pageStart` by an estimated Δt (from average sample
  rate) to bound the window, drain fully, truncate to `pageSize`. Simple query, but Δt
  estimation is fragile.

**DECIDED: (A) drain-then-truncate.** Bucket-fetch heuristic *plus* byte budget as the two
stop conditions; truncate to `pageSize` distinct timestamps for the soft cap. The query stays
simple (a bounded overlap query) and correctness comes from **truncation**, not from a clever
cursor exit; the only cost is occasional over-fetch, which is exactly the acceptable-per-Q1
boundary re-read cost, made explicit. (Rejected (B) assemble-until-count — the "later buckets
in `(pvName,firstTime)` order still contribute to the boundary row Tb" hazard makes it an
off-by-one prone to silently dropping samples at page seams; (C) Δt time-slice — fragile rate
estimation.)

#### 11.3.4 Column seeding from the resolved PV list (Q9)

Today the table's columns come from `tableValueMap.getColumnNameList()` — i.e. only PVs that
produced buckets. For V2, **seed the column set from `ResolvedQuery.pvNames` up front** so every
resolved PV gets a column (sorted by name) even if it contributes no samples in the page —
its column is all-unset `DataValue`s of the page's row count. `columnTableResultFromMap` already
substitutes an empty `DataValue` for a missing `(timestamp, column)` cell, so seeding the column
**index map** with all resolved names (not just seen ones) is the change. (Q11 guarantees every
resolved name has buckets *somewhere*, so an all-unset column means "no data in this page's
window," which is meaningful.)

#### 11.3.5 Non-scalar reject (Q4)

`addBucketToTable` throws `DpException` on the first non-scalar column. The V2 sample dispatcher
**catches** it and returns an `ExceptionalResult` naming the offending PV (per Q4: enrich the
shared exception with the PV name; translate to a `querySamples`-specific message pointing to
`queryBuckets`). No partial table. (Early pre-retrieval reject is the deferred Q4 ticket.)

#### 11.3.6 Output — V2 `ColumnTable` + representation flags

Evolve `columnTableResultFromMap` to emit the **V2** `ColumnTable` (`dp.service.query.ColumnTable`:
`TimestampList` + `DataColumn` per PV), **dropping** the V1 row-map format and the V1
`QueryTableResponse.ColumnTable` wrapper.

- **`useSerializedColumns` (Q5):** build the `DataColumn`s, then serialize each into
  `serializedDataColumns` (populate **exactly one** of the two lists), empty `encoding`, preserve
  unset-oneof missing values. No perf claim (Q5).
- **`excludeColumnMetadata` (Q8):** **inert** — `querySamples` carries no column metadata
  (`toDataColumn()` already omits it). No work; document.

#### 11.3.7 Wiring + files touched

- **Edit:** `QueryServiceImpl` (override `querySamples`; V2 `querySamplesResponse{Reject,Error,
  Empty}` + success helpers; reuse `resolveQueryV2` with `mode=SAMPLE`).
- **Edit:** `QueryHandlerInterface` (+ `handleQuerySamples`); `MongoQueryHandler` (build
  `QuerySamplesUnaryDispatcher` + shared `QueryV2Job`, enqueue).
- **New:** `query/handler/mongo/dispatch/QuerySamplesUnaryDispatcher.java` (the paging-aware
  assembly loop 11.3.2, column seeding, non-scalar catch, V2 `ColumnTable` output).
- **Edit:** `MongoSyncQueryClient` — `executeQuerySamplesV2(resolvedQuery, pageWindow)` (bounded
  overlap query over the page window + `$or` fragments; the bucket-fetch heuristic of 11.3.3(A)).
- **Refactor (shared):** a paging-aware variant of `TabularDataUtility.addBucketsToTable` that
  returns after a bounded page rather than erroring — or a new method alongside it, leaving the
  export-framework caller (`addBucketsToTable`) untouched (it has no paging need). Prefer a
  **new** method so the export path is unaffected (behavior-preserving, like step 1).
- **New:** V2 `ColumnTable` builder (evolved `columnTableResultFromMap`); the V1 one stays for
  `queryTable`.
- **Tests:** union-axis alignment with multi-rate PVs + missing values; trimming to `[begin,end)`
  (boundary samples dropped, contrast with step-3 whole buckets); multi-page timestamp
  continuation (no gap/overlap at page seam; empty token last page); byte-budget page split;
  every-resolved-PV-gets-a-column incl. all-unset column; non-scalar reject naming the PV;
  `useSerializedColumns` re-serialization; empty-result-is-empty-`ColumnTable`.

#### 11.3.8 Open sub-tasks surfaced by this step

1. ~~Window-sizing strategy~~ **DONE** — (A) drain-then-truncate (11.3.3).
2. **Page-seam correctness — TEST-ENFORCED INVARIANT (decided).** The `nextPageToken` =
   "timestamp immediately after the last emitted row" must resume the next window with **no
   dropped or duplicated timestamp** at the seam. This is treated as an invariant enforced by a
   **mandatory dedicated multi-page test**, including the edge case of a union timestamp landing
   exactly on a page boundary (and the multi-rate case where different PVs' samples straddle the
   seam). Not spelled out further in the plan by decision — the test is the specification.
3. **Shared-utility factoring** — new paging-aware assembly method vs. parameterizing the
   existing one; keep the export caller behavior-preserving either way.

### 11.4 Step 4 — `queryBucketsStream` (server-streaming buckets)

Fire-and-consume variant of step 3. **RPC shape:** `queryBucketsStream(QueryBucketsRequest)
returns (stream QueryBucketsResponse)` — plain server-streaming (single request → response
stream), so wiring mirrors V1 `queryDataStream` (NOT the bidi cursor path; no request observer).

**Deltas over step 3 (unary buckets):**

- **No paging token; `limit` = per-message chunk size (proto).** Reuse step-3 retrieval
  (`executeQueryBucketsV2`) but with **no keyset seek and no `pageSize+1` probe** — stream the
  full result of the (resolved intervals × PV list) overlap query to exhaustion.
- **Chunking loop** mirrors `QueryDataStreamDispatcher`: accumulate `DataBucket`s into a
  `BucketQueryResult` builder; when adding the next bucket would exceed **min(limit-as-count,
  byte budget)**, flush the current message (`onNext`) and start a new one; `onCompleted()` at
  cursor end. Empty result → one empty `BucketQueryResult` message, then complete.
- **`nextPageToken` empty on every message** (the stream signals completion — proto).
- **Reject non-empty inbound `pageToken`** with an `ExceptionalResult` (Q7/§6) — enforced in
  `resolveQueryV2` given `streaming=true`; do **not** silently return the first page.
- **Indivisible-oversized bucket** still errors (Q7), same as step 3 / V1.
- Representation flags (Q5 pass-through / Q8 include-or-suppress) identical to step 3.

**New:** `QueryBucketsStreamDispatcher` (chunking loop; shares the `DataBucket`-building and
flag handling with the unary dispatcher — factor the per-bucket build into a shared helper so
the two dispatchers differ only in accumulate-and-flush vs. accumulate-one-page). Wire
`queryBucketsStream` in `QueryServiceImpl` + `handleQueryBucketsStream` + `MongoQueryHandler`
(build stream dispatcher + shared `QueryV2Job`).

### 11.5 Step 6 — `querySamplesStream` (server-streaming samples)

Fire-and-consume variant of step 5. Same plain server-streaming shape as step 4.

**Deltas over step 5 (unary samples):**

- **`limit` = timestamps (rows) per streamed message (proto).** No resumable token.
- **Streaming avoids the step-5 window-sizing problem (11.3.3) entirely.** Because streaming is
  fire-and-consume with no resumable boundary, the server can run the **single** full overlap
  query for the whole `[timeRange]` × config-`$or` × PV list, assemble the complete union table
  once (as V1 `queryTable` does today), and **emit it in row-chunks of `limit` timestamps** —
  slicing an already-assembled, timestamp-ordered `TimestampDataMap` at row boundaries. No
  per-page re-query, no drain-then-truncate, no seam token. The Q1 machinery is a *unary*
  concern only.
  - **Memory caveat:** this materializes the full table server-side (like V1 `queryTable`),
    bounded by available heap, not by the per-message byte limit. For very large ranges the
    unary `querySamples` (bounded-memory, resumable) is the intended path — matches the proto's
    "use unary when resumable/bounded is required." Chunk emission itself still respects the
    per-message byte budget (flush a chunk before it exceeds the wire limit, even if < `limit`
    rows).
  - **Alternative (deferred):** a streaming version of the drain-and-slice that bounds memory by
    re-querying windows like unary does. Not needed for phase 1; note it if the full-materialize
    memory profile proves problematic.
- **Column seeding (Q9)** identical to step 5: seed columns from the resolved PV list; all-unset
  where absent — computed **once** for the whole table, so the column set is trivially stable
  across streamed chunks (no cross-page concern).
- **Non-scalar reject (Q4)** identical: catch `DpException` during assembly → `ExceptionalResult`.
- **Reject non-empty inbound `pageToken`** (Q7/§6). Empty `nextPageToken` per message.
- Output `ColumnTable` + flag handling (Q5/Q8) identical to step 5.
- **Row-slice seam:** each streamed `ColumnTable` carries its own `TimestampList` slice + the
  aligned column slices; a PV's `DataColumn` is split at the same row boundary as the timestamp
  axis. Simpler than the unary seam (no token, no re-query) but the slice alignment is still a
  test target.

**New:** `QuerySamplesStreamDispatcher` (assemble-once, then row-chunk emit). Wire
`querySamplesStream` in `QueryServiceImpl` + `handleQuerySamplesStream` + `MongoQueryHandler`.

### 11.6 Step 7 — Final wiring, cross-cutting, and cleanup

Not a separate feature — the connective tissue threaded through steps 3–6, collected here so
nothing is missed:

- **`QueryServiceImpl`:** all four V2 overrides wired; the V2 response-builder helper family
  complete (`queryBuckets*` / `querySamples*` reject/error/empty/success); `resolveQueryV2`
  shared by all four with `mode` + `streaming` flags driving the paging-token rule (Q7) and the
  result mode.
- **`QueryHandlerInterface` / `MongoQueryHandler`:** four `handleQuery{Buckets,Samples}[Stream]`
  methods; each builds the matching dispatcher + the shared `QueryV2Job`, enqueues. Confirm the
  single `QueryV2Job` cleanly serves all four via injected dispatcher (mirrors how `QueryDataJob`
  serves unary/stream/bidi) — if the sample-vs-bucket retrieval signatures diverge too far, split
  into `QueryBucketsV2Job` / `QuerySamplesV2Job` rather than force one.
- **Config keys (Q7): DONE.** `QueryHandler.queryV2DefaultPageSize` (10 000),
  `queryV2MaxPageSize` (100 000), `queryV2MaxResolvedPvCount` (10 000) added to **both**
  `application.yml` files (shadowing convention). A single page-size setting serves buckets,
  sample rows, and streaming chunk size (the `limit` semantics differ per RPC but the bound is
  uniform); the finalized numbers are the conservative phase-1 values — tune from real usage.
- **Shared exception enrichment (Q4):** land the PV-name-carrying, neutrally-worded `DpException`
  change once, consumed by both sample dispatchers.
- **`QueryTestBase` / `GrpcIntegrationQueryServiceWrapper`:** add V2 request builders,
  `*ResponseObserver` inner classes (CountDownLatch pattern), and `sendAndVerify*` helpers for
  the four RPCs (unary + streaming), mirroring the annotation test framework.
- **Exception logging (project convention #191):** all new catch sites pass the exception object
  as the final `logger` arg (stack trace).
- **Follow-up tickets filed** (from §0): keyset paging standardization; early non-scalar reject;
  usage-gated serialized-columns revisit.
- **Docs:** update `CLAUDE.md` (or the query subsystem notes) with the V2 method pattern and the
  "Dispatcher = formatter" reminder once the shape is final.

**Deliberately NOT in scope (restated):** `SampleStatusSelector` (reserved proto field 4); V1
methods unchanged; V1 row-map table format not carried to V2; typed-column serialized `encoding`
scheme (Q5); early pre-retrieval non-scalar reject (Q4 ticket); regex-complexity guard (Q10).
