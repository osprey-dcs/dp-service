# Plan: service handling for the modernized DataSets and Annotations APIs (issue #248)

- **Ticket**: [osprey-dcs/dp-service#248](https://github.com/osprey-dcs/dp-service/issues/248)
- **Proto change**: [dp-grpc#132](https://github.com/osprey-dcs/dp-grpc/issues/132), merged as
  [dp-grpc#145](https://github.com/osprey-dcs/dp-grpc/pull/145)
- **Upstream working document**: [`plan/tickets/132/dp-service-handoff.md`](https://github.com/osprey-dcs/dp-grpc/blob/main/plan/tickets/132/dp-service-handoff.md)
  — 13 sections of verified change sites. This plan does not restate it; it records where triage
  **disagrees** with it and how the work is sequenced.
- **Companion, already landed**: #252 / PR #253 removed `DataValue.ValueStatus` (dp-grpc #143), the
  other half of the same breaking release.
- **Supersedes**: #210, #211, #214.
- **Status**: triaged 2026-09-01 against dp-service `7be9a05` and dp-grpc `6dfff3f`; re-verified
  2026-09-02 against the merged protos and `main` at `2ec58a8` — #254 merged as PR #255, so Phase 1
  is **unblocked**. Phase 1 merged 2026-09-04 as PR #256. Phase 2 planned 2026-09-04 against `main`
  at `04e8902` (see [Phase 2 planning](#phase-2-planning-2026-09-04)) and implemented on
  `issue-248-phase-2-entity-audit-crud`; merged 2026-09-06 as PR #261. Phase 3 planned 2026-09-07
  against `main` at `d5c5928` (see [Phase 3 planning](#phase-3-planning-2026-09-07)); merged
  2026-09-08 as PR #263. Phase 4 planned 2026-09-08 against `main` at `7f6bfdf` (see
  [Phase 4 planning](#phase-4-planning-2026-09-08)).

## Overview

dp-grpc #132 modernized the DataSets and Annotations APIs — the oldest generation in
`DpAnnotationService` — bringing them to the conventions established by PV metadata, machine
configuration, and sample status. This ticket implements the service side.

It is also, right now, **the only thing keeping `main` from compiling**. Until it lands no PR in
this repo can show a green build, and no integration test runs anywhere in CI.

## Background / triage findings

Five findings change the work. Two contradict the upstream handoff; one contradicts this plan's
own first draft.

### 1. The compile break is 58 errors across 8 files, not the 5 that CI reports

CI, and a plain `mvn compile`, report five errors — all `QueryAnnotationsResponse.AnnotationsResult.Annotation`:

```
AnnotationClient.java:[243,70] / [268,63]
QueryAnnotationsApiResult.java:[10,65] / [22,85]
AnnotationDocument.java:[115,54]
```

**That count is an artifact of how javac fails, not a measure of the work.** When a *type*
reference cannot be resolved, attribution stops for the dependent code, so method-resolution
errors behind it are never reported. Raising `-Dmaven.compiler.maxerrs` does not help — this is
not the error cap.

Resolving just those five type references exposes **58 errors across 8 files**:

| File | Errors | Cause |
|---|---|---|
| `common/bson/dataset/DataSetDocument.java` | 12 | flat `SaveDataSetRequest` |
| `client/AnnotationClient.java` | 11 | criterion accessors, `comment` |
| `annotation/service/AnnotationServiceImpl.java` | 10 | criterion accessors |
| `annotation/handler/mongo/client/MongoSyncAnnotationClient.java` | 9 | criterion accessors |
| `annotation/handler/AnnotationValidationUtility.java` | 9 | `CalculationsDataFrame.frame` |
| `common/bson/calculations/CalculationsDataFrameDocument.java` | 4 | `CalculationsDataFrame.frame` |
| `annotation/handler/mongo/MongoAnnotationHandler.java` | 2 | criterion accessors |
| `annotation/handler/mongo/job/SaveDataSetJob.java` | 1 | flat `SaveDataSetRequest` |

Mechanically fixing two of those files raises the count again, to 88, as attribution reaches
further still. **The true surface is only knowable once everything compiles**, so treat every count
here as a lower bound, and expect the test sources — which no compile has yet reached — to add
more. Do not plan around the reported error count.

### 2. `common.DataFrame` keeps `dataColumns` — typed columns are additive, not forced

The first draft of this plan assumed `Calculations.CalculationsDataFrame` moving to
`common.DataFrame` forced the typed-column rewrite, and scoped it as the largest chunk. Verified
against the generated stubs, that is **wrong**:

```
common.DataFrame:
  hasDataTimestamps() / getDataTimestamps()      <- still present
  getDataColumnsList()                            <- still present (legacy escape hatch)
  getDoubleColumnsList() ... getBoolArrayColumnsList()   <- 16 typed accessors, additive
```

The proto describes `dataColumns` as the deliberate escape hatch "for heterogeneously typed columns
or columns with missing values." So the 13 errors in `CalculationsDataFrameDocument` and
`AnnotationValidationUtility` are **one level of indirection** — `frame.getX()` becomes
`frame.getFrame().getX()` — not a storage rewrite.

Supporting typed calculation columns end-to-end (and the export work that depends on it) remains
real work, but it is **additive and independently schedulable**, not a prerequisite for
compilation. This is the single biggest correction to the ticket's sizing.

### 3. The handoff's "preserve that behavior" for empty criteria is now wrong

The handoff (§2.1, written 2026-08-27) says of `executeQueryDataSets` / `executeQueryAnnotations`:

> Both currently return `null` when no criteria are supplied, which the callers treat as a
> rejection — preserve that behavior.

The merged proto says the opposite, for both queries:

> An empty criteria list matches all DataSets.  — `annotation.proto:928`, and `:1471` for Annotations

The proto wins, per this ticket's own "where this ticket and the merged protos disagree, the protos
win." The handoff simply predates #245, which settled exactly this question for the other three
annotation queries and merged as #251 on 2026-09-01.

So the two `return null` blocks — `MongoSyncAnnotationClient:258-262` (datasets) and `:502-506`
(annotations) — are **removed**, not preserved, and the two service-layer emptiness checks
(`AnnotationServiceImpl:217-220` and the annotations equivalent) are deleted — exactly as #245 deleted the three in `Query*Job`.
Carry the same explanatory comment those three now have, since an absent validation block reads
like an omission.

`DEFAULT_QUERY_LIMIT` (`MongoSyncAnnotationClient:76`, already shared by the three metadata
queries) extends to these two. Per #245's rationale the default is **unconditional** — never applied
only when criteria are absent, or a client removing its last filter silently changes page size.

### 4. Two fall-through reject paths in the service layer — one live, one dead code

Independent of the proto change, and the same defect the handoff flagged in the dispatcher:

- `AnnotationServiceImpl:127` — `saveDataSet` calls `sendSaveDataSetResponseReject(...)` **without
  `return`** when `request.getDataSet()` is null. But that guard is dead code: protobuf message
  getters never return null, so the branch cannot fire — today an unset `dataSet` is caught, with a
  misleading message, by `validateDataSet`'s blank-name check. This plan's first draft prescribed
  "add the missing `return`", which would have preserved permanently unreachable code. The block is
  deleted in Phase 1 regardless — the flat `SaveDataSetRequest` has no `dataSet` field — and what
  it leaves behind is the live requirement: the rewritten flat-request validation must `return`
  after every reject. (`AnnotationValidationUtility.validateDataSet:17,23` carries the same
  misapprehension — null checks on string getters, which also never return null; the `isBlank()`
  disjuncts are what actually work.)
- `AnnotationServiceImpl:218` — `queryDataSets` rejects on its empty-criteria check without
  `return`, then enqueues the job anyway: a genuine live fall-through, producing a second response
  on a closed observer. It disappears with the empty-criteria removal (§3).

These are the only two such sites in the file — every other reject in both validation switches has
its `return`. Neither is caught today because no test exercises the path past the first response.

The dispatcher has three more of the same shape (`QueryAnnotationsDispatcher:68`, `:83`, `:96`);
those disappear with the denormalization removal in Phase 1.

### 5. Nearly every criterion changed shape — validation switches are rewrites

Most criterion fields went singular → repeated, and three new criterion types were added to each
query. Two rows below are corrections over this plan's first draft, verified against the merged
proto: `TextCriterion` did **not** change shape, and the exact/prefix/contains lists belong to the
new `NameCriterion`, not to `PvNameCriterion` (which is DataSets-only):

| | Old | New |
|---|---|---|
| `IdCriterion` | `string id` | `repeated string ids` |
| `OwnerCriterion` | `string ownerId` | `repeated string ownerIds` |
| `TextCriterion` | `string text` | **unchanged** — still `string text`, both queries |
| `PvNameCriterion` (DataSets only) | `string name` | `repeated string names` |
| `DataSetsCriterion` | `string dataSetId` | `repeated string dataSetIds` |
| `AnnotationsCriterion` | `string annotationId` | `repeated string annotationIds` |
| `NameCriterion` (exact / prefix / contains lists), `TagsCriterion`, `AttributesCriterion` | — | new on both queries |

The `isBlank()`-per-criterion validation in `AnnotationServiceImpl` (~60 lines for datasets, ~90 for
annotations) is rewritten wholesale. The target shape already exists: the PV-metadata and
configuration validators handle exactly these repeated types, including the `isBlankKey()` guard
from #243. Copy those rather than adapting the legacy switches.

**The #243 invariant applies to `NameCriterion`'s `prefix` / `contains` lists** (on both queries;
`PvNameCriterion` has no prefix/contains). A blank string there builds
`"^" + Pattern.quote("")`, which matches everything — a silent match-all wearing the appearance of a
filter. Client-side, `AnnotationClient.nonBlank()` is the single source for this and must guard the
new dataset/annotation criterion builders too.

## Design decisions

**D1 — Phase 1 is "make it compile", and it is allowed to be mechanical.**
The repo currently has no CI signal at all. Restoring compilation is worth its own PR even though it
delivers no new capability, because every later phase is unverifiable without it. Phase 1
deliberately does *not* add RPCs, opaque page tokens, or typed columns. It does carry mechanical
skip-based paging for the two rewritten queries — D10 explains why leaving them limit-capped but
unpageable would be worse.

**D2 — Behavior changes forced by the proto go in Phase 1; optional ones do not.**
Some changes cannot be deferred: the `Annotation` message has no `dataSets` field, so the embedding
must go, and with it the N+1 fetch. Others *can* be deferred and are: opaque page tokens, new
RPCs, typed columns. The test is "does `main` compile without it," not "is it in the same handoff section."

**D3 — `comment` → `description` renames storage, not just the proto boundary.**
Decided by the ticket owner on 2026-09-01, choosing the deeper of two options. The BSON field, the
`BsonConstants` key, and the compound text index in `MongoClientBase` all move to `description`. The
alternative — mapping only at the proto boundary and leaving storage as `comment` — was rejected
because it leaves the schema permanently inconsistent with the API for no lasting benefit.

This makes Phase 1 carry a **data migration**; see [Upgrade note](#upgrade-note-required-for-d3).

**D4 — AND-combining criteria is deferred to Phase 3, not folded into Phase 1.**
Phase 1 must touch every criterion site anyway, so folding in the AND semantics is tempting. Rejected:
the handoff calls it "the highest-risk change" precisely because it is silent — two `TagsCriterion`
entries currently OR, and would afterward AND, with no error either way. It deserves a PR whose diff
is exactly that change, with tests written against it, not a line buried in a 58-error mechanical
sweep. Phase 1 preserves today's two-bucket semantics verbatim.

**D5 — Reuse `DEFAULT_QUERY_LIMIT`; do not add a config key.**
#245 established the constant and documented why it is shared across call sites: so a change to the
default cannot land on some queries and not others. Extending it to datasets and annotations keeps
that property. A config pair (`AnnotationHandler.metadataQueryDefaultPageSize` / `…MaxPageSize`)
covering all five queries is the shape to adopt *if* one is wanted later — and note
`src/test/resources/application.yml` shadows the main file, so both need the key.

**D6 — Opaque page tokens for datasets/annotations only.**
The other three queries use Base64 skip offsets, shipped and settled by #245 days ago. Converting all
five in the same PR that introduces five new RPCs changes the paging contract of three queries whose
behavior was just stabilized. Convert the two being modernized; file a follow-on for the rest.

**D7 — `deleteDataSet` rejection names one referencing annotation id plus a total count.**
One id is enough to act on; the count tells the caller whether to expect more; the message stays
bounded regardless of how many annotations reference the dataset.

**D8 — Fix orphaned Calculations forward only.**
`SaveAnnotationJob` gains a delete path for a replaced annotation's previous calculations. Whether to
sweep documents already orphaned in existing deployments is a deployment question, not a code one —
file separately if any deployment is known to have them. *Moved from Phase 4 into Phase 2 during
Phase 2 planning (D14): `deleteAnnotation` builds the calculations-delete machinery anyway.*

**D9 — the annotations text index drops `event.description` in Phase 1, inside the no-migration window.**
`MongoClientBase:275` still includes `BSON_KEY_EVENT_DESCRIPTION` ("event.description") in the
annotations text index — a field `AnnotationDocument` has never had, left over from the
`eventMetadata` feature now removed from the protos entirely. The merged proto's `TextCriterion`
doc names the indexed fields as `name` and `description` for both queries, matching the dataSets
index; the ticket states the same target. Timing matters: the version-1 migration has never shipped
in a release (latest is 1.15.0, pre-#254), so changing the replacement index now costs nothing —
the migration identifies the index it drops by `comment` in `weights`, which this does not touch.
Once a release ships v1, the same change needs a version-2 drop-and-recreate migration. Sites:
`MongoClientBase:275` and the `createNewTextIndex()` fixture in
`V1AnnotationCommentToDescriptionTest` (which documents itself as mirroring `MongoClientBase`);
`doc/schema-migration.md` nowhere enumerates the replacement index's fields, so it needs no change.

**D10 — skip-based paging ships in Phase 1; Phase 3 converts tokens to opaque.**
This plan's first draft had Phase 1 apply `DEFAULT_QUERY_LIMIT` while deferring all paging to
Phase 3. That combination creates a silently-truncated interim: both queries would return the first
100 records with a blank `nextPageToken` and no way to fetch the rest — an undetectable partial
result, the same class of wrong answer #245 just eliminated for `queryPvMetadata` (there the hazard
was an undetectable unbounded read; here it would be an undetectable truncation). Since Phase 1
rewrites both query methods anyway, it ships the proven mechanical pattern instead, copied from
`executeQueryConfigurations` (`:910`): limit resolution against `DEFAULT_QUERY_LIMIT`, Base64
skip-token decode, sort, `.skip()`, the `limit + 1` probe, trim and re-encode `nextPageToken`.
Phase 3 then converts these two queries' tokens to opaque with reject-on-malformed, which the
merged proto specifies (`annotation.proto:930-933`, `:1473-1476`); the interim Base64 tokens are a
documented temporary divergence from that contract, not the end state. D6 is unchanged: the three
metadata queries keep their skip tokens, follow-on ticket.

### Phase 1 PR (#256) review adjustments

The PR review (Claude + Copilot, 2026-09-04) tightened several Phase 1 behaviors beyond the plan as
drafted; these are the settled shape, not deviations to re-litigate:

- **Blank criterion entries are rejected server-side** (`RESULT_STATUS_REJECT`), not silently
  dropped, on both rewritten queries. The draft preserved the interim blank-skip filters in the
  Mongo client; review found that a blank-only criterion then vanished and turned the query into a
  silent match-all (#243 class) while a blank singular id was previously *rejected*. Validation in
  `AnnotationServiceImpl` now rejects blank entries in every criterion value list, including
  `NameCriterion`'s three lists, and the Mongo client builds filters without re-filtering.
- **`IdCriterion` ids are validated with `ObjectId.isValid()`**. Unvalidated, a malformed id threw
  `IllegalArgumentException` from the `ObjectId` constructor inside the worker thread, where
  `QueueHandlerBase` swallows it and the caller's stream hangs with no response ever sent.
- **Criterion filters route through `MongoQueryFilterBuilder`** (`nameMatchFilter` / `tagsFilter` /
  `attributeFilter`) instead of inline copies, per that class's single-implementation contract.
- **The five skip-paged queries share `applySkipPaging()` / `decodePageTokenSkip()`** on
  `MongoSyncAnnotationClient`; the helper also guards the `limit + 1` probe against int overflow at
  `limit = Integer.MAX_VALUE`. Phase 3's opaque-token conversion happens in one place.
- **`AnnotationClient` exposes paging** for `queryDataSets` / `queryAnnotations` (params `limit` /
  `pageToken`, results carry `nextPageToken`) — without it the client silently truncated at the
  server's default page size, the exact hazard D10 ships server paging to prevent.
- **The query dispatchers contain document-conversion failures**: `toDataSet()` / `toAnnotation()`
  run inside try/catch and dispatch an error, per the "malformed stored document must produce a
  reportable error, never an unchecked throw" invariant.
- **Known Phase 1 window**: `SaveDataSetRequest.tags` / `.attributes` are accepted and silently
  dropped (`DataSetDocument.fromSaveDataSetRequest` copies neither) while `TagsCriterion` /
  `AttributesCriterion` filtering ships — so a tag saved in this window can never be matched.
  Accepted because Phase 2 lands the save half before any release is cut, extending the same
  no-release reasoning as D9; do not cut a release between Phase 1 and Phase 2.

## Phases

Each phase is one PR against #248, which stays open until the last lands.

### Phase 1 — restore compilation (no new capability)

Fixes all 58+ main-source errors and whatever the test sources add behind them.

| Area | Work |
|---|---|
| `Annotation` hoist | Retarget 5 references from `QueryAnnotationsResponse.AnnotationsResult.Annotation` to top-level `Annotation` |
| `comment` → `description` | `AnnotationDocument` field/accessors/`fromSaveAnnotationRequest`/`diffSaveAnnotationRequest` (D3). The `BsonConstants` key and `MongoClientBase` text index halves already landed with #254, which is exactly why this half is mandatory — see [Migration strategy](#migration-strategy-resolved-by-254) |
| Text index target | Drop `event.description` (`BSON_KEY_EVENT_DESCRIPTION`) from the annotations text index at `MongoClientBase:275` and from `V1AnnotationCommentToDescriptionTest.createNewTextIndex()`, leaving text over `name` + `description` with ascending `ownerId` (D9) |
| Denormalization removal | `AnnotationDocument.toAnnotation()` loses its `dataSetDocuments` / `calculationsDocument` parameters and returns references only. The `dataSets` removal is **forced** — the proto field is gone. Dropping embedded calculations content is the query-path *contract*: `Annotation.calculations` still exists, but the proto has `queryAnnotations()` leave it empty and only `getAnnotation()` populate it — keep a seam for Phase 2 to set it. Deletes `QueryAnnotationsDispatcher`'s per-annotation `findDataSet` loop and its 3 no-`return` bugs, plus the now-dead `mongoClient` dependency |
| Flat `SaveDataSetRequest` | `DataSetDocument.fromSaveRequest`/`diffRequest`: `request.getDataSet().getX()` → `request.getX()`; `SaveDataSetJob:55` |
| `CalculationsDataFrame.frame` | `CalculationsDataFrameDocument` and `AnnotationValidationUtility`: `frame.getX()` → `frame.getFrame().getX()` (§2 — indirection only) |
| Criterion accessors | Singular → repeated at every site in `AnnotationServiceImpl`, `MongoSyncAnnotationClient`, `MongoAnnotationHandler`, `AnnotationClient` (§5 — `TextCriterion` alone keeps its old shape). **Semantics preserved** (D4): a repeated field with one value behaves as the old singular one |
| Empty criteria | Remove the `return null` at `MongoSyncAnnotationClient:258-262` and `:502-506`, plus the two service-layer checks (§3, D5) |
| Skip-based paging | Convert both queries from unbounded cursors to the paged `List` pattern of `executeQueryConfigurations` (`:910`): limit resolution applying `DEFAULT_QUERY_LIMIT` unconditionally, Base64 skip token, `limit + 1` probe, `nextPageToken` (D10) |
| Fall-through bug | The old null-`dataSet` guard is dead code, deleted with the flat-request rewrite; every reject in the rewritten `saveDataSet` validation gets a `return` (§4) |
| Tests | Update `AnnotationTestBase` (2467 lines), `GrpcIntegrationAnnotationServiceWrapper` (1745), `QueryAnnotationsIT`, `AnnotationCalculationsIT`, and the client ITs for the renamed field and repeated criteria |

**Exit criterion**: `mvn clean verify` green, CI green, no new RPCs, no opaque tokens, no typed columns.

### Phase 2 — entity and audit fields, new CRUD methods

`modifiedBy`, `createdTime`/`updatedTime` emission, `DataSet` tags/attributes; then `getDataSet`,
`getAnnotation`, `getCalculations`, `deleteDataSet` (D7), `deleteAnnotation`, and the two `patch*`
deferred stubs. `GetConfigurationJob` / `GetConfigurationDispatcher` are the template.

Two prerequisites in the Mongo client, both consequences of the #235 reject-vs-error invariant — a
get/delete must report not-found as `REJECT` and a query failure as `ERROR`, and the current
helpers cannot make the distinction:

- The throwing lookup variants `lookupDataSet` / `lookupAnnotation` exist but are **private**;
  promote them or add interface-level equivalents. `findCalculations`
  (`MongoSyncAnnotationClient:561`) has no throwing variant at all — it catches bare `Exception`,
  logs without the exception object (pre-#191 style), and returns null for "absent", "query
  failed", and "malformed id" alike — so `getCalculations` needs a `lookupCalculations` that
  throws `DpException`.
- Decide how a malformed ObjectId classifies for the new get/delete methods: `new ObjectId(id)`
  throws `IllegalArgumentException`, which `saveDataSet` deliberately routes to **error**
  (`MongoSyncAnnotationClient:125-130`); for a get/delete keyed on that id, a malformed id is a
  client mistake and arguably a **reject**. Pick one and document it — do not let the outcome fall
  out of whichever catch block happens to be nearest. *Resolved by D11 below: reject.*

`getAnnotation` populates `Annotation.calculations` inline — the proto assigns that to
`getAnnotation()` only (see Phase 1's denormalization row) — so it re-adds the calculations fetch
`queryAnnotations` lost, this time bounded to a single annotation.

#### Phase 2 planning (2026-09-04)

Triage of the Phase 2 scope against `main` @ `04e8902` surfaced four findings beyond the plan as
written above:

1. **None of the 7 new RPCs was overridden at all** — they answered with gRPC's default
   UNIMPLEMENTED status, not this repo's "not yet implemented" convention response.
2. **`SaveAnnotationResult.calculationsId` (proto field 2) was not emitted.** The handoff (§6)
   requires it and no phase owned it; `SaveAnnotationJob` already holds the id, so Phase 2 adds the
   dispatcher/sender plumbing.
3. **`MongoAnnotationHandler.validateSaveAnnotationRequest` used the swallowing `find*` helpers**,
   so a Mongo outage during save validation read as "no DataSetDocument found with id" — the #235
   inversion. Fixed with the promoted lookups; regression tests live in
   `MongoSyncAnnotationClientLookupFailureTest`. A side effect: malformed ids in
   `dataSetIds`/`annotationIds` are now rejected with a precise "contains invalid id" message
   (D11) rather than masquerading as absence.
4. **`AnnotationDocument.diffSaveAnnotationRequest` had two latent bugs** — the `dataSetIds` and
   `annotationIds` branches built a mismatch message but never added it to the diffs list, so those
   field mismatches were invisible to every wrapper verification. Fixed.

Design decisions, continuing the numbering:

- **D11 — malformed ObjectId on the new get/delete methods is a REJECT, validated in the job.**
  Each new job checks blank + `ObjectId.isValid()` before any client call — a malformed id is a
  client mistake, and validation also forecloses the `IllegalArgumentException`-in-worker-thread
  hang. Matches Phase 1's `IdCriterion` validation. `saveDataSet`/`saveAnnotation` keep their
  existing error classification for their internal lookups (documented divergence).
- **D12 — Annotation tags adopt house normalization; migration v2 normalizes stored tags.**
  (Ticket owner decision, 2026-09-04.) DataSet tags (clean slate — never persisted) and Annotation
  tags (existing field, previously stored as-given) both normalize lowercase/dedupe/sort on save
  via `DpBsonDocumentBase.normalizedTags()`, matching pvMetadata/configuration. Without a
  migration, previously stored mixed-case annotation tags would be unreachable by normalized
  `TagsCriterion` values — a #197-class silent wrong answer — so `V2NormalizeAnnotationTags` ships
  with this phase (idempotent: normalization is a fixpoint; a no-op scan where no tags exist). If
  no deployment holds annotation tags this migration is droppable, but it is safe regardless.
- **D13 — `updatedTime` stays unset on create.** Matches the four shipped entity types
  (pvMetadata, configuration, activation, sample status); the handoff §11's "equal on create"
  wording loses to in-service consistency. Absent `updatedTime` means "never updated".
- **D14 — D8 moves from Phase 4 into Phase 2.** (Ticket owner decision.) `deleteAnnotation` builds
  the `deleteCalculations` machinery anyway, so `SaveAnnotationJob` gains the delete-the-previous
  path now — covering both replace-with-new and the omit-clears case. Cleanup failure after a
  successful save logs the orphaned id at error level; the response reflects the save's outcome (a
  retry cannot remove the orphan, and reporting the save as failed would mislead).
- **D15 — deleteAnnotation deletes the annotation first, then its calculations.** A failure
  between the two leaves an orphaned calculations document (harmless, the known D8 class) rather
  than a live annotation whose dangling `calculationsId` would break `getAnnotation`. If the
  calculations delete fails, the error response names the orphaned id and states the annotation
  itself was deleted.
- **D16 — `getAnnotation` treats a dangling `calculationsId` as an ERROR, not empty content.** The
  annotation asserts calculations exist; absence of the document is corruption, and silently-empty
  content is exactly the wrong-answer failure mode this repo treats as the serious one.
- **D17 — client scope: get wrappers only, no delete wrappers.** `AnnotationClient` gains
  `getDataSet` / `getAnnotation` / `getCalculations` (per the `getConfiguration` pipeline) and the
  save params carry the new fields; no entity has a client delete wrapper today, and adding
  deletes across all entities is a follow-on.

### Phase 2 PR (#261) review adjustments

The PR review (Claude + Copilot, 2026-09-07) tightened several Phase 2 behaviors beyond the plan as
drafted; these are the settled shape, not deviations to re-litigate:

- **Tag lowercasing is `Locale.ROOT`** in `DpBsonDocumentBase.normalizedTags()` and migration v2 —
  the default-locale fold is environment-dependent (Turkish dotless i) and would bake locale-variant
  bytes into stored data no normalized `TagsCriterion` value could match. The three pre-existing
  inline normalization copies (pvMetadata / configuration / configurationActivation) converged onto
  the shared helper at the same time.
- **The D14 cleanup moved into `saveAnnotation()` (client)**, beside the lookup that captures the
  previous document — where `deleteAnnotation`'s cascade already lives — removing the job's
  duplicate lookup on every update-path save.
- **A rejected save deletes the calculations document it just inserted** (both reject paths precede
  any annotation write, so the compensating delete is safe); a save *error* only logs the possibly
  orphaned id, because deleting under an ambiguous write state could dangle a live annotation's
  `calculationsId` (the D16 corruption).
- **`validateSaveAnnotationRequest` throws `DpException` on lookup failure**, dispatched as
  `RESULT_STATUS_ERROR`. The Phase 2 draft fixed the #235 inversion in the message text only; the
  wire status still said REJECT, which is what clients branch on.
- **Reference ids are stored canonical** (lowercase hex): `deleteDataSet`'s referential-integrity
  check and the queryAnnotations dataSets/annotations criteria match strings while validation
  parses binary ObjectIds, so a case-variant id passed validation yet bypassed every reference
  check. Saves canonicalize; **migration v3** (`V3CanonicalizeAnnotationReferenceIds`) converts
  previously stored references; the delete check and criteria canonicalize their inputs.
- **`ExportDataJobBase` uses `lookupCalculations()`** — the last caller of the swallowing
  `findCalculations()` that could act on the failed-vs-absent distinction.
- **Migration v2/v3 name the offending document** when a stored array is malformed (non-string or
  null element), instead of failing startup with a bare unchecked exception.
- **`deleteDataSet`'s check-then-delete race** with concurrent `saveAnnotation` is documented at
  the check as an accepted v1 limitation, like `overlapExists()`. The losing-save orphan race on
  concurrent same-id saves remains possible and benign (an unreferenced calculations document).
- Mechanical: the 27-copy `requestQueue.put` boilerplate in `MongoAnnotationHandler` collapsed into
  `QueueHandlerBase.enqueueJob()` (which also fixes the #191 logging-convention violation); the five
  new jobs share `AnnotationValidationUtility.validateRequiredObjectId()`; patch-stub ITs pin
  `RESULT_STATUS_ERROR` on the wire; the save-annotation wrapper clears its calculationsId capture
  before each send so a rejected save cannot leave a stale value.

### Phase 3 — paging, ordering, and criteria semantics

Converts the two queries' Base64 skip tokens (shipped in Phase 1, D10) to opaque tokens with
reject-on-malformed per the proto contract (D6), adds the documented ordering with the activation
tiebreaker, and makes the all-AND criteria change (D4) with repeated `IdCriterion` compiling to
`$in`. The AND change needs its own release-note line: it silently changes results for
multi-criterion queries valid today.

#### Phase 3 planning (2026-09-07)

Triage of the Phase 3 scope against `main` @ `d5c5928` surfaced five findings:

1. **The two queries' ordering is already done.** Phase 1's paging shipped `sort(ascending(_id))`
   for both (`MongoSyncAnnotationClient:365`, `:714`), and the handoff's D8 table marks them
   "none — already correct". The "activation tiebreaker" in this phase's summary belongs to
   `queryConfigurationActivations`: it sorts on `startTime` alone (`:1497`), which is not unique,
   so ties can drop or duplicate rows across skip-page boundaries. The merged proto already
   documents the fix as contract: `startTime` asc, then `configurationName` asc, then id asc.
2. **Repeated `IdCriterion` → `$in` is already done.** Both criterion switches compile an
   `IdCriterion` to `Filters.in` on `_id` (Phase 1); under all-AND, multiple `IdCriterion`
   entries intersect, matching the proto. No code change — test coverage only.
3. **Two `$text` clauses cannot be ANDed** — verified against a throwaway MongoDB 8.0 container:
   `$and: [{$text: A}, {$text: B}]` fails server-side with "Too many text expressions". Under
   all-AND, a request with two `TextCriterion` entries would surface that as
   `RESULT_STATUS_ERROR` — a client mistake misclassified as a retryable service failure (the
   #235 inversion). It must be a validation REJECT (D21).
4. **`$text` under `$or` requires every other clause indexed** (same container check: planner
   error "Failed to produce a solution for TEXT under OR"). Today's queryDataSets OR bucket
   (`text OR pvName`) survives only because `dataBlocks.pvNames` happens to be indexed — the
   all-AND collapse removes that fragility class entirely. Keyset resume (`$text` AND `_id > x`,
   sort `_id` asc) composes fine — verified in the same session.
5. **Only one implementation site.** `MongoAnnotationClientInterface` has a single implementor
   (`MongoSyncAnnotationClient`; the async annotation client no longer exists), so the
   signature change for passing a decoded resume position is contained. The `AnnotationClient`
   wrappers treat tokens as opaque pass-through already — no client change.

Design decisions, continuing the numbering:

- **D18 — keyset tokens by `_id`, not an opaque envelope around a skip offset.** The token
  encodes the last-returned id; resume filters `_id > lastId`. This is the token type the
  handoff's D7 names (`SampleStatusPageToken` precedent), it is stable while documents are
  inserted or deleted mid-pagination (a skip offset drifts — the proto's "makes paging stable"
  language), and resume is O(1) instead of O(skip). The rejected alternative — wrapping the skip
  offset in a validatable envelope — satisfies reject-on-malformed but keeps both drift and the
  linear scan. The three metadata queries keep their skip tokens (D6, unchanged).
- **D19 — the token carries a query discriminator, and a wrong-query token is rejected.** The two
  tokens are otherwise structurally identical (one ObjectId hex), so a queryDataSets token pasted
  into queryAnnotations would decode cleanly and silently skip an arbitrary prefix of results —
  the silent-wrong-answer class this repo treats as the serious one. `SampleStatusPageToken`
  carries no discriminator only because a single method family consumes it.
- **D20 — token decode and rejection live in the job**, following `QuerySampleStatusesJob`
  verbatim: blank token → first page; undecodable or wrong-query token →
  `dispatcher.handleValidationError` (both query dispatchers gain `handleValidationError`); the
  decoded position passes to the client as an `ObjectId resumeAfterId` parameter. Criterion
  validation stays in `AnnotationServiceImpl` (Phase 1 shape) — the split matches sample status,
  where the request validator never sees the token either.
- **D21 — at most one `TextCriterion` per request; a second is a validation REJECT** in
  `AnnotationServiceImpl`, for both queries (triage finding 3). Nothing that works today is
  narrowed: two text criteria already fail on both queries (AND bucket for annotations, TEXT
  under OR for datasets) — they fail as errors; this makes the outcome honest.
- **D22 — the bucket collapse produces one filter list**: empty list → match-all (the #245
  contract, untouched), otherwise `Filters.and(list)`, with the keyset resume filter ANDed in
  after. The `Filters.exists(_id)` placeholder-and-`or()` scaffolding goes away in both methods.
- **D23 — the activation tiebreaker is added to `executeQueryConfigurationActivations` only.**
  The internal `getActiveConfigurations` also sorts bare `startTime`, but it is unpaged and
  carries no ordering contract; touching it would widen the diff for no behavioral need.
- **D24 — the AND change is its own commit, whose diff is exactly the collapse plus its tests.**
  D4 demanded the change be reviewable in isolation; within a one-PR phase, commit granularity is
  what delivers that. The release-note line lands in a new `doc/release-notes/rel-1.16.0.md`
  draft (none exists yet for 1.16.0; the file starts with the #248 behavior changes).

Implementation tasks:

- `annotation/handler/model/AnnotationQueryPageToken.java` (new) — record `(String query, String
  lastId)` with `encode()`/`decode(token, expectedQuery)` per `SampleStatusPageToken`; decode
  returns null unless parseable, `lastId` is valid ObjectId hex, and the discriminator matches.
  Discriminator constants for the two queries live on the record. Unit test beside
  `SampleStatusPageToken`'s.
- `MongoAnnotationClientInterface` / `MongoSyncAnnotationClient` — add `ObjectId resumeAfterId`
  to both query signatures; new `applyKeysetPaging()` helper beside `applySkipPaging()` (same
  `limit + 1` probe and trim, token from the last returned document's id); collapse the criterion
  buckets (D22); tiebreaker sort in `executeQueryConfigurationActivations` (D23); update the
  helper javadocs that describe Phase 3 as future work.
- `QueryDataSetsJob` / `QueryAnnotationsJob` — decode/reject per D20; `QueryDataSetsDispatcher` /
  `QueryAnnotationsDispatcher` gain `handleValidationError`.
- `AnnotationServiceImpl` — multiple-`TextCriterion` reject in both validation switches (D21).
- Tests — `QueryDataSetsIT` / `QueryAnnotationsIT`: malformed-token reject, wrong-query-token
  reject, two-`TagsCriterion` intersection, cross-type AND (datasets: text+pvName; annotations:
  tags+attributes), two-`TextCriterion` reject; existing pagination tests are token-agnostic and
  must pass unchanged. `ConfigurationIT`: activations sharing a `startTime` return in
  documented order and page stably across the tie.
- Docs — CLAUDE.md pagination/#245 sections (the two queries no longer use skip tokens; the
  "Phase 3 converts" sentences become past tense); `doc/release-notes/rel-1.16.0.md` draft with
  the AND-semantics line (D24).

### Phase 3 PR (#263) review adjustments

The PR review (Claude + Copilot, 2026-09-08) tightened two behaviors and recorded one deliberate
deferral:

- **Whitespace-only page tokens reject.** The keyset jobs guard the token with `isEmpty()`, not
  `isBlank()`: proto3's unset default is exactly `""`, and a whitespace token was never issued by
  the server, so reject-on-malformed applies — under `isBlank()` it silently restarted at the
  first page. Copilot flagged the two new jobs; the fix also covers `QuerySampleStatusesJob`,
  which they had copied verbatim (D20), so the three keyset jobs stay uniform.
- **The query reject paths assert the wire status.** `QueryDataSetsResponseObserver` and
  `QueryAnnotationsResponseObserver` capture `ExceptionalResultStatus`, and the two `sendQuery*`
  wrappers assert `RESULT_STATUS_REJECT` in their expectReject branch — without it, the new
  `handleValidationError` → reject wiring could route to `sendError` and no test would notice
  (the #235 divergence; the Save/Delete observers got this in the Phase 2 review). The remaining
  wrappers that assert only a message substring span every entity's query/get/delete paths and
  are a follow-on, not this phase.
- **The activation compound sort is not index-backed — deferred deliberately.**
  `configurationActivations` carries single-field indexes, so the D23 sort
  (`startTime`, `configurationName`, `_id`) is a blocking sort (disk-spilling by default on
  MongoDB 8.0: correct, but not index-assisted). At plausible activation-collection sizes this
  is immaterial; if activations grow large, an additive compound index
  `(startTime, configurationName, _id)` in `init()` restores an index-backed sort with no
  migration (index *additions* are not migration steps — only changes are).

### Phase 4 — typed calculation columns and export

`CalculationsDataFrameDocument` gains the 16 typed column types alongside `dataColumns` (§2), then
`ExportDataJobAbstractTabular` and the HDF5 path. Preserve the existing invariant: tabular formats
(CSV, XLSX) represent scalar columns only; array/binary are HDF5-only. Then inline `dataBlocks` as
an export source, and `ColumnProvenance.derivedFrom` stored-not-interpreted.

Independent of Phases 2–3 and the largest single chunk. (D8 originally sat here; Phase 2's D14
pulled it forward.)

#### Phase 4 planning (2026-09-08)

Triage of the Phase 4 scope against `main` @ `7f6bfdf` surfaced nine findings:

1. **The silent-loss premise holds, but which failure a client sees depends on the frame's
   shape.** `CalculationsDataFrameDocument.fromCalculationsDataFrame()` reads only
   `getDataColumnsList()` (`CalculationsDataFrameDocument.java:58`), so typed columns are dropped
   on save — but `validateSaveAnnotationRequest` rejects a frame whose legacy `dataColumns` list
   is empty (`AnnotationValidationUtility.java:170`). A typed-columns-only frame is therefore
   **rejected** today, and the silent loss is reachable only for a frame mixing legacy and typed
   columns. Both halves are wrong; they just fail differently.

2. **The field retype requires schema migration v4.** Verified in-JVM against the driver, with a
   codec registry built the way `MongoClientBase.getPojoCodecRegistry()` builds it (discriminator
   handling is purely client-side, so no server was needed): the POJO codec **does** write the
   `_t` discriminator even when the declared field type is the concrete class, and a stored entry
   *without* `_t` decodes fine under a concrete declared type but throws
   `CodecConfigurationException` under an abstract one. Calculations storage shipped in
   rel-1.10.0 (`04b6c29`, #119) using the pre-hierarchy `DataColumnDocument`; the
   `@BsonDiscriminator` arrived with #173 (`f37b643`) in rel-1.13.0. A deployment that saved
   calculations on 1.10–1.12 therefore holds columns the retyped `List<ColumnDocumentBase>`
   field cannot decode. The old and new BSON shapes are otherwise identical
   (`name`/`valueCase`/`valueType`/`bytes`), so stamping `_t: "dataColumn"` where missing is
   sufficient — and idempotent.

3. **The same `_t` gap exists for buckets, today, independent of this ticket — and it is
   fully silent.** Pre-#173 `BucketDocument` declared the concrete
   `DataColumnDocument dataColumn`; the current field is the abstract `ColumnDocumentBase`. Any
   bucket written by a pre-1.13 build fails codec decode under every current build — mid-cursor,
   upstream of `dataBucketFromDocument()`, so the "deserialization must fail as `DpException`"
   contract never gets the chance to apply: the exception escapes the dispatchers'
   `DpException`-only catch and the client receives zero buckets with no error. Nothing at
   startup notices either — `BucketSpanVerifier` scans raw `Document`s via aggregation, never
   POJOs, and its missing-field corruption check tests `dataColumn` presence, not `_t`. Not
   caused or worsened by Phase 4, but repaired by it: see D27.

4. **`ColumnProvenanceDocument` does not store `derivedFrom` at all**
   (`ColumnProvenanceDocument.java:8-11` — `source` and `process` only). A typed column carrying
   provenance links loses them at write time on every path that stores columns — ingestion
   buckets today, calculations once the retype lands. The proto contract ("derivedFrom links are
   stored as supplied", `common.proto:41`) dates from #132 (dp-grpc `7b2ea35`); dp-service never
   caught up. The legacy column has a subtler variant: `DataColumnDocument.bytes` holds the
   complete metadata, but `toProtobufColumn()` calls `applyMetadataToProto()`, which overwrites
   the complete in-bytes metadata with the lossy document version — so `derivedFrom` vanishes on
   the HDF5-export read path even for legacy columns, while `toDataColumn()` (tabular) preserves
   it. This asymmetry is the real content of the "stored-not-interpreted" scope line.

5. **A count-mismatched calculations column hangs the export stream today.** Nothing compares a
   calculation column's value count to its frame's timestamp count, and
   `TabularDataUtility.addColumnsToTable()` indexes `dataColumn.getDataValues(valueIndex)`
   (`TabularDataUtility.java:268`) — a short column throws `IndexOutOfBoundsException`,
   unchecked, escaping `exportData_()` and `execute()` into `QueueHandlerBase`, which swallows
   it: the caller's stream hangs with no response. Reachable now by saving a legacy `DataColumn`
   shorter than its frame's axis and exporting tabular. (Ingestion validates exactly this,
   `IngestionValidationUtility.java:220`; calculations never did.)

6. **Frame-name distinctness is proto contract but unenforced.** The `CalculationsDataFrame`
   comment says duplicate names "are unaddressable and are rejected" — they key
   `CalculationsSpec.dataFrameColumns` and provenance links. No such check exists in
   `validateSaveAnnotationRequest`. Column names within a frame are the same kind of addressing
   key (ingestion's analog is the unique-PV-names-per-frame cross-check).

7. **Inline `dataBlocks` are rejected today** by `validateExportDataRequest` ("either dataSetId
   or calculationsSpec must be specified", `AnnotationValidationUtility.java:204`); nothing in
   the service references `getDataBlocksList()`. The output filename derives from
   `exportObjectId` — dataSetId, else calculationsId (`ExportDataJobBase.java:88-121`) — and
   `getExportFileSubdirectory()` (`ExportConfiguration.java:144`) assumes an ObjectId-shaped
   string for its balanced directory layout, so an inline-only request needs a generated id.

8. **Export failure classification predates #235.** `ExportDataDispatcher` has only
   `handleError`: dataset/calculations not-found, bad filter names, and non-scalar-in-tabular
   all reach the wire as `RESULT_STATUS_ERROR`, while the proto says a tabular request for
   array/image/struct content "is rejected". The dataset fetch still uses the null-collapsing
   `findDataSet()` (`ExportDataJobBase.java:88`) — the calculations fetch beside it was already
   converted to `lookupCalculations` with the #235 comment — and `dataSetId` is never
   ObjectId-validated (harmless today because `lookupDataSet` catches the
   `IllegalArgumentException`, but a malformed id then reads as "not found").
   `NonScalarColumnException` was designed for caller-phrased guidance (Q4) and the querySamples
   dispatchers use it that way; the export framework instead lets it fall into the generic
   `DpException` catch ("exception building tabular result: ...").

9. **`handleExportData` responds then throws on its defensive enum branches.** The
   UNSPECIFIED/UNRECOGNIZED cases send an error response but fall through to
   `Objects.requireNonNull(job)` (`MongoAnnotationHandler.java:343`) — an NPE on the gRPC thread
   after `onCompleted()`. Unreachable while validation holds; fix with `return`s in passing.

Also confirmed ready to reuse: `ingestionDataFrame` *is* `common.DataFrame`
(`ingestion.proto:220`), so one DataFrame-to-columns dispatch can serve both paths (D26);
`validateColumnMetadata`/`validateAllColumnMetadata`
(`IngestionValidationUtility.java:590`, `:635`) carry the metadata limits but hardcode ingestion
field paths; `TimestampDocument` exists for storing `TimeRange`; and the client passes
`Calculations` through verbatim on save (`AnnotationClient.java:607`), so the only client change
is `ExportDataRequestParams`.

Design decisions, continuing the numbering:

- **D25 — `CalculationsDataFrameDocument.dataColumns` becomes one polymorphic
  `List<ColumnDocumentBase>` under the existing BSON field name.** Matches the bucket pattern
  (`BucketDocument.dataColumn`), keeps every post-1.13 stored document readable as-is
  (finding 2), and the discriminator round-trips the concrete type.
  `toCalculationsDataFrame()` dispatches each document back to its `DataFrame` repeated field by
  concrete type. Rejected: 16 parallel typed list fields on the document — drift-prone, no
  addressing benefit, and a new BSON shape for no reason.

- **D26 — the 16-branch DataFrame dispatch is extracted into a shared helper** (new
  `ColumnDocumentUtility.fromDataFrame(DataFrame)` in `common/bson/column/`), consumed by both
  `BucketDocument.generateBucketsFromRequest()` (`BucketDocument.java:131`, behavior-identical
  refactor) and `CalculationsDataFrameDocument.fromCalculationsDataFrame()`. Duplicating the
  dispatch is exactly the drift the `normalizedTags()` history warns about, and the "Systematic
  Process for Adding New Protobuf Column Types" gains one shared step instead of two parallel
  ones.

- **D27 — schema migration v4 stamps `_t: "dataColumn"` on every embedded legacy column
  missing it, in `buckets` and `calculations` alike.** (Ticket owner decision, 2026-09-08;
  supersedes the planning draft's separate-ticket split.) The two halves are one defect — #173
  added `@BsonDiscriminator` without a migration, because the mechanism did not exist until
  #254 — so v4 is the migration #173 should have shipped, plus what the D25 retype newly
  requires. The asymmetry decides it: folding costs at worst a one-time full scan of `buckets`
  that matches nothing (minutes to perhaps an hour at the reference archive's 33.8M documents),
  while deferring ships a release that silently zeroes out query results on any archive holding
  pre-1.13 buckets (finding 3) and still owes the same scan later as a mandatory v5 — released
  migrations are append-only. A one-time full bucket scan at startup also has precedent:
  `BucketSpanVerifier` already does exactly that.

  Shape and safety: one migration, two `updateMany` calls filtering on a present `dataColumn`
  subdocument whose `_t` key is absent (per array entry for calculations frames) — idempotent
  by construction, and the filter naturally skips v1-shaped buckets that predate the embedded
  subdocument. The pre-1.13 subdocument is field-identical to today's
  (`name`/`valueCase`/`valueType`/`bytes`), so stamping is sufficient. Migrations operate on
  raw `Document`s (#254 rule), so v4 is immune to the decode failure it repairs, and the runner
  is ordered before anything that decodes bucket POJOs. A long scan cannot lose its claim:
  takeover requires a *released* claim (`SchemaMigrationRunner.migrateOrWait()`), never a
  merely old one.

  Operational consequence, documented rather than engineered around: while the elected process
  scans, the other services wait `CLAIM_WAIT_TIMEOUT_MILLIS` (5 minutes, hardcoded) and then
  refuse to start with the held-claim message — normal during a long v4, self-healing under a
  supervisor, and the constant deliberately stays hardcoded (making it configurable is scope
  creep for a one-time event). `doc/schema-migration.md` and the release notes must say so,
  with expected duration and a note that the timeout message during a *running* migration is
  not the stuck-claim case it also describes. Implementation must verify the retype + v4
  against a throwaway `mongo:8.0` container seeded with pre-1.13-shaped bucket and calculations
  documents (the #254 lesson), not only the in-JVM probe from finding 2.

- **D28 — save-side validation extends to the full frame shape, and count-match becomes
  mandatory for every column type, legacy included.** Per column (all 16 types): blank-name
  reject, empty-values reject, value count must equal the frame's timestamp count — the count
  check is what closes the export hang (finding 5); a legacy `DataValue` with no arm set still
  occupies its position, so sparse legacy columns remain expressible. Per frame: at least one
  column of any type (replaces the legacy-list-only emptiness check); column names unique across
  all column types in the frame. Per Calculations object: frame names unique (finding 6).
  Column metadata gets the ingestion limits by extracting
  `validateColumnMetadata`/`validateAllColumnMetadata` into a shared utility parameterized on
  the field-path prefix, with `IngestionValidationUtility` delegating. The count check on legacy
  columns narrows what `saveAnnotation` accepts — a release-notes line, same
  no-production-consumers justification as D4. (`SerializedDataColumn` entries carry no
  countable values; they get the name/metadata checks only.)

  *Review addition:* the ingestion **value caps** apply too — string values ≤ 256 chars, array
  dims product ≤ 10M elements, image ≤ 50MB, struct ≤ 1MB — via the shared `ColumnValueLimits`
  (`common/handler`), which ingestion's private constants now alias. Review found the caps were
  ingestion-only, so data ingestion would reject was storable through saveAnnotation; one shared
  constants home closes that and prevents drift. Ingestion's identity-field requirements
  (enumId, schemaId, imageDescriptor, serialized encoding) deliberately remain ingestion-only:
  calculations columns are not PV channels.

- **D29 — `ColumnProvenanceDocument` gains `derivedFrom`**: new embedded
  `ColumnSourceDocument` (`pvName`; `calculationsColumn` as embedded
  `CalculationsColumnDocument` with calculationsId/frameName/columnName; `timeRange` as
  begin/end `TimestampDocument`s), registered in the codec ahead of its parents and
  round-tripped in `fromColumnProvenance`/`toColumnProvenance`. Stored as supplied: no existence
  checks and no ObjectId parse of `calculationsId` — a link may point at records not yet
  created; only the shared length limits (D28) apply. One change fixes ingestion buckets and
  calculations alike, and dissolves the legacy `applyMetadataToProto()` overwrite loss
  (finding 4) without touching that mechanism.

- **D30 — export client mistakes become rejections: `ExportDataDispatcher` gains
  `handleReject`.** Rejected: dataset or calculations id not found; filter frame/column names
  that don't exist; non-scalar content in a tabular export — `ExportDataJobAbstractTabular`
  catches `NonScalarColumnException` ahead of `DpException` and phrases the Q4 guidance
  ("...export to HDF5 instead"), for the dataset and calculations paths alike. Errors stay
  errors: lookup `DpException` (outage), file I/O, size-limit-exceeded. The dataset fetch
  switches to `lookupDataSet()` with the same catch shape the calculations fetch already has,
  and `validateExportDataRequest` ObjectId-validates a non-blank `dataSetId` so malformed reads
  as malformed rather than "not found". ("data block query returned no data" stays an error —
  pre-existing behavior, not litigated here.)

- **D31 — inline blocks merge into one effective `DataSetDocument`; the export id falls back to
  a generated ObjectId.** Validation: at least one of
  `dataSetId`/`dataBlocks`/`calculationsSpec` (supersedes the two-source check, finding 7), with
  per-block validation via a `validateDataBlock()` helper extracted from
  `validateSaveDataSetRequest` (`AnnotationValidationUtility.java:57-79`). In the job: fetch the
  stored document when `dataSetId` is set and append inline blocks to its block list, or build a
  transient document when it is not; everything downstream — block queries, tabular assembly,
  HDF5 `writeDataSet` — proceeds unchanged and records the effective block list; nothing is
  persisted. `exportObjectId` resolution: dataSetId, else calculationsId, else `new ObjectId()`,
  preserving `getExportFileSubdirectory()`'s shape assumption. Client:
  `ExportDataRequestParams` gains `dataBlocks` and `buildExportDataRequest` emits them.

- **D32 — HDF5 calculations columns get the bucket treatment**: iterate `ColumnDocumentBase`,
  write `toProtobufColumn().toByteArray()` plus the self-describing `DATA_COLUMN_ENCODING`
  (`"proto:" + simpleName`) tag per column (`DataExportHdf5File.writeCalculations`,
  `:246-334`, mirroring the bucket writer at `:200-211`). Files written by earlier builds carry
  no tag for calculations columns and were implicitly `DataColumn`-encoded; export files are
  point-in-time artifacts, so the addition is a release-notes line, not a compatibility
  mechanism.

- **D33 — tabular calculations get the `addBucketToTable` narrowing, and `execute()` stops
  trusting `exportData_()` not to throw unchecked.** `addCalculationsToTable` narrows each
  column: `ScalarColumnDocumentBase` → `toDataColumn()`; `DataColumnDocument` →
  `toDataColumn()`; else `NonScalarColumnException`, named "frameName/columnName" in the PV-name
  slot. Independently, `ExportDataJobBase.execute()` wraps the `exportData_()` call in a
  `RuntimeException` catch dispatched as an error: D28 stops new count-mismatched writes, but a
  column stored before this phase's validation would still hang the stream through the
  finding-5 mechanism, and a hang is strictly worse than a misclassified error (the #235
  hierarchy).

Implementation tasks:

- Storage and migration — `common/bson/column/ColumnDocumentUtility.java` (new, D26) with
  `generateBucketsFromRequest()` refactored onto it; `CalculationsDataFrameDocument` retype and
  conversions (D25); `CalculationsDocument.frameColumnNamesMap()` / `diffCalculations()` over
  `ColumnDocumentBase` (near-mechanical — `getName()` and proto equality live on the base);
  `V4StampColumnDiscriminators` (new, buckets + calculations) +
  `SchemaMigrationRunner.MIGRATIONS` + `SCHEMA_VERSION = 4` + `doc/schema-migration.md`
  including the long-scan operator guidance (D27).
- Provenance — `ColumnProvenanceDocument.derivedFrom` plus new `ColumnSourceDocument` /
  `CalculationsColumnDocument` (D29); codec registrations (embedded helpers before parents);
  shared column-metadata validator extraction with field-path parameter (D28/D29),
  `IngestionValidationUtility` delegating.
- Validation — `validateSaveAnnotationRequest` per D28; `validateExportDataRequest` per
  D30/D31 (three-source rule, `validateDataBlock()` helper, `dataSetId` ObjectId check).
- Export — `ExportDataDispatcher.handleReject` (D30); `ExportDataJobBase`: `lookupDataSet`,
  reject routing, dataBlocks merge, filename fallback, `RuntimeException` guard (D30/D31/D33);
  `ExportDataJobAbstractTabular`: `NonScalarColumnException` catch → reject (D30);
  `TabularDataUtility.addCalculationsToTable`: narrowing + typed iteration (D33);
  `DataExportHdf5File.writeCalculations` (D32); `MongoAnnotationHandler.handleExportData`:
  `return` after the defensive error sends (finding 9).
- Client — `AnnotationClient.ExportDataRequestParams` / `buildExportDataRequest` gain
  `dataBlocks` (D31).
- Tests — `AnnotationTestBase`: `verifyCalculationsDocumentHdf5Content` reads the encoding tag
  and parses by it (the bucket verifier's two-case encoding switch needs the same extension);
  typed-column calculations builders. `AnnotationCalculationsIT`: typed save/get round-trip
  (representative set — Double, String, Enum, DoubleArray, Struct, plus legacy and serialized —
  with column metadata including `derivedFrom`); HDF5 export of all; CSV scalar-only happy
  path; CSV-with-array reject; count-mismatch, duplicate-frame-name, and duplicate-column-name
  rejects. `ExportDataIT`: inline dataBlocks (CSV and HDF5), each source combination,
  no-source reject, malformed-`dataSetId` reject, not-found rejects. Migration: V4 test beside
  V2/V3's (stamps missing `_t` in both collections, leaves stamped entries alone, leaves
  v1-shaped buckets without a `dataColumn` subdocument untouched, idempotent; no new
  collection, so the reflection-pinned managed-collection list is unaffected), plus the
  real-container verification from D27. Ingestion: `derivedFrom`
  round-trip through the bucket path (extend `IngestDataColumnMetadataIT`). Unit tests for the
  shared dispatch helper and shared metadata validator.
- Docs — CLAUDE.md: calculations typed-column invariants (v4, D25–D29 outcomes), export
  classification, shared-dispatch step in the "Systematic Process" section;
  `doc/release-notes/rel-1.16.0.md`: typed calculations columns and inline `dataBlocks`
  (features), legacy count-check narrowing (D28), export reject classification (D30), HDF5
  encoding tag (D32), and the v4 migration's one-time bucket scan with its startup
  choreography (D27).

## Upgrade note (required for D3)

The `comment` → `description` rename changes a stored field name and a text index. **This is
delivered by the migration mechanism in
[#254](https://github.com/osprey-dcs/dp-service/issues/254), not by hand-run shell commands** — see
[Migration strategy](#migration-strategy-resolved-by-254) below.

Its version-1 migration performs both halves: a `$rename` of `comment` → `description` on the
`annotations` collection, and a drop of the old compound text index. The drop identifies the index
by the presence of `comment` in its `weights` document — not by name (the default-derived
`name_text_comment_text_event.description_text_ownerId_1` may differ if the index was ever created
explicitly), and not by key spec, which cannot work: MongoDB stores every text index with the same
key document and moves the indexed text fields into `weights`, so the old and new indexes' keys are
identical.

**The drop is not optional, and it is not merely a performance matter.** Mongo permits only one text
index per collection, so on an existing deployment `createMongoIndexesAnnotations()` cannot create
the new index while the old one exists — it fails. #254 therefore orders the migration runner before
all `createMongoIndexes*()` calls in `MongoClientBase.init()`. The earlier draft of this note, which
treated the stale index as write overhead the service tolerates, was wrong on this point.

The replacement index the service then builds is text over `name` + `description` with ascending
`ownerId` — `event.description` is dropped per D9.

Verify the end state with `db.annotations.getIndexes().map(i => i.name)`, and the applied schema
version with `db.serviceMetadata.findOne({_id: "schemaVersion"})`.

Annotations saved before the migration and not migrated read back with a null description; no error,
just a missing field — which is exactly the failure mode this repo treats as the serious one, since a
caller cannot tell an unmigrated record from one saved without a description. That is the reason the
mechanism fails closed rather than logging and continuing.

## Migration strategy (resolved by #254)

**Resolved 2026-09-02.** This was an open question blocking Phase 1's merge; it is now owned by
[#254](https://github.com/osprey-dcs/dp-service/issues/254), planned at
[`plan/tickets/254/plan.md`](../254/plan.md).

The outcome, for Phase 1's purposes:

- Option (2) — startup migration with a schema-version marker — was chosen, **with refuse-to-start**.
  Options (1) and (3) both fail open, and this change's own failure mode is already silent.
- The interim runbook is **not** used. #254 lands first and Phase 1 consumes its version-1 migration.
- Refuse-to-start required more than a policy decision: `GrpcServerBase.initService_()` is `void` and
  all three servers log-and-return on init failure while `start()` binds the port anyway. #254 fixes
  that, which also fixes a pre-existing bug in which a failed Mongo init leaves a service serving
  requests against an uninitialized handler.

**#254 merged 2026-09-02 (PR #255), which discharges the blocker — and tightened the coupling in a
way this plan's first draft did not anticipate.** #254 shipped three of the rename's four halves:
the version-1 migration, the `BsonConstants` key, and the `MongoClientBase` text index all say
`description`. The fourth half — the `AnnotationDocument` POJO field — belongs to this ticket and
still reads and writes `comment`: there are no `@BsonProperty` annotations anywhere in the
codebase, so the POJO codec derives the BSON field name from the Java property name. If that
combination ever ran, every post-migration save would land in `comment` while the text index and
every `TextCriterion` search consult `description` — silently empty text-search results,
intermittently "healed" by the idempotent `$rename` on the next restart. It cannot run today only
because `main` does not compile. Two consequences:

- Phase 1's D3 rename is **required to make the already-merged migration correct**, not
  modernization. The stash's `AnnotationDocument` rename (see
  [Work already done](#work-already-done)) is the missing half.
- **No release may be cut between #254 and Phase 1.** The latest release is 1.15.0, pre-#254, so
  nothing inconsistent has shipped — and this same fact is what holds D9's no-new-migration window
  open.

## Work already done

Phase 1 is partially drafted, in `stash@{0}` (created on the now-superseded
`issue-248-phase-a-restore-compilation` branch; work continues on
`issue-248-phase-1-restore-compilation`, based on post-#254 `main`) — the client retargeting, the
D3 storage rename, and the `toAnnotation()` / dispatcher rewrite including the N+1 removal. Resume
there rather than restarting — but sized honestly: the stash covers five files and none of the four
largest error clusters (`DataSetDocument`, `AnnotationServiceImpl`, `MongoSyncAnnotationClient`,
`AnnotationValidationUtility`), and its base (`7be9a05`) predates #254. It touches none of #254's
files, so it applies cleanly onto a branch off current `main`; its `AnnotationDocument` rename is
the half that closes the migration inconsistency described in
[Migration strategy](#migration-strategy-resolved-by-254).

## Out of scope

- **Sample Status API** — unaffected by #132.
- **`queryProviders` empty criteria** (`QueryServiceImpl:591`) — still rejects; it is a Query
  Service method and was out of #245's scope too. Worth a follow-on ticket.
- **Opaque tokens for the three metadata queries** — D6; follow-on.
- **Sweeping already-orphaned Calculations documents** — D8; separate, deployment-dependent.

## Dependencies and sequencing

Phase 1 blocks everything, in this repo and not only this ticket: no PR against `main` can show a
green build until it lands.

Phases 2 and 4 are independent of each other and of Phase 3. Phase 3's paging work touches the two
query methods Phase 1 also touches, so it should follow Phase 1 rather than run beside it.

dp-grpc is already merged (`6dfff3f`); there is no upstream dependency remaining.

**The #254 blocker is discharged** — it merged 2026-09-02 as PR #255 and Phase 1 consumes its
version-1 migration. What replaces it is a constraint, not a dependency: **no release between #254
and Phase 1**, because the merged migration renames a stored field that the POJO on `main` still
writes under its old name — see
[Migration strategy](#migration-strategy-resolved-by-254). `main` stays red until Phase 1 lands.
