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
- **Status**: triaged 2026-09-01 against dp-service `7be9a05` and dp-grpc `6dfff3f`. Not yet
  implemented; Phase 1 is drafted (see [Work already done](#work-already-done)).

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

So the two `return null` blocks — `MongoSyncAnnotationClient:258-262` (datasets) and `:503-507`
(annotations) — are **removed**, not preserved, and the two service-layer emptiness checks
(`AnnotationServiceImpl:217-220` and the annotations equivalent) are deleted — exactly as #245 deleted the three in `Query*Job`.
Carry the same explanatory comment those three now have, since an absent validation block reads
like an omission.

`DEFAULT_QUERY_LIMIT` (`MongoSyncAnnotationClient:76`, already shared by the three metadata
queries) extends to these two. Per #245's rationale the default is **unconditional** — never applied
only when criteria are absent, or a client removing its last filter silently changes page size.

### 4. Two live fall-through bugs in the service layer, on `main` today

Independent of the proto change, and the same defect the handoff flagged in the dispatcher:

- `AnnotationServiceImpl:126` — `saveDataSet` calls `sendSaveDataSetResponseReject(...)` on a null
  `dataSet` **without `return`**, then falls through to `handler.handleSaveDataSet(...)`.
- `AnnotationServiceImpl:217-220` — `queryDataSets` does the same on its empty-criteria check.

Both send a rejection and then enqueue the job, producing a second response on a closed observer.
The second disappears with the empty-criteria removal (§3); the first must be fixed deliberately.
Neither is caught today because no test exercises the path past the first response.

The dispatcher has three more of the same shape (`QueryAnnotationsDispatcher:68`, `:83`, `:96`);
those disappear with the denormalization removal in Phase 1.

### 5. Every criterion changed shape — validation switches are rewrites

All criterion fields went singular → repeated, and three new criterion types were added to each query:

| | Old | New |
|---|---|---|
| `IdCriterion` | `string id` | `repeated string ids` |
| `OwnerCriterion` | `string ownerId` | `repeated string ownerIds` |
| `TextCriterion` | `string text` | `repeated string text` |
| `PvNameCriterion` | `string name` | exact / prefix / contains lists |
| `DataSetsCriterion` | `string dataSetId` | `repeated string dataSetIds` |
| `AnnotationsCriterion` | `string annotationId` | `repeated string annotationIds` |
| `NameCriterion`, `TagsCriterion`, `AttributesCriterion` | — | new on both queries |

The `isBlank()`-per-criterion validation in `AnnotationServiceImpl` (~60 lines for datasets, ~90 for
annotations) is rewritten wholesale. The target shape already exists: the PV-metadata and
configuration validators handle exactly these repeated types, including the `isBlankKey()` guard
from #243. Copy those rather than adapting the legacy switches.

**The #243 invariant applies to the new `prefix` / `contains` lists.** A blank string there builds
`"^" + Pattern.quote("")`, which matches everything — a silent match-all wearing the appearance of a
filter. Client-side, `AnnotationClient.nonBlank()` is the single source for this and must guard the
new dataset/annotation criterion builders too.

## Design decisions

**D1 — Phase 1 is "make it compile", and it is allowed to be mechanical.**
The repo currently has no CI signal at all. Restoring compilation is worth its own PR even though it
delivers no new capability, because every later phase is unverifiable without it. Phase 1
deliberately does *not* add RPCs, paging, or typed columns.

**D2 — Behavior changes forced by the proto go in Phase 1; optional ones do not.**
Some changes cannot be deferred: the `Annotation` message has no `dataSets` field, so the embedding
must go, and with it the N+1 fetch. Others *can* be deferred and are: paging, new RPCs, typed
columns. The test is "does `main` compile without it," not "is it in the same handoff section."

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
file separately if any deployment is known to have them.

## Phases

Each phase is one PR against #248, which stays open until the last lands.

### Phase 1 — restore compilation (no new capability)

Fixes all 58+ main-source errors and whatever the test sources add behind them.

| Area | Work |
|---|---|
| `Annotation` hoist | Retarget 5 references from `QueryAnnotationsResponse.AnnotationsResult.Annotation` to top-level `Annotation` |
| `comment` → `description` | `AnnotationDocument` field/accessors/`fromSaveAnnotationRequest`/`diffSaveAnnotationRequest`; `BsonConstants.BSON_KEY_ANNOTATION_COMMENT` → `…_DESCRIPTION`; text index in `MongoClientBase:256` (D3) |
| Denormalization removal | `AnnotationDocument.toAnnotation()` loses its `dataSetDocuments` / `calculationsDocument` parameters and returns references only — **forced**, the proto field is gone. Deletes `QueryAnnotationsDispatcher`'s per-annotation `findDataSet` loop and its 3 no-`return` bugs, plus the now-dead `mongoClient` dependency |
| Flat `SaveDataSetRequest` | `DataSetDocument.fromSaveRequest`/`diffRequest`: `request.getDataSet().getX()` → `request.getX()`; `SaveDataSetJob:55` |
| `CalculationsDataFrame.frame` | `CalculationsDataFrameDocument` and `AnnotationValidationUtility`: `frame.getX()` → `frame.getFrame().getX()` (§2 — indirection only) |
| Criterion accessors | Singular → repeated at every site in `AnnotationServiceImpl`, `MongoSyncAnnotationClient`, `MongoAnnotationHandler`, `AnnotationClient`. **Semantics preserved** (D4): a repeated field with one value behaves as the old singular one |
| Empty criteria | Remove the `return null` at `MongoSyncAnnotationClient:258-262` and `:503-507`, plus the two service-layer checks; apply `DEFAULT_QUERY_LIMIT` unconditionally (§3, D5) |
| Fall-through bug | Add the missing `return` at `AnnotationServiceImpl:126` (§4) |
| Tests | Update `AnnotationTestBase` (2467 lines), `GrpcIntegrationAnnotationServiceWrapper` (1745), `QueryAnnotationsIT`, `AnnotationCalculationsIT`, and the client ITs for the renamed field and repeated criteria |

**Exit criterion**: `mvn clean verify` green, CI green, no new RPCs, no paging, no typed columns.

### Phase 2 — entity and audit fields, new CRUD methods

`modifiedBy`, `createdTime`/`updatedTime` emission, `DataSet` tags/attributes; then `getDataSet`,
`getAnnotation`, `getCalculations`, `deleteDataSet` (D7), `deleteAnnotation`, and the two `patch*`
deferred stubs. `GetConfigurationJob` / `GetConfigurationDispatcher` are the template.

### Phase 3 — paging, ordering, and criteria semantics

Opaque tokens for the two queries (D6), documented ordering with the activation tiebreaker, and the
all-AND criteria change (D4) with repeated `IdCriterion` compiling to `$in`. The AND change needs
its own release-note line: it silently changes results for multi-criterion queries valid today.

### Phase 4 — typed calculation columns and export

`CalculationsDataFrameDocument` gains the 16 typed column types alongside `dataColumns` (§2), then
`ExportDataJobAbstractTabular` and the HDF5 path. Preserve the existing invariant: tabular formats
(CSV, XLSX) represent scalar columns only; array/binary are HDF5-only. Then inline `dataBlocks` as
an export source, and `ColumnProvenance.derivedFrom` stored-not-interpreted.

Independent of Phases 2–3 and the largest single chunk. Also carries D8.

## Upgrade note (required for D3)

The `comment` → `description` rename changes a stored field name and a text index. Existing
deployments need, after deploying Phase 1:

```js
db.annotations.updateMany({}, { $rename: { "comment": "description" } })
db.annotations.dropIndex("name_text_comment_text_event.description_text_ownerId_1")
```

Confirm the old index's actual name first with `db.annotations.getIndexes()` and drop by that name
— the literal above is Mongo's default derivation from the key spec
(`name`/`comment`/`event.description` text plus ascending `ownerId`) and may differ if the index was
ever created explicitly.

The service creates the new text index on startup but **does not drop the old one** — not by
deliberate policy, but because nothing in the codebase drops indexes at all. `MongoSyncClient`'s
`createMongoIndex*` methods only ever call `createIndex()`, and there is no migration mechanism of
any kind in the repo. See [Migration strategy](#migration-strategy-unresolved) below: this plan
should not be merged assuming hand-run shell commands are an adequate answer. Leaving the stale index costs write overhead on every annotation save and
gives the planner a competing candidate.

Verify the end state with `db.annotations.getIndexes().map(i => i.name)`.

Annotations saved before the migration and not migrated will read back with a null description; no
error, just a missing field — which is exactly the failure mode this repo treats as the serious one,
since a caller cannot tell an unmigrated record from one saved without a description.

## Migration strategy (unresolved)

**This is an open question that blocks Phase 1's merge, not a detail of it.**

The repo has no migration mechanism: no version marker, no migration runner, no
`dropIndex` call anywhere, and no record of which schema a given database is at. Every schema change
to date has been absorbed by there being no deployment that mattered.

The `comment` → `description` rename is the first change that needs one, and it arrives just as real
users do. Its properties are worth stating plainly, because they generalize:

- **It is not backward compatible.** A record written by the old code is unreadable-as-intended by
  the new code, and vice versa. There is no window where both versions can run against one database,
  so this is not a rolling deploy.
- **It fails silently.** An unmigrated annotation reads back with a null description rather than an
  error.
- **It cannot be verified after the fact from the data alone.** A null description is
  indistinguishable from a record legitimately saved without one.

Options, in rough order of cost:

1. **Documented manual runbook** (what this plan currently assumes). Cheapest, and adequate only
   while every deployment is one we operate and can take offline. It does not survive a user
   upgrading unattended, and nothing detects a skipped migration.
2. **Startup migration with a schema-version marker.** A `schemaVersion` document in a
   `serviceMetadata` collection; on startup the service compares, runs pending migrations, and
   records the new version. Follows the `bucketSpanVerification` precedent already in the codebase —
   which is exactly this pattern for a different purpose, so there is a shape to copy. Requires
   deciding whether the service refuses to start on a version mismatch it cannot handle, which is
   the safe behavior and also the one that turns a bad migration into an outage.
3. **Separate migration tool** shipped alongside the service, run deliberately. Keeps startup code
   out of the business of mutating archives — the instinct behind the sentence I wrongly attributed
   to #231 — at the cost of a second artifact and a step an operator can forget.

**Recommendation: (2), with the refuse-to-start behavior, and (1) as the interim for Phase 1 only if
Phase 1 must land before the mechanism exists.** The deciding factor is that (1) and (3) both fail
open — a skipped migration is silent — whereas (2) fails closed. Given that this change's failure
mode is already silent, layering a silent delivery mechanism on top of it is the combination worth
avoiding.

This deserves its own ticket rather than being smuggled into #248. Phase 1 can proceed on the
runbook if the mechanism lands before any real deployment upgrades; it should not proceed on the
runbook if that ordering is not guaranteed.

## Work already done

Phase 1 is partially drafted, on branch `issue-248-phase-a-restore-compilation`
(stashed, not committed) — the client retargeting, the D3 storage rename, and the
`toAnnotation()` / dispatcher rewrite including the N+1 removal. It compiles as far as the
`DataSetDocument` cluster. Resume there rather than restarting.

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
