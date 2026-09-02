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
  is **unblocked**. Not yet implemented; Phase 1 is partially drafted (see
  [Work already done](#work-already-done)).

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
file separately if any deployment is known to have them.

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
  out of whichever catch block happens to be nearest.

`getAnnotation` populates `Annotation.calculations` inline — the proto assigns that to
`getAnnotation()` only (see Phase 1's denormalization row) — so it re-adds the calculations fetch
`queryAnnotations` lost, this time bounded to a single annotation.

### Phase 3 — paging, ordering, and criteria semantics

Converts the two queries' Base64 skip tokens (shipped in Phase 1, D10) to opaque tokens with
reject-on-malformed per the proto contract (D6), adds the documented ordering with the activation
tiebreaker, and makes the all-AND criteria change (D4) with repeated `IdCriterion` compiling to
`$in`. The AND change needs its own release-note line: it silently changes results for
multi-criterion queries valid today.

### Phase 4 — typed calculation columns and export

`CalculationsDataFrameDocument` gains the 16 typed column types alongside `dataColumns` (§2), then
`ExportDataJobAbstractTabular` and the HDF5 path. Preserve the existing invariant: tabular formats
(CSV, XLSX) represent scalar columns only; array/binary are HDF5-only. Then inline `dataBlocks` as
an export source, and `ColumnProvenance.derivedFrom` stored-not-interpreted.

Independent of Phases 2–3 and the largest single chunk. Also carries D8.

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
