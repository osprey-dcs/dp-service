# Plan: schema migration mechanism (issue #254)

- **Ticket**: [osprey-dcs/dp-service#254](https://github.com/osprey-dcs/dp-service/issues/254)
- **Parent**: [osprey-dcs/data-platform#83](https://github.com/osprey-dcs/data-platform/issues/83)
- **Blocks**: [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 1, whose
  `comment` → `description` rename is the first change that needs a migration. See
  [`plan/tickets/248/plan.md`](../248/plan.md), section "Migration strategy (unresolved)" — this
  plan resolves it.
- **Prior art**: `common/bson/bucket/BucketSpanVerifier.java` (#197) — a startup check that records
  its outcome in a marker document. Structurally similar, but see [D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim) for why its
  concurrency posture does **not** transfer.
- **Status**: triaged and implemented 2026-09-02 against `0208d09`. Three findings during
  implementation changed the design; see [Implementation findings](#implementation-findings).

## Overview

The service has no schema migration mechanism: no version marker, no runner, no migration, and no
`dropIndex` call anywhere in the repo. Every schema change to date has been absorbed by there being
no deployment that mattered. #248 Phase 1 ends that.

This ticket delivers the mechanism, its first real migration, and the startup plumbing that makes a
version mismatch fail closed rather than open.

## Background / triage findings

Four findings change the work. Two of them contradict assumptions in the ticket as filed, and one
uncovers an existing bug that this ticket must fix in order to deliver its central property.

### 1. "Refuse to start" is not a policy decision — the plumbing does not exist

The ticket presents refuse-to-start as an open sub-question, implying it is a behavior to choose.
It is not available to choose today.

`GrpcServerBase.initService_()` is declared `void` (`common/server/GrpcServerBase.java:45`), and all
three implementations handle failure by logging and returning:

```java
// annotation/server/AnnotationGrpcServer.java:42-45  (query/:41-44, ingest/:42-45 identical)
if (!serviceImpl.init(handler)) {
    LOGGER.error("initService serviceImpl.init failed");
    return;
}
```

`start()` calls `initService_()` and then unconditionally proceeds to build and bind the server
(`GrpcServerBase.java:57-85`). The `return` exits `initService_()`, not `start()`.

**This is an existing bug, independent of migrations.** A service whose Mongo connection or handler
init fails today logs one error line and then starts listening, serving requests against an
uninitialized handler. Nothing in the repo exercises this path. Delivering "fails closed" therefore
requires fixing it, which is why [D3](#d3-refuse-to-start-is-built-by-fixing-initservice_-not-by-routing-around-it) folds the fix in rather than working around it.

### 2. Every service creates every collection — so the runner runs in all three processes

`MongoClientBase.init()` (`:400-470`) initializes all ten collections and creates all their indexes,
unconditionally. Ingestion, query, and annotation all reach it:

| Service | Call site |
|---|---|
| Ingestion | `MongoIngestionHandler.java:62` (`mongoIngestionClient.init()`), `:66` (`mongoQueryClient.init()`) |
| Query | `MongoQueryHandler.java:70` |
| Annotation | `MongoAnnotationHandler.java:83`, `:87` |

The annotation service initializes **two** clients, each of which runs the full `init()`.

Consequence: a migration runner placed on this path executes in every service process, and in the
annotation service twice. **Concurrent startup against one database is the normal case, not an edge
case** — the documented deployment starts three JVMs. This is the finding that forces [D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim).

It also means the mechanism cannot be scoped to one service. A migration is a property of the
database, and all three services share one.

### 3. `BucketSpanVerifier`'s concurrency posture does not transfer

The ticket correctly identifies `BucketSpanVerifier` as the shape to copy for a marker document, and
for the marker it is. But its read-check-write is deliberately unsynchronized
(`BucketSpanVerifier.java:228-236` reads, `:332-341` upserts), and that is safe there for two
reasons that a migration runner has neither of:

- **Its work is idempotent and read-only.** Two processes scanning buckets concurrently waste effort
  and reach the same answer. Two processes running `$rename` concurrently do not.
- **Its failure mode is a lost optimization.** `verifyBucketSpans()` returns `true` unconditionally
  and documents this: *"verification never blocks startup; a failure disables the optimization
  instead."* A failed migration is data corruption.

Copy the marker; do not copy the locking model.

### 4. The first migration's surface is two lines of main source

The `comment` → `description` rename touches:

- `common/bson/BsonConstants.java:54` — `BSON_KEY_ANNOTATION_COMMENT = "comment"`
- `common/mongo/MongoClientBase.java:256` — the compound text index
  (`name` / `comment` / `event.description` text + ascending `ownerId`)

Those are the only two references in `src/main` and `src/test`. The stored-data half is a
`$rename` over the `annotations` collection; the index half is a drop of the old compound text index.

**Mongo permits only one text index per collection.** The new index cannot be created until the old
one is dropped, so on an existing deployment the `createIndex()` call at `:252-258` fails today
rather than silently leaving a stale index alongside the new one. This makes the annotations text
index a *sharper* case than the ticket's general "stale index costs write overhead" framing: it is
not a performance regression, it is a startup failure. It also means the drop must be ordered before
the index creation in `init()`, which [D5](#d5-migrations-run-before-index-creation-and-index-changes-are-migration-steps) addresses.

## Implementation findings

Three things found while building it changed the design. All were caught by running against a real
MongoDB rather than by reading the driver docs.

### A. A text index is not identified by its key document

[D5](#d5-migrations-run-before-index-creation-and-index-changes-are-migration-steps) said to drop the stale index "by key specification, not by name". Verified against MongoDB
8.0, that does not work. Mongo stores a text index as:

```js
{
  key:     { _fts: "text", _ftsx: 1, ownerId: 1 },   // the indexed text fields are NOT here
  name:    "name_text_comment_text_event.description_text_ownerId_1",
  weights: { comment: 1, "event.description": 1, name: 1 }   // they are here
}
```

The old and new indexes therefore have **identical key documents** and differ only in `weights`. A
key-spec match would find both or neither. Matching the literal default name is also wrong, for the
reason D5 already gave: an index created explicitly may carry another name.

The migration matches on **the presence of `comment` in `weights`**, which is what actually
distinguishes the two. `V1AnnotationCommentToDescriptionTest.testIdentifiesTheIndexByWeightsNotByName`
pins this by creating the index under a non-default name.

The same run confirmed D5's central claim: creating the new text index while the old one exists fails
with `IndexOptionsConflict`, so the drop is a startup prerequisite rather than a tidiness matter.

### B. The index declaration had to move with the migration, so the BSON constant moved too

The end-to-end upgrade test initially showed the migration dropping the old index and
`createMongoIndexesAnnotations()` immediately **recreating it over `comment`** — because
`BsonConstants.BSON_KEY_ANNOTATION_COMMENT` still read `"comment"`. Version 1 was recorded as applied
while the schema was only half changed, and the rebuilt index covered a field no document had.

This plan had assigned the whole rename to #248 Phase 1. That boundary does not hold: the index
declaration and the migration that replaces the index are one change. So #254 also renames
`BSON_KEY_ANNOTATION_COMMENT` → `BSON_KEY_ANNOTATION_DESCRIPTION = "description"` and the single index
line that uses it — two lines, touching no proto accessor.

**Decided by the ticket owner on 2026-09-02**, choosing this over shipping a knowingly half-applied
migration or splitting the change across two migration versions.

What remains #248 Phase 1's is the `AnnotationDocument.comment` **field** and its proto accessors,
which are entangled with the `SaveAnnotationRequest` changes Phase 1 must make anyway.

### C. `Instant` round-trips through BSON as `java.util.Date`

`Document.get(key, Instant.class)` throws `ClassCastException` on a value written as `Instant`, since
BSON has one date type and the driver decodes it to `Date`. Left unhandled this escaped
`SchemaVersionMarker.read()` unchecked and would have surfaced as something other than a marker
problem. `readInstant()` accepts both. Caught by `SchemaVersionMarkerTest`.

## Design decisions

### D1 — Absence of a marker is resolved by emptiness, not by assumption

A database with no version marker is either a fresh install or a pre-migration deployment, and
nothing in the data distinguishes them.

- **No marker, and every managed collection is empty** → fresh install. Stamp at the current version;
  run nothing.
- **No marker, and any managed collection holds a document** → legacy database. Run every migration
  from version 0.

Rejected alternatives:

- *No marker always means version 0.* Never silently skips, but every fresh install and every test
  database runs the accumulating migration list against empty collections. That cost and risk grow
  with each migration for no benefit.
- *No marker means refuse to start.* Safest, but breaks every fresh install and every test database
  until an operator runs a stamping command.

The emptiness probe uses `estimatedDocumentCount()` on each of the ten collections named by
`MongoClientBase.COLLECTION_NAME_*`, short-circuiting at the first non-zero. It is O(collections),
not O(documents) — `estimatedDocumentCount()` reads collection metadata.

**The probe must enumerate collections from the `COLLECTION_NAME_*` constants**, not a hand-copied
list. A collection added later and omitted from a copied list makes a populated database look fresh,
which stamps it as migrated and skips every migration — the exact silent failure this ticket exists
to prevent. A single static list adjacent to the constants, asserted in a unit test against the
constants, is the guard.

### D2 — Scope is mechanism *plus* the first migration

#254 delivers the marker, the runner, the startup plumbing, the `comment` → `description` migration
including its index drop, and operator documentation. #248 Phase 1 then consumes it.

Rejected: shipping the mechanism with only a synthetic test migration and letting #248 add the real
one. It keeps each diff smaller, but a migration framework whose only exercise is a fixture is a
framework validated against the easy case. The `comment` → `description` migration is the one that
surfaces the text-index ordering problem in [finding 4](#4-the-first-migrations-surface-is-two-lines-of-main-source),
and that problem should be found here, not in #248.

Also rejected: the interim runbook. It is what #248's plan currently assumes, and it ships exactly
the fail-open behavior the ticket argues against.

### D3 — Refuse-to-start is built by fixing `initService_()`, not by routing around it

`initService_()` changes from `void` to `boolean`; the three implementations return the result of
`serviceImpl.init(handler)`; `GrpcServerBase.start()` aborts before binding the port if it is false.

This fixes the existing bug in [finding 1](#1-refuse-to-start-is-not-a-policy-decision-the-plumbing-does-not-exist)
along with delivering the migration behavior. Rejected: a narrow abort path used only by the
migration check. It is a smaller diff but leaves two different startup-failure behaviors in the
codebase — a failed migration stops the service while a failed Mongo connection does not — and the
inconsistency is the kind that gets discovered during an incident.

**How `start()` aborts matters.** It must throw, not return: `main()` calls `start()` then
`blockUntilShutdown()`, and a silent return from `start()` leaves `server` null, so
`blockUntilShutdown()` falls straight through to `finiService_()` and the process exits 0. A
failed migration must exit non-zero, or a supervisor treats it as a clean shutdown and does not
alert. Throw `DpRuntimeException` from `start()` and let it propagate out of `main()`.

### D4 — Version numbers are integers owned by the code, not derived from the project version

`SCHEMA_VERSION` is a plain incrementing `int` constant, and the migration list is an ordered list of
`(version, description, Migration)` entries. The `comment` → `description` migration is version 1;
the implicit pre-migration state is version 0.

Rejected: deriving the schema version from the Maven project version (`1.16.0`). Most releases change
no schema, so the two version lines move at different rates, and coupling them means either bumping
the schema version on every release or maintaining a mapping. An integer that increments only when
the schema changes is self-describing.

The runner **refuses to start on a marker version higher than `SCHEMA_VERSION`** — that is a database
written by a newer service than the running binary, i.e. a rollback. Its data may already use fields
this binary does not understand, so this is the case where continuing is most likely to corrupt
silently. Downgrade migrations are not supported and are out of scope.

### D5 — Migrations run before index creation, and index changes are migration steps

Index drops are versioned migration steps, not a separate reconciliation pass. They carry the same
ordering and record-keeping requirements as data changes, and the version history makes each drop
auditable.

Rejected: reconciling live indexes against the declared set on every startup and dropping anything
unrecognized. It is self-healing and needs no migration per rename, but it would drop an index an
operator added deliberately for an ad-hoc query or a one-off investigation — destroying operator work
with no record of what was removed.

Also rejected: leaving stale indexes and documenting the manual drop. For the annotations text index
specifically this does not work at all (finding 4): the new index cannot be created while the old one
exists.

**Ordering within `init()`** — the runner is invoked after collections are initialized but **before**
any `createMongoIndexes*()` call:

```
initMongoClient / initMongoDatabase
  → init all collections (initMongoCollection*)
  → runMigrations()          <-- new
  → createMongoIndexes*()     (unchanged, still purely additive)
```

This keeps `createMongoIndex*` additive-only, as it is today, and means the version-1 migration drops
the old text index just before `createMongoIndexesAnnotations()` creates the replacement.

The drop is **by neither name nor key document** — see
[implementation finding A](#a-a-text-index-is-not-identified-by-its-key-document), which corrected
this decision's original instruction. A text index's stored key is `{_fts, _ftsx, ...}` with the
indexed fields in a separate `weights` document, so the old and new indexes have identical keys. The
migration enumerates `listIndexes()`, matches on `comment` appearing in `weights`, and drops by the
name it finds — never a hardcoded name, since the default derivation
(`name_text_comment_text_event.description_text_ownerId_1`) differs if the index was ever created
explicitly. A drop that matches nothing is not an error: on a fresh database there is nothing to
drop, and [D7](#d7-every-migration-is-individually-idempotent) requires each migration to tolerate that.

### D6 — Concurrent startup is coordinated by an atomic marker claim

Three services start against one database ([finding 2](#2-every-service-creates-every-collection-so-the-runner-runs-in-all-three-processes)),
so two processes can reach the runner simultaneously. `BucketSpanVerifier`'s unsynchronized
read-check-write is not adequate here ([finding 3](#3-bucketspanverifiers-concurrency-posture-does-not-transfer)).

The runner claims the right to migrate with a **conditional update on the marker document**, relying
on the atomicity of a single-document update:

1. `findOneAndUpdate` on `_id: "schemaVersion"` with filter `{ version: <observed>, migrating: { $ne: true } }`,
   setting `migrating: true`, `migratingSince: <now>`, `migratingHost: <host/pid>`.
2. The process whose update matched runs the pending migrations, then sets `version: <new>`,
   `migrating: false` in one update.
3. A process whose update did not match waits, polling the marker until `migrating` is false and the
   version is current, up to a bounded timeout. On timeout it **fails to start** — the same
   fail-closed posture as a mismatch, because it cannot establish that the schema is what it needs.

A `migrating: true` marker left behind by a crashed process blocks startup until an operator clears
it. That is deliberate: a half-applied migration is precisely the state where automatic recovery
guesses, and the operator documentation ([task 7](#7-operator-documentation)) covers clearing it.
`migratingSince` and `migratingHost` are recorded so the operator can tell a genuinely stuck
migration from one that is merely slow.

**A single-document conditional update is the only primitive available here.** Multi-document
transactions require a replica set, and the deployment does not guarantee one.

### D7 — Every migration is individually idempotent

Each migration must be safe to run twice. The marker makes re-running unlikely, not impossible: a
process can crash after applying a migration and before recording the version, and the operator
recovery path in [D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim) can then re-run it.

For version 1 both halves satisfy this naturally — `$rename` on a field that no longer exists is a
no-op, and the index drop is skipped when `listIndexes()` finds no match. Later migrations must state
their idempotency explicitly in the migration's Javadoc; this is a requirement on new migrations, not
a property the runner can enforce.

### D8 — Migrations are `MongoDatabase`-level, not POJO-level

A migration receives a `MongoDatabase` and works with `Document` and raw collection operations, not
the POJO codec. The codec is bound to the *current* class shape; a migration by definition operates
on documents written under a *previous* one. Deserializing an unmigrated `AnnotationDocument` through
a codec that expects `description` is the failure the migration is meant to prevent.

This also keeps migrations stable over time: a migration written today must still work after the
document classes have moved on, and it can only do that if it never references them.

## Implementation tasks

### 1. `common/mongo/migration/SchemaVersionMarker.java`

Read/write of the marker document in a new `serviceMetadata` collection, `_id: "schemaVersion"`.

- Fields: `version` (int), `updatedAt` (Instant), `migrating` (boolean), `migratingSince` (Instant),
  `migratingHost` (String), `appliedMigrations` (list of `{version, description, appliedAt}`).
- `readVersion(MongoDatabase)`, `claimForMigration(...)` (the conditional `findOneAndUpdate` from
  [D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim)), `recordApplied(...)`, `releaseClaim(...)`, `stampFresh(...)`.
- `COLLECTION_NAME_SERVICE_METADATA = "serviceMetadata"` on `MongoClientBase`, following the
  convention in the CLAUDE.md "Adding a New MongoDB Collection" section. No POJO registration —
  per [D8](#d8-migrations-are-mongodatabase-level-not-pojo-level) the marker is handled as a raw `Document`.

### 2. `common/mongo/migration/Migration.java`

```java
public interface Migration {
    int version();
    String description();
    void apply(MongoDatabase database) throws DpException;
}
```

`DpException` (checked) rather than a runtime exception, consistent with the CLAUDE.md convention
that a Mongo-client helper whose failure must reach the caller throws checked — here the caller is
the runner, and the decision it must be forced to make is whether to abort startup.

### 3. `common/mongo/migration/SchemaMigrationRunner.java`

- `SCHEMA_VERSION` constant and the ordered `List<Migration> MIGRATIONS`, with a unit test asserting
  the list is contiguous from 1 to `SCHEMA_VERSION` with no duplicates or gaps.
- `run(MongoDatabase)`: fresh-vs-legacy probe ([D1](#d1-absence-of-a-marker-is-resolved-by-emptiness-not-by-assumption)), version comparison, higher-version refusal
  ([D4](#d4-version-numbers-are-integers-owned-by-the-code-not-derived-from-the-project-version)), claim/apply/record/release ([D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim)), wait-and-poll for the non-claiming process.
- The managed-collection list for the emptiness probe, adjacent to the `COLLECTION_NAME_*` constants
  it enumerates, with the test described in [D1](#d1-absence-of-a-marker-is-resolved-by-emptiness-not-by-assumption).
- Config key `MongoClient.runSchemaMigrationsOnStartup` (default `true`), added to **both**
  `src/main/resources/application.yml` and `src/test/resources/application.yml` — the test file
  shadows the main one. Disabling it skips the runner entirely, for an operator who migrates out of
  band; it does **not** skip the version check, which still refuses to start on a mismatch.

### 4. `common/mongo/migration/migrations/V1AnnotationCommentToDescription.java`

- `$rename` `comment` → `description` on the `annotations` collection.
- Drop the old compound text index, matched on `weights` ([D5](#d5-migrations-run-before-index-creation-and-index-changes-are-migration-steps), finding A).
- Rename `BSON_KEY_ANNOTATION_COMMENT` → `BSON_KEY_ANNOTATION_DESCRIPTION` and the one index line in
  `MongoClientBase` that uses it (finding B) — otherwise index creation rebuilds the index the
  migration just dropped, over a field no document has.
- Refuse rather than rename when a document carries **both** fields: `$rename` overwrites the target,
  so proceeding would destroy an existing description with no record of it.
- Javadoc stating its idempotency per [D7](#d7-every-migration-is-individually-idempotent).

### 5. Wire into `MongoClientBase.init()`

Insert `runMigrations()` between collection initialization and the `createMongoIndexes*()` calls
([D5](#d5-migrations-run-before-index-creation-and-index-changes-are-migration-steps)). `init()` returns `false` on migration failure; today it unconditionally returns `true`
(`:469`).

### 6. Startup plumbing ([D3](#d3-refuse-to-start-is-built-by-fixing-initservice_-not-by-routing-around-it))

- `GrpcServerBase.initService_()`: `void` → `boolean`.
- `GrpcServerBase.start()`: throw `DpRuntimeException` when `initService_()` returns false, before
  building the server.
- `AnnotationGrpcServer:35`, `QueryGrpcServer:34`, `IngestionGrpcServer:35`: return the init result.
- `IngestionStreamHandler` has no Mongo client of its own; confirm during implementation whether its
  server needs the same treatment or is unaffected.

### 7. Operator documentation

New `docs/schema-migration.md`, linked from the README:

- How to read the current version (`db.serviceMetadata.findOne({_id: "schemaVersion"})`).
- What a version-mismatch startup failure looks like in the log, and what to do about each direction
  (binary older than database → deploy the newer binary; database older → it migrates automatically).
- How to recover from a stuck `migrating: true` marker, including how to tell stuck from slow using
  `migratingSince`/`migratingHost`.
- The fresh-vs-legacy rule from [D1](#d1-absence-of-a-marker-is-resolved-by-emptiness-not-by-assumption), so an operator restoring a backup into an empty database
  understands what will happen.

### 8. Tests

- `SchemaMigrationRunnerTest` — fresh stamps without running; legacy runs from 0; already-current is
  a no-op; higher marker version refuses; migration list contiguity ([task 3](#3-commonmongomigrationschemamigrationrunnerjava)).
- `SchemaVersionMarkerTest` — claim succeeds once and fails for a second caller against the same
  observed version; release restores.
- `V1AnnotationCommentToDescriptionTest` — rename over seeded documents; index dropped when present
  and no-op when absent; **applying twice leaves the same state** ([D7](#d7-every-migration-is-individually-idempotent)).
- Managed-collection list matches the `COLLECTION_NAME_*` constants ([D1](#d1-absence-of-a-marker-is-resolved-by-emptiness-not-by-assumption)).

Note that `MongoTestClient.init()` (`src/test/.../MongoTestClient.java:41-57`) drops and recreates
the test database on every test, so integration tests always take the fresh-install path. That is
correct, and it means **the legacy path gets no integration coverage from the existing suite** — the
unit tests above are its only coverage. Worth stating plainly rather than assuming the ITs exercise it.

## Out of scope

- **Downgrade migrations.** A database at a higher version than the binary refuses to start
  ([D4](#d4-version-numbers-are-integers-owned-by-the-code-not-derived-from-the-project-version)); reversing a migration is not supported.
- **Backfilling or reprocessing archived bucket data** — per the ticket.
- **Migrating the `bucketSpanVerification` marker** into `serviceMetadata`. It is a separate concern
  with a separate lifecycle (invalidated by a config change, not by a schema change), and merging
  them would couple two unrelated invalidation rules.
- **The rest of #248.** This ticket delivers the migration Phase 1 needs; Phase 1's compile fixes
  remain #248's.
- **Cross-service downtime coordination** beyond the startup ordering in [D6](#d6-concurrent-startup-is-coordinated-by-an-atomic-marker-claim) — per the ticket.

## Dependencies and sequencing

**#254 lands before #248 Phase 1.** Phase 1's storage rename (its D3) needs the version-1 migration
to exist, and #254's version-1 migration is written against the pre-rename schema, so it does not
depend on Phase 1's code changes. The two are separable in this direction only.

The boundary between them moved during implementation
([finding B](#b-the-index-declaration-had-to-move-with-the-migration-so-the-bson-constant-moved-too)):
#254 now carries the `BsonConstants` key and the index declaration, because those are the same change
as the index the migration replaces. **Phase 1 retains** the `AnnotationDocument.comment` field, its
getter/setter, and the proto accessors in `fromSaveAnnotationRequest` / `diffSaveAnnotationRequest` /
`toAnnotation` — all entangled with the `SaveAnnotationRequest` shape change Phase 1 must make anyway.

Until Phase 1 lands, `AnnotationDocument` writes its Java field `comment` to BSON while the text index
covers `description`. That is a real inconsistency, and it is confined to the annotations text search
path on a tree that does not compile — no deployment can reach it. Phase 1 closes it by renaming the
field. Do not deploy #254 alone to an environment that serves annotation text queries.

This ordering has a cost worth acknowledging: `main` does not compile until #248 Phase 1 lands, so
**#254 is developed and reviewed against a non-compiling `main`**. Its own sources compile — nothing
in this plan touches the annotation classes that are broken — but `mvn verify` will not pass on the
branch, and CI cannot go green until Phase 1 merges.

Two ways to handle it, to settle at implementation time:

- Branch #254 from `main` and accept a red CI on its PR, with the compile errors confirmed to be
  exactly the pre-existing #248 set and no others.
- Branch #254 on top of the drafted Phase 1 work (currently stashed on
  `issue-248-phase-a-restore-compilation`), which gives a green build at the cost of a PR whose diff
  includes Phase 1's.

The first keeps the diffs honest and is preferred unless review against a red build proves
impractical.

Nothing else blocks on #254. The mechanism is additive for every deployment that is up to date, and
the fresh-install path ([D1](#d1-absence-of-a-marker-is-resolved-by-emptiness-not-by-assumption)) means no existing test or development database changes behavior.
