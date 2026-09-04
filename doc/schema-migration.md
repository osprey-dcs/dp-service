# Schema migration

The services apply pending database schema migrations at startup, and **refuse to start** on a schema
they cannot handle. This document covers what an operator needs to know: how to read the current
version, what each startup failure means, and how to recover from an interrupted migration.

Implemented under [issue #254](https://github.com/osprey-dcs/dp-service/issues/254).

---

## Why the service stops instead of continuing

Most of this repo's serious defects share one shape: a **wrong answer rather than an error** (see the
max bucket span invariant, the `querySamples` fragment clamp, and the blank-criterion guard). A
skipped migration is another instance. The first migration renames the annotation `comment` field to
`description`; an annotation that has not been migrated reads back with a *null description* — no
error, and indistinguishable from a record legitimately saved without one.

A delivery mechanism that also failed silently would compound one silent failure with another. So the
service fails closed: if it cannot establish that the database is at the schema version it expects, it
stops rather than serving requests against data it may misread.

The practical consequence is that a bad migration becomes an **outage**, which is visible, rather than
**silent corruption**, which is not. That trade is deliberate.

---

## Reading the current state

```js
db.serviceMetadata.findOne({_id: "schemaVersion"})
```

```js
{
  _id: "schemaVersion",
  version: 1,                  // schema version the database is at
  updatedAt: ISODate("..."),
  migrating: false,            // true while a migration is in progress
  appliedMigrations: [
    { version: 1, description: "rename annotation 'comment' field to ...", appliedAt: ISODate("...") }
  ]
}
```

While a migration is running the document also carries `migratingSince` and `migratingHost`,
identifying when it started and which process holds it.

The version is a **plain incrementing integer owned by the service code**, not the Maven project
version. Most releases change no schema, so the two move at different rates.

---

## What happens at startup

| Marker | Database | Action |
|---|---|---|
| absent | empty | Fresh install. Stamped at the current version; nothing runs. |
| absent | has data | Predates this mechanism. Treated as version 0 and fully migrated. |
| present, version < binary | — | Pending migrations applied in order. |
| present, version == binary | — | Nothing to do. |
| present, version > binary | — | **Refuses to start.** |
| present, `migrating: true` | — | Waits for the holder, then proceeds — or fails after 5 minutes. |

The empty-versus-populated distinction matters and cannot be assumed either way. Treating "no marker"
as always-version-0 would make every fresh install replay an ever-growing migration list against empty
collections. Treating it as always-current would silently stamp a real unmigrated deployment as done —
exactly the failure the mechanism exists to prevent.

"Has data" is judged across every collection the services manage, and that deliberately includes
`bucketSpanVerification` — the marker written by the bucket-span check on a previous startup. A
database whose data collections have been emptied by a purge, a retention wipe, or a partial restore
still carries that marker, and it is proof the database has been served before. Counting it can only
push a database toward "populated", never toward "fresh", which is the safe direction: re-running
migrations against empty collections is harmless because every migration is idempotent, while a
legacy database mistaken for fresh is stamped as migrated with its migrations silently skipped.

**Restoring a backup into an empty database:** restore *first*, then start the service. A service
started against the empty database stamps it as a fresh install at the current version; restoring
older data underneath that marker afterwards leaves the database claiming to be migrated when it is
not. If this has already happened, delete the marker and restart — the runner will then see data with
no marker and migrate it correctly.

---

## Startup failures and what to do

### "database schema version N is newer than this service supports"

The database was written by a **newer build** than the one starting. Its data may already use fields
this binary does not understand.

**Fix:** deploy a service build of at least that schema version. Downgrade migrations are not
supported — if you must go back, restore a database backup taken at the older version.

### "schema migration version N (...) failed"

A migration failed partway. The migration claim is **deliberately left in place**, so every
subsequent startup blocks rather than serving from a database in an unknown state.

**Fix:**

1. Read the error in the service log — it names the migration and the underlying cause.
2. Inspect the database and decide whether the migration partially applied.
3. Resolve the underlying problem, or restore from backup.
4. Clear the claim (below) and restart.

### "timed out after 300s waiting for a schema migration held by ..."

Another process holds the claim, and it did not finish within five minutes. Either a migration is
genuinely still running on a large archive, or a process **crashed while holding the claim**.

**Distinguish them** using the marker:

```js
db.serviceMetadata.findOne({_id: "schemaVersion"}, {migrating: 1, migratingSince: 1, migratingHost: 1})
```

- `migratingSince` a few minutes ago, and the named host is running → a real migration; wait.
- `migratingSince` hours or days ago, or the named host is gone → a crashed process; clear the claim.

`migratingHost` is the JVM's runtime name (typically `pid@hostname`), so it identifies the process.

### "... requires migration to N, but MongoClient.runSchemaMigrationsOnStartup is disabled"

Migrations are turned off but the database is not at the expected version. Disabling that setting
skips *applying* migrations; it does **not** skip the version check.

**Fix:** enable the setting, or apply the migration out of band, then restart.

### "schema migrations and the schema version check are not supported on the async mongo client"

A warning, not a startup failure. `MongoAsyncClient` has no synchronous database handle and so cannot
run the migration runner. It is not a second database — it connects to the same one the sync clients
do, and every deployed service runs a sync client that migrates it — so a process using it is not
skipping a migration that would otherwise happen.

What it *is* skipping is the version check: such a process cannot confirm the database matches the
schema its binary expects. That is acceptable only while the async client stays off every production
path, which it is today (`MongoIngestionHandler`'s async factory is commented out, and the only
construction is in a test).

**If you see this in a deployed service, treat it as a defect**: the version check must be
implemented for that client before it carries production traffic.

---

## Clearing a stuck migration claim

Only after confirming no migration is actually running.

```js
db.serviceMetadata.updateOne(
  {_id: "schemaVersion"},
  {$set: {migrating: false}, $unset: {migratingSince: "", migratingHost: ""}}
)
```

Then restart the service. It re-reads the version and resumes from wherever the interrupted run
actually got to — the version advances after *each* migration, not once at the end, so a crash partway
through a multi-migration upgrade leaves the version at the last one that completed.

Every migration is also individually idempotent, so re-running one that had already finished is safe.

---

## Concurrent startup

The documented deployment starts three service processes (ingestion, query, annotation) against one
database, and each initializes every collection. So more than one process can reach the migration
runner at the same time — this is the normal case, not an edge case.

One process wins an atomic claim on the marker and migrates; the others wait, then proceed once the
version is current. No coordination between service processes is needed, and their start order does
not matter.

A migration holds no database-wide lock. Services that have not yet completed startup are not serving,
but any process already running against the database continues to read and write during the migration.
**Stop the services before a major upgrade** rather than relying on the claim to protect in-flight
traffic — it protects against two concurrent *migrations*, not against a service reading through one.

---

## Configuration

| Key | Default | Effect |
|---|---|---|
| `MongoClient.runSchemaMigrationsOnStartup` | `true` | Whether pending migrations are applied. |

Environment override: `DP_MONGO_RUN_SCHEMA_MIGRATIONS_ON_STARTUP`.

Setting it to `false` skips *applying* migrations, not the version check — a service whose expected
version does not match the database still refuses to start. Use it only when migrating out of band.

---

## Migration history

| Version | Change | Notes |
|---|---|---|
| 1 | Rename annotation `comment` → `description`; replace its text index | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 1 |
| 2 | Normalize annotation `tags` to lowercase/deduplicated/sorted | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 2 |

### Note on version 1

MongoDB permits only **one text index per collection**. The old index (text over
`name`/`comment`/`event.description` plus ascending `ownerId`) must therefore be dropped before the
replacement can be created — otherwise index creation fails with `IndexOptionsConflict`. The migration
runs ahead of all index creation for this reason.

The migration identifies that index by the presence of `comment` in its `weights` document, not by
name and not by key. A text index's stored key is `{_fts: "text", _ftsx: 1, ownerId: 1}` with the
indexed fields moved into `weights`, so the old and new indexes have *identical* keys, and an index
created under a non-default name would be missed by a name match.

Verify afterwards:

```js
db.annotations.getIndexes().map(i => ({name: i.name, weights: i.weights}))
db.annotations.countDocuments({comment: {$exists: true}})   // expect 0
```

If the migration reports documents carrying **both** `comment` and `description`, it stops rather than
renaming — `$rename` would overwrite the existing description and destroy data. Find them with:

```js
db.annotations.find({comment: {$exists: true}, description: {$exists: true}})
```

Resolve each by hand, then restart.

### Note on version 2

Annotation saves normalize tags (lowercase, deduplicated, sorted) as of #248 Phase 2, matching the
pvMetadata and configuration collections. This migration brings previously stored annotation tags
into line: without it, a stored mixed-case tag can never be matched by a normalized `TagsCriterion`
value — a silent empty result, not an error. The migration is a no-op scan on databases whose
annotations carry no tags or only already-normalized tags.

Verify afterwards:

```js
db.annotations.find({tags: {$exists: true}}, {tags: 1})   // all lowercase, sorted, no duplicates
```

---

## Adding a migration (developers)

1. Implement `Migration` in `common/mongo/migration/migrations/`, named `V<n><WhatItDoes>`.
2. Operate on `MongoDatabase` and raw `Document` only — **never** the POJO document classes. The codec
   registry is bound to the *current* class shape, while a migration by definition reads documents
   written under a previous one.
3. Make it idempotent, and say why in the class Javadoc. The runner cannot enforce this.
4. Add it to `SchemaMigrationRunner.MIGRATIONS` and bump `SCHEMA_VERSION`.
5. Add a test covering the change, the no-op case, and applying it twice.
6. Add a row to the history table above.

Index drops belong in migrations rather than in a startup reconciliation pass. Reconciliation would
drop an index an operator added deliberately for an ad-hoc query, with no record of what was removed.
The `createMongoIndex*` methods stay purely additive.
