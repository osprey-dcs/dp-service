# Schema migration

The services apply pending database schema migrations at startup, and **refuse to start** on a schema
they cannot handle. This document covers what an operator needs to know: how to read the current
version, what each startup failure means, and how to recover from an interrupted migration.

Implemented under [issue #254](https://github.com/osprey-dcs/dp-service/issues/254); the takeover of
a migration whose process has gone away under
[issue #292](https://github.com/osprey-dcs/dp-service/issues/292). To rehearse
the migrations against a copy of a production database before upgrading it, see
[schema-migration-rehearsal.md](schema-migration-rehearsal.md).

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

While a migration is running the document also carries the claim of the process running it:

```js
{
  migrating: true,
  migratingSince: ISODate("..."),      // when this process claimed, or took over, the migration
  migratingHost: "12345@dp-ingest-0",  // the JVM's runtime name, typically pid@hostname
  claimToken: "6f1c...",               // identifies this claim; changes on every takeover
  heartbeatSeq: Long("42"),            // incremented every 10 s while the process is alive
  migratingHeartbeat: ISODate("..."),  // server time of the last heartbeat
  takeoverCount: 0                     // takeovers since the version last advanced
}
```

`migratingHeartbeat` is the field to read: a value within the last few seconds means the migrating
process is alive. A claim with **no** `claimToken` was written by 1.16.0 or earlier, which has no
heartbeat.

A migration that **failed** leaves its claim in place with three more fields, and stays that way
until an operator clears it:

```js
{
  migrationFailed: true,
  migrationFailedVersion: 4,
  migrationFailedMessage: "error stamping column discriminators: ..."
}
```

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
| present, `migrating: true`, heartbeat moving | — | Waits for the holder, then proceeds, or fails after 5 minutes. |
| present, `migrating: true`, heartbeat stopped for 60 s | — | Takes the migration over and resumes from the recorded version (see [A migration whose process has gone away](#a-migration-whose-process-has-gone-away)). |
| present, `migrating: true`, `migrationFailed: true` | — | **Refuses to start** at once. |
| present, `migrating: true`, no `claimToken` | — | A 1.16.0 claim. Waits, then fails after 5 minutes; never taken over. |

The empty-versus-populated distinction matters and cannot be assumed either way. Treating "no marker"
as always-version-0 would make every fresh install replay an ever-growing migration list against empty
collections. Treating it as always-current would silently stamp a real unmigrated deployment as done —
exactly the failure the mechanism exists to prevent.

"Has data" is judged across every collection the services manage, and that deliberately includes
`bucketSpanVerification` — the marker written by the startup bucket-span check that #232 removed.
Nothing writes it any more and migration v5 drops it, but the name is kept as a legacy constant so
a database written by an earlier build is still classified correctly: one whose data collections
have been emptied by a purge, a retention wipe, or a partial restore may still carry that marker,
and it is proof the database has been served before. Counting it can only
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

A migration failed partway. The migration claim is **deliberately left in place and marked
`migrationFailed`**, so every subsequent startup blocks rather than serving from a database in an
unknown state. A failed claim is never taken over by another process: the migration would fail
again.

The process whose migration failed reports the message above. Every other process, and every later
startup, reports *"schema migration version N failed in ... and its claim is held for an operator"*
with the stored failure message, at once and without waiting.

**Fix:**

1. Read the error in the service log, or `migrationFailedMessage` on the marker. It names the
   migration and the underlying cause.
2. Inspect the database and decide whether the migration partially applied.
3. Resolve the underlying problem, or restore from backup.
4. Clear the claim (below) and restart.

If the message ends *"The failure could NOT be recorded on the marker"*, the write that marks the
claim failed too, most likely in the same outage. That claim is not marked, so it reads as
abandoned and a starting process will take it over and run the migration again, at most three
times.

### "timed out after 300s waiting for a schema migration held by ..."

Another process holds the claim, and it did not finish within five minutes. The rest of the message
says which of four cases this is.

**"Its heartbeat is current, so the migration is still running."** A real migration on a large
archive. Wait for it, then restart this service (a supervisor that restarts on failure does this by
itself). Do not clear the claim.

**"Its heartbeat has stopped ... but N operation(s) ... are still running on the server."** The
process that held the claim is gone, but an operation it sent is still running: a client that is
killed does not take its operation with it. The claim will be taken over by a starting process as
soon as that operation ends, so restarting is enough. The message names the operation, how long it
has run, and the `db.killOp(...)` that ends it early. Ending v4's update early loses nothing (the
takeover stamps what is left); ending v5's aggregate discards its work, and the takeover starts
that scan again.

**"... the check for its operations still running on the server failed."** The claim looks
abandoned, but the service could not list its own running operations, so it refused to take over.
See [the privilege a sharded deployment needs](#the-privilege-a-sharded-deployment-needs).

**"The claim has no heartbeat: it was written by a service build that predates migration
takeover."** A 1.16.0 process holds or left the claim. Nothing can tell whether it is alive, so it
is never taken over. Distinguish by hand:

```js
db.serviceMetadata.findOne({_id: "schemaVersion"}, {migrating: 1, migratingSince: 1, migratingHost: 1})
```

- `migratingSince` a few minutes ago, and the named host is running → a real migration; wait.
- `migratingSince` hours or days ago, or the named host is gone → a crashed process; clear the claim.

`migratingHost` is the JVM's runtime name (typically `pid@hostname`), so it identifies the process.

### "the schema migration from version N has been abandoned by 4 processes in a row"

The claim was taken over three times and every process that held it went away without the schema
version advancing. It will not be taken over a fourth time. Something is ending the migrating
process each time, and another attempt would meet it again:

- a liveness or startup probe that kills a pod which has not bound its port yet (a long migration
  holds startup for its whole length);
- an out-of-memory kill, of the container or by `-XX:+ExitOnOutOfMemoryError`;
- the migration itself ending the process.

**Fix:** find and remove the cause first (the previous pods' exit reasons and logs say which), then
clear the claim (below) and restart. Clearing resets the count.

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

## A migration whose process has gone away

Before [#292](https://github.com/osprey-dcs/dp-service/issues/292), a process killed while it
migrated left its claim held forever, and every later startup failed until an operator cleared it.
The claim is now a lease. The migrating process increments `heartbeatSeq` every 10 seconds. A
starting process that finds the claim held watches that number; if it has not changed for 60
seconds, the holder is gone, and the waiting process takes the claim over and resumes from the
recorded version. Nothing compares clocks between hosts: the waiter measures the 60 seconds itself,
so it always waits that long, even for a process that died days ago.

Three things stop a takeover, and each is deliberate:

| The claim | What a starting process does |
|---|---|
| is marked `migrationFailed` | Refuses to start at once. A migration that threw would throw again. |
| has an operation still running on the server | Waits for it (checked every 5 s), against the five-minute limit. |
| has been taken over three times at this version | Refuses to start. See the "abandoned by 4 processes" failure above. |

**The second one matters on a large archive.** MongoDB does not stop an operation when its client
dies (measured: an `updateMany` and a `$group`/`$merge` aggregate both ran to completion after
`kill -9` of the client, on a standalone and through mongos). A takeover that started beside one
would add a second full scan of `buckets`, and a third after the next kill. Every operation a
migration issues therefore carries the comment `dp-schema-migration:<claimToken>`, and a process
about to take over first looks for any operation with that prefix:

```js
db.getSiblingDB("admin").aggregate([
  {$currentOp: {allUsers: false}},
  {$match: {"command.comment": /^dp-schema-migration:/, ns: /^dp\./}}
])
```

(`dp` is the service database name. Run it as the service user; `allUsers: false` lists that user's
own operations.) An `opid` it reports can be ended with `db.killOp(<opid>)` by a user allowed to;
through mongos the opid is a string such as `"shard01:12345"`.

A process that was only **stalled**, not dead, and is taken over, stops at its next write to the
marker and exits with *"another process ... took the schema migration over from this one"*. That is
the expected end of a displaced process, not a second failure.

**This does not stop a probe from killing the migrating pod.** If a liveness probe kills each
migrating process in turn, each takeover is killed the same way and the cap is reached. v4's update
keeps the work of each attempt; v5's aggregate starts over. A waiting pod also has to survive the
60 seconds itself to take over at all. The lasting fix is a probe that allows for the migration.

### The privilege a sharded deployment needs

On a standalone or a replica set, any user may list its own operations and nothing needs granting.
**Through mongos, `$currentOp` is refused unless the user holds the `inprog` action**, own
operations included (measured on 8.0.26). Without it the check above cannot run, every takeover is
refused, and an abandoned claim needs the manual clear below, exactly as before #292. To enable
takeover on a sharded deployment, grant the service user the action once:

```js
db.getSiblingDB("admin").createRole({
  role: "dpMigrationInprog",
  privileges: [{resource: {cluster: true}, actions: ["inprog"]}],
  roles: []
})
db.getSiblingDB("admin").grantRolesToUser("<service user>", [{role: "dpMigrationInprog", db: "admin"}])
```

`inprog` lets that user see every user's running operations, not only its own. The service itself
only ever asks for its own.

**Every service start checks for this and says so.** A service that cannot run the check logs, at
`warn`, *"this service cannot take over an abandoned schema migration: ..."* with the reason, and
starts normally. Look for that line after deploying to a sharded cluster, so that the missing
grant is found then and not when a claim is stuck.

---

## Clearing a stuck migration claim

Needed after a failed migration, at the takeover cap, for a claim left by a 1.16.0 process, and on
a sharded deployment without the `inprog` privilege. An abandoned claim otherwise clears itself.

**First confirm no migration is running.** The holder must be gone (`migratingHeartbeat` not
advancing over 30 seconds, or for a 1.16.0 claim the named host not running), and no operation of
a previous holder may still be running on the server:

```js
db.getSiblingDB("admin").aggregate([
  {$currentOp: {allUsers: true}},
  {$match: {"command.comment": /^dp-schema-migration:/, ns: /^dp\./}}
])
```

(`dp` is the service database name.) Run this one as an administrator: `allUsers: true` lists every
user's operations and needs the `inprog` privilege on a standalone or replica set as well as
through mongos, where the service's own check above needs it only through mongos. It is the form
to use here because it does not depend on connecting as the service user. Without such a user, run
the `allUsers: false` query from
[A migration whose process has gone away](#a-migration-whose-process-has-gone-away) as the service
user instead.

Wait for anything this lists to finish, or end it with `db.killOp(<opid>)`. An operation started by
1.16.0 carries no comment; look for a long-running `update` or `aggregate` on `buckets` instead.
Then:

```js
db.serviceMetadata.updateOne(
  {_id: "schemaVersion"},
  {
    $set: {migrating: false},
    $unset: {
      migratingSince: "", migratingHost: "",
      claimToken: "", heartbeatSeq: "", migratingHeartbeat: "", takeoverCount: "",
      migrationFailed: "", migrationFailedVersion: "", migrationFailedMessage: ""
    }
  }
)
```

Unset every field, as shown. (The command in the 1.16.0 version of this document unset only the
first two. A marker cleared that way is still claimed correctly by this build, which removes the
rest itself.)

**After the 1.16.0 form of the clear, do not let a 1.16.0 process claim the migration.** That form
leaves `claimToken` and `heartbeatSeq` on the marker, and a 1.16.0 claim does not remove them. The
claim then carries a token whose heartbeat never moves, and a process of this build takes it over
after 60 seconds although the 1.16.0 migrator is alive: its operations carry no comment, so the
check for running operations does not see them. The two then migrate at once. Use the command
above, which removes the token, or upgrade every service before restarting.

Then restart the service. It re-reads the version and resumes from wherever the interrupted run
actually got to — the version advances after *each* migration, not once at the end, so a crash partway
through a multi-migration upgrade leaves the version at the last one that completed.

Every migration is also individually idempotent, so re-running one that had already finished is safe.

**Clearing the claim of a process that is alive stops that process**, whichever form of the clear
was used. It finds its claim gone at its next write to the marker and exits with *"the migration claim was cleared or released while this process held
it"*. The operation it had already sent keeps running on the server. A 1.16.0 process is not
stopped this way; it carries on and records its versions.

---

## Before a release upgrade

Take a restorable backup first. There are no downgrade migrations, and a binary rolled back to the
previous release does not refuse a migrated database — it misreads it (a renamed field reads as
absent, a dropped marker collection re-triggers work the old build did at startup). Restoring the
pre-upgrade backup is the only supported way back, and it discards anything written after the
upgrade began. Rehearse the migrations against a restored copy beforehand:
[schema-migration-rehearsal.md](schema-migration-rehearsal.md).

## Concurrent startup

The documented deployment starts three service processes (ingestion, query, annotation) against one
database, and each initializes every collection. So more than one process can reach the migration
runner at the same time — this is the normal case, not an edge case.

One process wins an atomic claim on the marker and migrates; the others wait, then proceed once the
version is current. No coordination between service processes is needed, and their start order does
not matter. If the process that won goes away, one of the others takes the migration over (see
[A migration whose process has gone away](#a-migration-whose-process-has-gone-away)).

A migration holds no database-wide lock. Services that have not yet completed startup are not serving,
but any process already running against the database continues to read and write during the migration.
**Stop the services before a major upgrade** rather than relying on the claim to protect in-flight
traffic — it protects against two concurrent *migrations*, not against a service reading through one.

---

## Configuration

| Key | Default | Effect |
|---|---|---|
| `MongoClient.runSchemaMigrationsOnStartup` | `true` | Whether pending migrations are applied. |
| `MongoClient.schemaMigrationHeartbeatIntervalSeconds` | `10` | How often the migrating process increments its heartbeat. |
| `MongoClient.schemaMigrationStaleAfterSeconds` | `60` | How long a waiting process must see no heartbeat before it takes the migration over. |

Environment overrides: `DP_MONGO_RUN_SCHEMA_MIGRATIONS_ON_STARTUP`,
`DP_MONGO_SCHEMA_MIGRATION_HEARTBEAT_INTERVAL_SECONDS`,
`DP_MONGO_SCHEMA_MIGRATION_STALE_AFTER_SECONDS`.

The two lease settings are checked at every startup, whether or not a migration is pending. The
service refuses to start if the interval is below 1, if stale-after is below three intervals (one
slow heartbeat would read as a dead process), or if stale-after is not below the five-minute claim
wait (no waiting process would last long enough to take over). The takeover cap (three per schema
version) and the five-minute wait are not configurable.

Setting it to `false` skips *applying* migrations, not the version check — a service whose expected
version does not match the database still refuses to start. Use it only when migrating out of band.

---

## Migration history

| Version | Change | Notes |
|---|---|---|
| 1 | Rename annotation `comment` → `description`; replace its text index | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 1 |
| 2 | Normalize annotation `tags` to lowercase/deduplicated/sorted | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 2 |
| 3 | Canonicalize annotation `dataSetIds`/`annotationIds` to lowercase hex | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 2 review |
| 4 | Stamp `_t` discriminator on legacy bucket and calculations columns | [#248](https://github.com/osprey-dcs/dp-service/issues/248) Phase 4. **One-time full scan of `buckets`** — see note. |
| 5 | Seed per-PV `pvStats.maxBucketSpanSeconds` from `buckets`; drop `bucketSpanVerification` | [#232](https://github.com/osprey-dcs/dp-service/issues/232). **One-time full scan of `buckets`** — see note. |

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

### Note on version 3

Annotation reference ids (`dataSetIds`, `annotationIds`) are matched as **strings** by
`deleteDataSet`'s referential-integrity check and the `queryAnnotations` dataSets/annotations
criteria, while save validation parses them as binary ObjectIds, which accept either hex case. A
reference stored in a case variant therefore passed validation but was invisible to every
string-matched check — `deleteDataSet` could remove a dataset such an annotation still references.
Saves canonicalize to the lowercase hex form `ObjectId.toHexString()` emits as of the #248 Phase 2
review fixes; this migration canonicalizes previously stored references.

An entry that is not parseable as an ObjectId at all is left unchanged and logged: it can never have
referenced a real document, so rewriting it would destroy evidence without fixing anything.

Verify afterwards:

```js
db.annotations.find({$or: [{dataSetIds: /[A-F]/}, {annotationIds: /[A-F]/}]})   // expect none
```

### Note on version 4

Embedded column documents written before rel-1.13.0 predate the `_t` class discriminator that
[#173](https://github.com/osprey-dcs/dp-service/issues/173) introduced, and #173 shipped no
migration because this mechanism did not exist yet. The stored shape is otherwise identical to the
current one, but the POJO codec cannot decode a discriminator-less entry under the abstract field
type both storage sites now declare. For **buckets** the failure is fully silent: the decode error
escapes the query dispatchers mid-cursor and the client receives *zero buckets with no error* —
indistinguishable from "no data in range" — for any query covering a pre-1.13 bucket. For
**calculations**, the #248 Phase 4 retype of frame columns would make pre-1.13 calculations
unreadable the same way. This migration stamps `_t: "dataColumn"` on every embedded legacy column
missing it, in both collections.

**Expect a long run on a large archive.** There is no index on `dataColumn._t`, so the buckets
update is a one-time full collection scan — minutes to perhaps an hour at tens of millions of
buckets. Two operational consequences:

- **Waiting services will time out and must be restarted.** While the elected process migrates, the
  other services wait five minutes and then refuse to start with the held-claim timeout message.
  During a long v4 run that message says *"Its heartbeat is current, so the migration is still
  running"*: let the migrating process finish, and restart the others (a supervisor that restarts
  on failure self-heals). Do **not** clear the claim while the migrating host is alive.
- **If the migrating process is killed**, a waiting process takes the migration over once the
  killed process's update has finished on the server. That update keeps stamping after its client
  is gone, so the takeover's own update finds little or nothing left to do.
- Buckets that never held an embedded column subdocument (the v1 storage shape) and columns already
  stamped by 1.13+ builds are untouched; a re-run matches nothing.

Verify afterwards:

```js
db.buckets.countDocuments({dataColumn: {$exists: true}, "dataColumn._t": {$exists: false}})   // expect 0
db.calculations.countDocuments({dataFrames: {$elemMatch: {dataColumns: {$elemMatch: {_t: {$exists: false}}}}}})   // expect 0
```

### Note on version 5

[#232](https://github.com/osprey-dcs/dp-service/issues/232) replaced the startup full-collection
scan for the longest stored bucket (the `bucketSpanVerification` marker) with a per-PV statistic,
`pvStats.maxBucketSpanSeconds`, that the ingestion service records as it writes buckets. The query
side now derives the `firstTime` lower bound of every bucket time-range filter from that value, and
a PV with **no** `pvStats` document is treated as having a span of zero. Without a seed, every bucket
written before the upgrade would be invisible to any query whose window begins after the bucket's
first second — until the PV happened to ingest a bucket at least as long, which for a retired or
slowly sampled PV is never. This migration seeds `pvStats` from the existing archive in one
server-side pipeline (`$group` by `pvName` taking the `$max` span, then `$merge` into `pvStats`
keeping the larger of any stored and computed value), and drops the `bucketSpanVerification`
collection, which nothing reads or writes any more.

**Expect a long run on a large archive.** The seed is a one-time full scan of `buckets` — the same
order of work as v4, with the same operational consequence: **waiting services will time out and
must be restarted.** Follow the v4 guidance above: let the migrating process finish, restart the
others, and do **not** clear the claim while the migrating host is alive. Memory is bounded by the
number of distinct PVs, not the number of buckets.

**If the migrating process is killed**, its aggregate still runs to completion on the server and
writes `pvStats`; a waiting process takes the migration over only after it ends, and then runs the
whole scan again, because the pipeline is not resumable. The second run changes nothing (`$max`),
but it costs a second full scan. Avoid killing a v5 migrator.

**Stop every service.** A pre-upgrade ingestion process still writing after the seed has run
produces buckets no statistic covers, and a bucket longer than its PV's recorded span is skipped by
any query whose window begins after the bucket starts. The migration claim coordinates only the
processes that are *starting*; it does nothing to a pre-upgrade process already running, which
keeps serving against the schema the migrations change underneath it — after v1, a still-running
1.15 annotation service reads every annotation's comment as empty. If a full stop is impossible,
upgrade ingestion first and let it run the migrations, but stop or upgrade the query and annotation
services before it starts. An upgraded ingestion service against a not-yet-*started* query or
annotation service is harmless; against a still-*running* 1.15 one it is not.

A PV is left **without** a `pvStats` document — span zero, the same as a PV never seen — when every
one of its buckets is unusable for the measure: no `dataTimestamps` subdocument, or `lastTime`
before `firstTime`. No readable data is lost by that: a bucket without timestamps cannot be
deserialized on the query path in any case, and an inverted bucket still satisfies the overlap test
whenever it did before. A bucket whose `pvName` is missing, null, or not a string is skipped in the
same way, before grouping: such a value cannot be a `pvStats` `_id`, and the server would reject the
whole seed rather than that one bucket — and nothing readable is lost there either, since every
bucket query filters on `pvName`, so a bucket without a usable one is unreachable regardless. A PV
with at least one well-formed bucket is seeded normally.

Verify afterwards:

```js
db.pvStats.countDocuments()                                    // expect db.buckets.distinct("pvName").length, less any PV skipped as above
db.pvStats.find().sort({maxBucketSpanSeconds: -1}).limit(5)    // the longest recorded spans
db.getCollectionNames().includes("bucketSpanVerification")     // expect false
```

**If buckets are ever written by anything other than the ingestion service** — a direct Mongo
import, a restore that adds buckets — that writer must raise the affected PVs' statistics too, or
those buckets are subject to the same invisibility. The recourse is one write per PV, with no
restart:

```js
db.pvStats.updateOne({_id: "<pvName>"}, {$max: {maxBucketSpanSeconds: NumberLong("<seconds>")}}, {upsert: true})
```

or re-run the seed pipeline by hand (it is idempotent; the runner will not re-run it once version 5
is recorded). **Lowering or removing a value by hand requires an ingestion restart**: the ingestion
process caches a per-PV watermark and skips writes the collection would then no longer reflect.

---

## Adding a migration (developers)

1. Implement `Migration` in `common/mongo/migration/migrations/`, named `V<n><WhatItDoes>`.
2. Operate on `MongoDatabase` and raw `Document` only — **never** the POJO document classes. The codec
   registry is bound to the *current* class shape, while a migration by definition reads documents
   written under a previous one.
3. Make it idempotent, and say why in the class Javadoc. The runner cannot enforce this.
4. **Make it safe to run late and beside another run of itself**, and say why in the Javadoc.
   Idempotent is not enough. A migrating process that stalls is taken over; its operation and the
   new holder's then run at once, and its migration can finish after later migrations have been
   applied and the services have started. Filter every write on the state it was computed from (an
   absent field, the value that was read), or make it monotone (`$max`). A read followed by a write
   filtered on `_id` alone overwrites whatever a save path stored in between, with no error; v2 and
   v3 shipped that way and were corrected under #292.
5. **Pass `context.operationComment()` to every operation**, on every driver call that takes a
   comment. That is how a takeover sees an operation of a previous holder still running on the
   server. `MigrationOperationCommentTest` fails a migration that issues an uncommented command
   other than `dropIndexes` and `drop`; add the migration's expected commands and fixture data
   there. In driver 5.4.0 `createIndexes` cannot carry a comment, and an index build on `buckets`
   is long and survives its client: a migration that needs one needs its own design for this
   first.
6. Add it to `SchemaMigrationRunner.MIGRATIONS` and bump `SCHEMA_VERSION`.
7. Add a test covering the change, the no-op case, and applying it twice.
8. Add a row to the history table above.

Index drops belong in migrations rather than in a startup reconciliation pass. Reconciliation would
drop an index an operator added deliberately for an ad-hoc query, with no record of what was removed.
The `createMongoIndex*` methods stay purely additive.
