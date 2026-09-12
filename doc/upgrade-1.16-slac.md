# Upgrading the SLAC deployment to 1.16.0

Sequenced procedure for the SLAC deployment, whose archive shape (35M+ buckets, sharded, a handful
of ~42-day outlier buckets) makes two of this release's changes need planning rather than a rolling
restart. Companion to [schema-migration.md](schema-migration.md), which explains the migration
mechanism and what each startup failure means, and to the
[1.16.0 release notes](release-notes/rel-1.16.0.md), which describe what changed and why.

This runbook covers the bucket-span work (#232) specifically. The other 1.16.0 migrations (v1–v4)
touch the annotations, calculations, and buckets collections and are covered by the general
mechanism doc; [schema-migration-rehearsal.md](schema-migration-rehearsal.md) describes rehearsing
v1–v3 against a restored copy.

## Why this upgrade needs a window

Two independent facts:

1. **First startup runs two full scans of `buckets`** — migration v4 (stamp `_t` discriminators,
   from #248) and migration v5 (seed `pvStats`, from #232). Both are one-time and idempotent, both
   run inside the first upgraded service's `MongoClientBase.init()`, before its port binds.

2. **Buckets written by a pre-1.16.0 ingestion process after the v5 seed are silently unqueryable.**
   The query side derives each PV's lower bound from `pvStats`, and a 1.15 ingestion process does
   not write `pvStats`. A bucket longer than its PV's recorded span is then missing from any query
   whose window begins after the bucket starts — a wrong answer, not an error. This is the one
   ordering constraint in the upgrade that cannot be relaxed.

The good news is that the thing this replaces was worse: 1.15's startup scan blocked the port for
**hours** on this archive and had to be worked around by hand-seeding the `bucketSpanVerification`
marker. That workaround is now obsolete, and after this upgrade startup cost no longer scales with
archive size at all.

## Before the window

**Measure the seed's cost.** Run the v5 pipeline's grouping stage read-only against a secondary, to
get the PV count and confirm the span distribution. This is the same work the migration does, minus
the write:

```js
db.buckets.aggregate([
  {$match: {$expr: {$eq: [{$type: "$pvName"}, "string"]}}},
  {$group: {_id: "$pvName", maxBucketSpanSeconds: {$max: {$subtract: [
      "$dataTimestamps.lastTime.seconds", "$dataTimestamps.firstTime.seconds"]}}}},
  {$match: {maxBucketSpanSeconds: {$gte: 0}}},
  {$group: {_id: null, pvs: {$sum: 1}, worst: {$max: "$maxBucketSpanSeconds"}}}
], {allowDiskUse: true})
```

Record the elapsed time — the migration's scan is the same shape, so this is a direct estimate — and
the two values. `pvs` is what `db.pvStats.countDocuments()` should approximately equal afterwards;
`worst` should still be ~3,650,327 (the value measured 2026-08-17) unless the outlier buckets have
since been repaired.

**Confirm the outlier PVs.** These are the four that drove the limit to 3,700,000, and they are the
PVs whose queries will still carry a long lookback after the upgrade — everyone else's shrinks:

```js
db.pvStats.find().sort({maxBucketSpanSeconds: -1}).limit(10)   // after the upgrade
```

**Know the two settings you will change.** `DP_BUCKETS_VERIFY_SPANS_ON_STARTUP` is no longer read by
any service and can be deleted from the deployment at any time. `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS`
becomes **ingestion-only** — see step 5.

## The upgrade

**1. Stop all services** — ingestion, query, and annotation. This is what guarantees no pre-1.16
ingestion process writes after the seed. If a full stop is not acceptable, the alternative is to
upgrade **ingestion first** and let it be the process that runs the migrations; an upgraded
ingestion service against a not-yet-upgraded query service is harmless, but a 1.15 ingestion
service against a seeded database is not.

**2. Start one service and let it migrate.** The first to start claims the migration and runs v1
through v5. Expect it to be unavailable for roughly the elapsed time you measured above, doubled —
v4 and v5 each scan the collection once. Watch its log for:

```
V5SeedPvStatsMaxBucketSpan: seeded pvStats from buckets; pvStats now holds N document(s)
V5SeedPvStatsMaxBucketSpan: dropped legacy collection bucketSpanVerification
```

**3. Do not clear the migration claim.** Other services started during the migration will wait five
minutes and exit with the held-claim message. That is the expected "a migration is genuinely
running" branch, not a stuck claim. Under a supervisor they restart on their own and come up once
the migration finishes. Only clear the claim if `migratingSince` is hours old **and** the host named
in `migratingHost` is gone — the triage is in
[schema-migration.md](schema-migration.md).

**4. Verify the seed before starting the rest.**

```js
db.serviceMetadata.findOne({_id: "schemaVersion"})              // version: 5, migrating: false
db.pvStats.countDocuments()                                     // ≈ the `pvs` count measured above
db.pvStats.find().sort({maxBucketSpanSeconds: -1}).limit(5)     // the outlier PVs, ~3.65M seconds
db.getCollectionNames().includes("bucketSpanVerification")      // false
```

A PV legitimately has no `pvStats` document when every one of its buckets lacks `dataTimestamps` or
has `lastTime` before `firstTime`; such buckets are unreadable on the query path regardless. A
materially short count otherwise is worth investigating before proceeding.

**5. Lower `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS` back to 86,400 on ingestion.** The 3,700,000 value
exists only because the 1.15 query side sized its lower bound from this setting and the archive
contained 42-day buckets. The query side no longer reads it. Setting it back to the intended limit
restores ingestion's rejection of over-long frames, which is what keeps the outlier population
frozen, and it has **no effect on queries** — stored `pvStats` values are what queries use, and they
are unaffected by this setting.

Remove the setting entirely from the query and annotation deployments; it does nothing there.

**6. Start the remaining services and spot-check a query.** Pick a PV with 1-second buckets and a
window well after its first data, and confirm results are returned and the latency reflects a short
lookback rather than a 42-day one.

## After the upgrade

**Expect the improvement to be uneven, and that is the point.** Queries naming only well-behaved PVs
get a lower bound sized by that PV's own longest bucket — seconds, not weeks. Queries naming one of
the four outlier PVs still carry a ~42-day lookback for that PV, because the bound is the maximum
over the PVs the request names. Repairing those buckets (issue #258) would close that last gap; it
is no longer urgent, since the outliers no longer affect anyone else's queries.

**Queries with a `ConfigurationSelector` improve but retain a known multiplier.** Cost grows
linearly with the number of resolved retrieval fragments, each contributing its own copy of the
bound (issue #203). Each fragment's window is now much smaller, so these queries get faster, but a
many-fragment request is still disproportionately expensive. Report it against #203 rather than as a
1.16 regression.

**Any writer that bypasses the ingestion service must maintain `pvStats`.** A direct Mongo import or
a restore that adds buckets leaves those buckets uncovered, with the same silent-invisibility
consequence. The recourse is one write per affected PV, no restart needed:

```js
db.pvStats.updateOne({_id: "<pvName>"}, {$max: {maxBucketSpanSeconds: NumberLong("<seconds>")}}, {upsert: true})
```

**Lowering a stored value by hand requires an ingestion restart.** The ingestion process caches a
per-PV high-watermark and will skip writes that the collection no longer reflects.
