# Upgrading the SLAC deployment to 1.16.0

Sequenced procedure for the SLAC deployment, whose archive shape (35M+ buckets, sharded, a handful
of ~42-day outlier buckets) makes two of this release's changes need planning rather than a rolling
restart. Companion to [schema-migration.md](schema-migration.md), which explains the migration
mechanism and what each startup failure means, and to the
[1.16.0 release notes](../release-notes/rel-1.16.0.md), which describe what changed and why.

This runbook covers the bucket-span work (#232), the span-class partition and outbound flow control
that build on it (#274), and the deployment changes that land in the same release. The other
1.16.0 migrations (v1–v4) touch the annotations, calculations, and buckets collections; the general
mechanism doc explains how they work, and the note below on which of them are no-ops here says what
to expect from each in this deployment specifically.
[schema-migration-rehearsal.md](schema-migration-rehearsal.md) describes rehearsing v1–v3 against a
restored copy.

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

**Take a backup you can restore from, and treat it as the only way back.** There are no downgrade
migrations. Rolling the binaries back to 1.15.0 after the migrations have run leaves the database
in a state 1.15.0 misreads rather than refuses: migration v1 renamed the annotation `comment` field,
so a 1.15.0 service reads every annotation's comment as empty; v5 dropped the
`bucketSpanVerification` marker, so a 1.15.0 service repeats the hours-long startup scan (or needs
the hand-seeded marker again). Going back therefore means restoring the pre-upgrade backup, which
also discards anything ingested after the window opened.

**What each migration touches, and what that means for the backup:**

| migration | collection(s) written | recoverable from a metadata-only dump? |
|---|---|---|
| v1, v2, v3 | `annotations` | yes |
| v4 | `buckets`, `calculations` | `calculations` yes; `buckets` **no** |
| v5 | `pvStats` (created), drops `bucketSpanVerification` | yes — `pvStats` is derived, and re-seeding is just a re-run |

A `mongodump` of everything except `buckets` and `sampleStatusBuckets` covers v1–v3 and v5
completely. The rehearsal doc's Part 1 command produces exactly that dump, but it is written for
*rehearsal*, not backup — it excludes `sampleStatusBuckets` as well, which is data, not derived
state. For a backup, drop that second exclusion.

That leaves v4's write to `buckets`, and the reassuring part is that **v4 does not need to be
rolled back**. It only adds `_t: "dataColumn"` to legacy embedded columns, and 1.15.0 already
declared `BucketDocument.dataColumn` as the abstract `ColumnDocumentBase` with
`@BsonDiscriminator` on `DataColumnDocument` (since #173) — so the stamped discriminator is exactly
what a rolled-back 1.15.0 binary expects to read. A 1.15.0 service reads a v4-migrated `buckets`
correctly. Take a filesystem or cluster snapshot of `buckets` if one is cheaply available, but a
35M-bucket `mongodump` is not something to start during the window, and v4 is not the reason to.

What a `buckets` backup *would* buy you is recovering post-window ingestion if you roll back — the
same thing a snapshot of any live collection buys. Decide that on the usual grounds, not on v4.

**Most of these migrations will do nothing on this deployment, but two of them still cost a full
pass over `buckets`.** The migration runner applies every version below the binary's
`SCHEMA_VERSION` regardless of whether the collections it targets hold anything, so all five run —
but four of them should have nothing to find here. Confirm the two collection counts before the
window (`db.annotations.countDocuments()`, `db.calculations.countDocuments()`); if either is
non-zero, that migration does real work and the rehearsal in
[schema-migration-rehearsal.md](schema-migration-rehearsal.md) is worth running first.

| migration | what it targets | expected here |
|---|---|---|
| v1 | annotation `comment` → `description` rename, text index | no-op — `annotations` expected to be empty |
| v2 | normalize stored annotation tags | no-op — `annotations` expected to be empty |
| v3 | canonicalize annotation reference ids | no-op — `annotations` expected to be empty |
| v4 | stamp `_t` on legacy columns in `buckets` and `calculations` | `calculations` no-op (expected to be empty); **`buckets` scans every document** |
| v5 | seed `pvStats`, drop `bucketSpanVerification` | **scans every document** |

So the window's cost is v4's scan plus v5's scan, and nothing else. `sampleStatusBuckets` is not
touched by any migration — the Sample Status API is new in 1.16.0, so that collection does not yet
exist here.

**v4's scan may well modify zero documents, and will still take as long.** It stamps only columns
written *before* rel-1.13.0, which is when `@BsonDiscriminator` was added — the filter keys on the
discriminator being absent, not on which column type the bucket holds. A bucket written by any
1.13.0-or-later build already carries its own `_t` (`"dataColumn"` for the legacy `DataColumn`
type, `"doubleColumn"`, `"doubleArrayColumn"` and so on for the newer column-oriented types) and is
skipped. If this archive was created on 1.13.0 or later, v4 will report stamping 0 documents.

That is not a reason to expect it to be quick. There is no index on `dataColumn._t`, so determining
that nothing matches requires reading all 35M+ documents — the same scan either way. Budget for it
on the measured timing below, not on the modified count. The count appears in the first upgraded
service's log as `V4StampColumnDiscriminators: stamped _t on N bucket document(s)`, which is worth
capturing: a non-zero `N` means the archive predates 1.13.0 and those buckets were unreadable
before this upgrade.

**Measure the seed's cost.** Run the v5 pipeline's grouping stage read-only, through `mongos` with a
secondary read preference so it runs on each shard's secondaries, to get the PV count and confirm
the span distribution. This is the same work the migration does, minus the write:

```js
db.getMongo().setReadPref("secondary")
db.getMongo().getReadPrefMode()   // confirm: "secondary", not "primary"
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

**Inventory the bucket indexes, and drop the leftovers.** The archive has accumulated indexes the
current code never declared: startup only ever *creates* indexes, so every shape an earlier release
declared is still there, and so is any index added by hand. The shipped set on `buckets` is exactly
three plus the shard key:

| index | key |
|---|---|
| `_id_` | `{_id: 1}` |
| `pvName_1_dataTimestamps.firstTime.seconds_1_dataTimestamps.firstTime.nanos_1_dataTimestamps.lastTime.seconds_1_dataTimestamps.lastTime.nanos_1` | `(pvName, firstTime.seconds, firstTime.nanos, lastTime.seconds, lastTime.nanos)` — the one every bucket query runs on |
| `providerId_1` | `{providerId: 1}` |
| *(shard key index)* | whatever `sh.status()` reports for `buckets` |

Anything else is a leftover. The ones earlier releases created and never dropped are `pvName_1`
(retired in rel-1.15.0), `pvName_1_dataTimestamps.firstTime.seconds_1_dataTimestamps.firstTime.nanos_1`,
`pvName_1_dataTimestamps.firstTime.dateTime_1`, and `pvName_1_dataTimestamps.lastTime.dateTime_1`
(all retired in beta-1.6.0); the `(pvName, lastTime…, firstTime…)` index built during the August
troubleshooting is one more. Run this against `mongos` to see what is there:

```js
db.buckets.getIndexes().map(i => ({name: i.name, key: i.key}))
```

Every extra `pvName`-led index is a planner candidate for every bucket query. Before 1.16.0 that
was a measured cost: the planner trials each candidate's scan before choosing, and on recent-window
queries it was choosing the hand-built index with a blocking in-memory sort. 1.16.0 pins the bucket
queries to the shipped compound index with a `hint` (#271), so the leftovers no longer affect plan
choice — but each one still costs a write per ingested bucket and its share of disk, and there is no
reason to keep any of them. Dropping an index is a metadata operation and can be done at any time,
before or after the upgrade, outside the window:

```js
const shipped = "pvName_1_dataTimestamps.firstTime.seconds_1_dataTimestamps.firstTime.nanos_1_dataTimestamps.lastTime.seconds_1_dataTimestamps.lastTime.nanos_1";
const shardKey = db.getSiblingDB("config").collections.findOne({_id: db.getName() + ".buckets"})?.key;
const leftovers = db.buckets.getIndexes()
  .filter(i => Object.keys(i.key)[0] === "pvName" && i.name !== shipped)
  .filter(i => !(shardKey && JSON.stringify(i.key) === JSON.stringify(shardKey)));
leftovers.forEach(i => print("leftover: " + i.name + " " + JSON.stringify(i.key)));
// after reviewing the list:
// leftovers.forEach(i => db.buckets.dropIndex(i.name));
```

Do **not** drop the shipped compound index or the shard key index (the snippet skips an index whose
key equals the shard key; if the shard key is `pvName`, the shipped compound index also supports it
and `pvName_1` can still go). If the shipped index is ever
missing on a shard, 1.16.0's hinted queries fail with a driver error naming the hint rather than
silently falling back to a collection scan — the client gets an error response, not a stalled
request — and the next service start re-creates it.

Please also send back the `getIndexes()` output and the shard key for `buckets` (from `sh.status()`
or `db.getSiblingDB("config").collections.findOne({_id: "<db>.buckets"}).key`) — the shard key was
never captured, and it determines whether a single-PV query is targeted at one shard or broadcast.

**Know the settings you will change.** `DP_BUCKETS_VERIFY_SPANS_ON_STARTUP` is no longer read by
any service and can be deleted from the deployment at any time. `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS`
becomes **ingestion-only** — see step 5. Two query-service settings are new and need no change for
this upgrade; know they exist for the tuning notes after the window:
`DP_QUERY_HANDLER_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS` (60) and
`DP_QUERY_HANDLER_STREAM_READY_TIMEOUT_SECONDS` (300), both described in
[running.md](../running.md).

**Free up a metrics port for every service you run, or that service will not start.** This is
unrelated to the bucket-span work but lands in the same release, and it is the one 1.16.0 change
that can stop a service from coming back up inside the window. Each service now binds a second port
for its Prometheus metrics endpoint and **fails to start if it cannot bind it** (issue #212) — a
service silently running without metrics was judged worse than a loud failure. The defaults:

| service | metrics port | notes |
|---|---|---|
| ingestion | 9464 | |
| query | 9465 | |
| annotation | 9466 | |
| ingestion stream | 9467 | only if this service is deployed |

The rest of this runbook refers to the three services this deployment is known to run — ingestion,
query, and annotation. The ingestion stream service is a separate process (see
[running.md](../running.md)); if SLAC runs it, free 9467 as well and include it wherever the steps
below say "all services". If not, 9464–9466 are enough.

Before the window, confirm nothing on each host already holds these:

```
ss -lntp | grep -E '946[4-7]'
```

**Then restrict them at the firewall, to the monitoring host only.** This is a required step, not a
judgment call. The endpoint binds `0.0.0.0` by default and has **no authentication and no TLS**, so
on a routable interface it is readable by anything that can reach the host. It exposes no PV names
and no data values — the cardinality policy in [metrics.md](../metrics.md) guarantees that — but it
does reveal request rates, latencies, and collection names.

The default is deliberately not loopback: Prometheus scrapes these ports over the network, so
`127.0.0.1` would silently yield no data in any scrape topology except a node-local scraper or
sidecar. Binding wide and restricting at the firewall is the combination that works; binding narrow
would trade a visible security control for an invisible monitoring outage. Only if the scraper runs
on the service host itself should you instead set `DP_TELEMETRY_PROMETHEUS_HOST=127.0.0.1`, which
makes the firewall rule unnecessary.

Confirm the rule does what you expect, from a host that is *not* the monitoring host:

```
curl -s --max-time 5 http://<service-host>:9465/metrics | head    # expect no response
```

and from the monitoring host, that scraping still works:

```
curl -s --max-time 5 http://<service-host>:9465/metrics | head    # expect metric lines
```

To change a port set `DP_<SERVICE>_SERVER_METRICS_PORT`; to turn the whole thing off set
`DP_TELEMETRY_ENABLED=false`, and no port is bound. Full reference: [metrics.md](../metrics.md).

## The upgrade

**1. Stop all services** — ingestion, query, and annotation. This is what guarantees no pre-1.16
ingestion process writes after the seed.

If a full stop is not acceptable, the alternative is to upgrade **ingestion first** and let it be
the process that runs the migrations — but query and annotation must still be stopped or upgraded
before it starts. The migration claim coordinates only the processes that are *starting*; it has
no effect on a 1.15 process already running, which keeps serving against the schema the migrations
are changing underneath it. Concretely, once v1 renames the annotation `comment` field, a still-running
1.15 annotation service reads every annotation's comment as empty — the same misreading described
under rollback above, except live and unnoticed. A 1.15 ingestion service against a seeded database
is the other unsafe combination (#232). An upgraded ingestion service against a not-yet-*started*
query service is harmless; an upgraded ingestion service against a still-*running* 1.15 query or
annotation service is not.

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

Also capture the span-class distribution, and send it back with the index inventory. Since #274 a
bucket query that names its PVs explicitly — the V1 named-PV path and all three V2 paths —
partitions them into power-of-two span classes (class 0 for spans up to 1 s, class *k* for spans in
`(2^(k−1), 2^k]`) and issues one find per class present in the request, so this histogram says how
many finds a typical multi-PV request will cost and which PVs share a class with the outliers.

The one exception is the **V1 `queryTable` pattern branch**, which has no PV list: it matches the
pattern against `pvStats._id` and takes a single maximum over the matches, so it issues one find
bounded by the widest span among the matching PVs. A pattern that happens to match one of the four
outliers therefore still carries a ~42-day lookback for every PV it matches. Read the histogram as
describing named-PV and V2 queries; for pattern queries, what matters is whether the pattern can
reach an outlier.

```js
db.pvStats.aggregate([
  {$project: {cls: {$cond: [{$lte: ["$maxBucketSpanSeconds", 1]}, 0,
      {$ceil: {$log: ["$maxBucketSpanSeconds", 2]}}]}}},
  {$group: {_id: "$cls", pvs: {$sum: 1}}},
  {$sort: {_id: 1}}
])
```

(`$log` is floating-point, so a span that is an exact power of two may land one class high; the
service computes the class exactly. The shape of the histogram is what matters here.) On the
numbers measured in August — spans from 10 s to 3,650,327 s with a 186 s average — expect most PVs
in classes 4 through 8 and the four outliers alone in class 22.

**5. Lower `DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS` back to 86,400 on ingestion.** The 3,700,000 value
exists only because the 1.15 query side sized its lower bound from this setting and the archive
contained 42-day buckets. The query side no longer reads it. Setting it back to the intended limit
restores ingestion's rejection of over-long frames, which is what keeps the outlier population
frozen, and it has **no effect on queries** — stored `pvStats` values are what queries use, and they
are unaffected by this setting.

Remove the setting entirely from the query and annotation deployments; it does nothing there.

**6. Start the remaining services and spot-check two queries.** First, pick a PV with short
buckets and a window well after its first data, and confirm results are returned and the latency
reflects a short lookback rather than a 42-day one. Second, run a `querySamples` naming that PV
**and** one of the four outlier PVs over a window where both have data, and confirm that both
columns come back populated: this is the request shape #274 fixed (before it, a large page could
return the second PV all-empty with no error) and the one the span-class partition exists for (the
short-bucket PV's retrieval is no longer widened by the outlier's 42-day span). If either query
takes over a second it appears in the `dp.slowquery` log with its stage breakdown, which is the
fastest way to see whether the time went to the database or to assembly.

## After the upgrade

**Expect the improvement to be uneven, and that is the point.** On a query that names its PVs (the
V1 named-PV path and all three V2 paths), each PV's retrieval is bounded by its own span class —
seconds to minutes for the well-behaved majority. A request naming one of the four outlier PVs
still pays a ~42-day lookback **for that PV's find only**: since #274 the request is split into one
find per span class, so the outlier no longer widens the retrieval of the other PVs named alongside
it (under #232 alone it did, because the bound was the maximum over the whole request).

The V1 `queryTable` **pattern** branch is the exception, as noted with the histogram above: it has
no PV list, so it takes one maximum over every PV the pattern matches and issues a single find at
that bound. A pattern matching an outlier still widens the lookback for every PV it matches, just
as #232 alone did.

The cost of the outlier's own find is what repairing those buckets (issue #258) would remove; it is
no longer urgent for named-PV queries, since it no longer affects anyone else's, but it is still
what stands between a pattern query and the same improvement.

**Slow streaming clients now occupy a worker instead of the heap.** `queryDataStream`,
`queryBucketsStream`, and `querySamplesStream` wait for the client to drain each message before
sending the next, for up to `DP_QUERY_HANDLER_STREAM_READY_TIMEOUT_SECONDS` (300) per message. A
client that reads slowly holds its query worker for the life of the stream, and the query service
has `DP_QUERY_HANDLER_NUM_WORKERS` (7) of them: seven stalled readers stop every other query until
one of them drains or times out. Watch `dp_handler_workers_active{dp_service="query"}` against
`dp_handler_workers_max`, and the `abandoned` outcome in `dp_query_requests_total`, which counts
streams cut short by a cancel or a readiness timeout. If a site has many long-lived streaming
consumers, raise the worker count rather than the timeout — a longer timeout only lengthens how
long a dead client holds a worker.

**Queries with a `ConfigurationSelector` improve but retain a known cost.** A request that resolves
to several retrieval fragments is bounded on the index by the earliest fragment's begin (minus the
PV's span) and the latest fragment's end; the buckets between fragments are scanned and filtered out
(issue #203). A request whose fragments are far apart in time is therefore still disproportionately
expensive. Report it against #203 rather than as a 1.16 regression.

**Any writer that bypasses the ingestion service must maintain `pvStats`.** A direct Mongo import or
a restore that adds buckets leaves those buckets uncovered, with the same silent-invisibility
consequence. The recourse is one write per affected PV, no restart needed:

```js
db.pvStats.updateOne({_id: "<pvName>"}, {$max: {maxBucketSpanSeconds: NumberLong("<seconds>")}}, {upsert: true})
```

**Lowering a stored value by hand requires an ingestion restart.** The ingestion process caches a
per-PV high-watermark and will skip writes that the collection no longer reflects.

**Confirm the metrics endpoints came up, and use them to check this upgrade's work.** One scrape per
service:

```
curl -s localhost:9465/metrics | head
```

Verify by whether the port is listening rather than by the startup log line — under
`OTEL_METRICS_EXPORTER=none` the service still logs a "prometheus endpoint" that was never bound.

The new metrics are the most direct evidence of whether this upgrade did what it was meant to. After
a representative query load, the buckets-read-per-query figure is what the bucket-span bound exists
to reduce, and it should fall sharply for well-behaved PVs:

```promql
rate(dp_query_buckets_total[5m]) / rate(dp_query_requests_total[5m])
```

`doc/metrics.md` has the full diagnostic workflow, including the stage breakdown that says whether a
slow query's time is in the database or elsewhere. Note that its step 0 matters here: the query
stage histograms start at handler entry and cannot see wire time, so a client reporting slowness is
not contradicted by healthy stage numbers.
