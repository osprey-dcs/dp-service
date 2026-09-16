# dp-service 1.16.0 Release Notes

Changes since rel-1.15.0. Builds against dp-grpc 1.16.0 (tag `rel-1.16.0`), which carries the
protobuf changes referenced below by dp-grpc issue number.

## Upgrading from 1.15.0 — read this first

1.16.0 is the first release delivered through the schema migration mechanism, and its first
startup against an existing database changes stored data. In order:

1. **Free ports 9464–9467** on every service host (or set the metrics-port variables); each
   service now binds a metrics endpoint and refuses to start without it (#212, below).
2. **Take a restorable backup.** There are no downgrade migrations, and a 1.15.0 binary against a
   migrated database misreads it rather than refusing: it sees every annotation's comment as empty
   (v1 renamed the field) and repeats its hours-long startup bucket scan (v5 dropped the marker
   that skipped it). Restoring the backup is the only way back.
3. **Stop every service.** A 1.15.0 ingestion process writing after migration v5 has seeded
   `pvStats` produces buckets that queries can silently miss (#232). The migration claim
   coordinates the *migrating* processes only — it does not hold off a 1.15.0 process that is
   already running, and such a process keeps serving against the migrated schema: after v1 a
   1.15.0 annotation service reads every annotation's comment as empty, exactly as it would after
   a rollback. If a full stop is impossible, upgrade ingestion first and let it migrate, but stop
   or upgrade query and annotation before it does — leaving them up is a live wrong answer, not
   just a risk window.
4. **Start one service and let it migrate.** Five migrations run in the first upgraded process,
   two of them full scans of `buckets`; budget the window from a read-only measurement of the
   archive (the SLAC runbook has the query). Other services started meanwhile exit after a
   five-minute claim wait and must be restarted once the migration finishes.
5. **Verify** the marker (`version: 5`), the `pvStats` count, and the index set, then start the
   rest.

The mechanism and its failure messages: [`doc/runbooks/schema-migration.md`](../runbooks/schema-migration.md).
Rehearsing against a restored copy: [`schema-migration-rehearsal.md`](../runbooks/schema-migration-rehearsal.md).
The SLAC sequence with measured numbers: [`upgrade-1.16-slac.md`](../runbooks/upgrade-1.16-slac.md).

## Schema migration mechanism (Issue #254)

### DEPLOYMENT CHANGE: services migrate the database at startup and fail closed

Every service now records the database's schema version in `serviceMetadata` and applies pending
migrations during `MongoClientBase.init()`, before its port binds. A database whose version the
binary cannot establish — newer than the build, a migration that failed partway, a claim held by a
process that did not finish within five minutes — **stops the service** instead of being served
from. The choice is deliberate: every migration in this release exists because the unmigrated shape
reads as a wrong answer rather than an error (a null description, an unmatchable tag, an invisible
bucket), and a delivery mechanism that logged and continued would compound one silent failure with
another.

Concurrent startup is the normal case: one process wins an atomic claim on the marker and migrates,
the others wait and then proceed. A database with no marker is classified by content — empty means
a fresh install stamped at the current version; any document in any managed collection means a
legacy database migrated from version 0. **Restore backups before the first start**, never
underneath a marker.

Migrations can be disabled with `DP_MONGO_RUN_SCHEMA_MIGRATIONS_ON_STARTUP=false`, which skips
*applying* them but not the version check: a mismatched database still refuses to start. The five
migrations shipped in 1.16.0 (v1–v3 on `annotations`, v4 and v5 on `buckets`) are described in
their owning sections below and inventoried in the runbook. The async Mongo client cannot run the
check and logs a warning; it is on no production path.

## Modernized DataSets and Annotations APIs (Issues dp-grpc #132, dp-service #248)

### API CHANGE: message shapes follow the current conventions (#248 Phase 1)

dp-grpc #132 reshaped the oldest generation of `DpAnnotationService` to the conventions the PV
metadata, machine configuration, and sample status APIs established, and 1.16.0 implements the
service side. Clients built against 1.15.0 protos must be regenerated. The changes a caller sees:

- `Annotation` is a top-level message, and its `comment` field is now `description`. Stored
  annotations are renamed by **schema migration v1**, which also replaces the annotations text
  index (now over `name` and `description` with ascending `ownerId`; `event.description` is no
  longer indexed). The migration halts rather than overwrite if a document carries both fields —
  the runbook has the pre-check.
- `SaveDataSetRequest` is flat: the dataset's fields are on the request, not on a nested
  `dataSet`.
- Every query criterion takes **repeated** values; a criterion with one value behaves exactly as
  the old singular one. Two keep a singular field: `TextCriterion` is still a single `text`, and
  `AttributesCriterion` is a single `key` alongside repeated `values` (an empty `values` list is a
  key-only existence search).
- `queryAnnotations` returns references only: the `dataSets` field is gone, and
  `Annotation.calculations` is populated by `getAnnotation` alone (below). Callers that read
  embedded dataset or calculations content from query results must fetch it by id.
- `CalculationsDataFrame` carries its frame under a `frame` submessage.

### New CRUD methods, entity fields, and delete semantics (#248 Phase 2)

`getDataSet`, `getAnnotation`, `getCalculations`, `deleteDataSet`, and `deleteAnnotation` are
implemented; `patchDataSet` and `patchAnnotation` respond "not yet implemented". All entities emit
`modifiedBy`, `createdTime`, and `updatedTime` (unset until the first update), and DataSets carry
tags and attributes. The behaviors worth knowing:

- A malformed ObjectId on any get/delete is a **rejection**, as is a not-found.
- `deleteDataSet` is rejected while any annotation references the dataset; the message names one
  referencing annotation and the total count.
- `deleteAnnotation` also deletes the annotation's calculations document, and is not blocked by
  other annotations' references to it — those links may dangle. `saveAnnotation` likewise deletes
  the calculations document it replaces or clears.
- `getAnnotation` reports a `calculationsId` that resolves to no document as an **error**, never as
  empty calculations.
- Annotation tags are now normalized (lowercase, deduplicated, sorted) on save like every other
  tagged entity; **schema migration v2** normalizes previously stored annotation tags, without
  which a stored mixed-case tag could never match a `TagsCriterion` value. **Schema migration v3**
  canonicalizes stored `dataSetIds`/`annotationIds` to lowercase hex, which the reference checks
  compare as strings.

### BEHAVIOR CHANGE: query criteria combine with AND (#248 Phase 3)

`queryDataSets` and `queryAnnotations` now combine multiple criteria list entries with **AND**;
multiple values within a single criterion still OR. Previously some criterion-type combinations
ORed with each other (for `queryDataSets`: text and pvName criteria; for `queryAnnotations`:
annotationIds, tags, and attributes criteria), so a multi-criterion query that is valid under
both releases can **silently return different results** — e.g. two `TagsCriterion` entries used
to return records carrying *either* tag and now return records carrying *both*. To OR two tag
values, list them in one `TagsCriterion`.

A request with more than one `TextCriterion` is now rejected: text criteria cannot be
AND-combined (MongoDB permits a single `$text` expression per query).

### Opaque page tokens for queryDataSets / queryAnnotations (#248 Phase 3)

The two queries' `pageToken` is now an opaque keyset token (encoding the last-returned record id)
rather than a Base64 skip offset. A malformed token — including a token issued for the other
query — is **rejected** with an `ExceptionalResult`, where the interim Phase 1 implementation
silently restarted at the first page. Page boundaries are now stable while records are inserted
or deleted mid-pagination. Tokens are continuation state, not bookmarks: obtain them only from
`nextPageToken` of a previous response, and do not persist them across sessions.

The three metadata queries (`queryPvMetadata`, `queryConfigurations`,
`queryConfigurationActivations`) keep their skip-offset tokens; converting them is a follow-on
(issue #193).

### queryConfigurationActivations ordering completed (#248 Phase 3)

Results are now ordered by `startTime` ascending, then `configurationName` ascending, then record
id ascending, per the proto ordering contract. Previously the sort was `startTime` alone, which
is not unique, so activations sharing a start time could be dropped or duplicated across page
boundaries.

### Typed calculations columns (#248 Phase 4)

`saveAnnotation` now accepts, stores, and returns calculations frames using all 16 column forms
(the typed scalar/array/binary columns alongside legacy `DataColumn` and `SerializedDataColumn`).
Previously typed columns were **silently dropped** from a frame that also carried legacy columns,
and a frame carrying only typed columns was rejected. `getAnnotation`/`getCalculations` return
stored columns in their typed form. Column provenance `derivedFrom` links (a PV name or a
calculations column address, with optional time range) are now stored and round-tripped as
supplied — on calculations columns and ingested bucket columns alike — with no existence checks:
links may reference records not yet created.

### BEHAVIOR CHANGE: saveAnnotation validates calculations content fully (#248 Phase 4)

`saveAnnotation` now validates the complete shape of a `Calculations` payload: every column of
every type must have a non-blank name and non-empty values, and **its value count must equal the
frame's timestamp count** (for array columns, timestamp count times the dims product;
`SerializedDataColumn` entries carry no countable values and get name checks only). Column names
must be unique across all column types within a frame, and frame names must be unique within the
Calculations object — both are addressing keys for `CalculationsSpec` and provenance links.
Column metadata is checked against the same limits as ingestion, and so are column values:
string values are capped at 256 characters, an array column's per-sample element count (dims
product) at 10M, each image at 50MB, and each struct value at 1MB — the shared
`ColumnValueLimits` contract, so a payload ingestion would reject cannot be stored through
`saveAnnotation` either.

This narrows what was previously accepted for legacy `DataColumn` lists: a column shorter or
longer than its frame's timestamp axis used to be stored as-is — and a short column would hang a
subsequent tabular export with no response. A frame carrying only typed columns (no legacy
`dataColumns`) is now accepted; previously it was wrongly rejected by a legacy-list-only
emptiness check.

### Inline dataBlocks as an exportData source (#248 Phase 4)

`exportData` accepts a new repeated `dataBlocks` field — the same time-range-plus-PV-names
building block a DataSet contains — as an inline, ad-hoc export source. At least one of
`dataSetId`, `dataBlocks`, or `calculationsSpec` must now be supplied (previously: one of the
first and last two). Inline blocks are validated like saveDataSet blocks, merged after any
stored dataset's blocks into one effective block list, and nothing is persisted. An
inline-only export's output file is keyed by a generated ObjectId, since there is no stored id
to name it by.

### BEHAVIOR CHANGE: export client mistakes are rejected, not errored (#248 Phase 4)

`exportData` failures caused by the request — a `dataSetId` or `calculationsId` matching no
record, a `calculationsSpec.dataFrameColumns` filter naming a frame or column the calculations
object does not contain, and a malformed `dataSetId` (now detected in validation rather than
reading as "not found") — are now reported with `RESULT_STATUS_REJECT` instead of
`RESULT_STATUS_ERROR`, per the #235 classification. Service-side failures (database errors,
file I/O, the tabular export file size limit) remain errors. Clients branching on the
exceptional status will see the changed classification; messages are unchanged.

### Typed calculations columns export to HDF5 with a per-column encoding tag (#248 Phase 4)

HDF5 export now writes calculations columns of every type (the 14 typed column forms alongside
the legacy `DataColumn` and `SerializedDataColumn`), where previous builds supported only legacy
columns. Each calculations column's serialized bytes are now accompanied by a self-describing
`dataColumnEncoding` tag (`"proto:" + <proto message name>`, e.g. `proto:DoubleArrayColumn`) in
the column's group, the same scheme bucket data has always used. Files written by earlier builds
carry no tag for calculations columns; their columns are implicitly `DataColumn`-encoded. Export
files are point-in-time artifacts, so no compatibility mechanism accompanies the change.

Tabular formats (CSV, XLSX) export typed *scalar* calculations columns; a calculations column
with no tabular representation (array, image, struct, serialized) is rejected with guidance to
export to HDF5 instead — a rejection, not an error, per the classification above.

### Schema migration v4 — one-time full bucket scan at first startup (#248 Phase 4)

The first 1.16.0 service to start against an existing database runs schema migration v4
(`V4StampColumnDiscriminators`), stamping the `_t` class discriminator on embedded legacy columns
written before rel-1.13.0, in `buckets` and `calculations` alike. Without the stamp, such columns
cannot be decoded under the polymorphic field types — which reads as silently empty query and
export results, not an error.

Operationally this is a **one-time full scan of the `buckets` collection**: expect minutes up to
roughly an hour on archives in the tens of millions of buckets. While the elected process runs
the migration, other starting services wait five minutes on the migration claim and then exit
with the held-claim message; during a long v4 run this is the "a migration is genuinely running"
branch of that message's triage, not a stuck claim. Under a supervisor this self-heals — the
waiting services restart and come up once the migration completes. Do not clear the claim while
the migrating host is alive. See `doc/runbooks/schema-migration.md` for triage guidance and the migration
inventory.

## Sample Status API (Issues dp-grpc #121, dp-service #238)

The Annotation Service implements `saveSampleStatuses`, `querySampleStatuses`,
`querySampleStatusesStream`, and `deleteSampleStatuses`; the two domain-registry methods
(`saveSampleStatusDomain`, `querySampleStatusDomains`) respond "not yet implemented". A status is
keyed by (pvName, timestamp, domain, layer) at nanosecond precision and stored in the new
`sampleStatusBuckets` collection, whose indexes every service creates at startup. Saving carves
exactly-colliding timestamps out of existing documents before inserting, so no two documents ever
assert a status for the same key; deleting is exact at the sample axis over `[beginTime, endTime)`;
querying returns boundary documents whole, ordered by (pvName, domain, layer, firstTime), with
keyset page tokens that are **rejected** when malformed. Timestamps are range-checked on the save,
query, and delete paths against the epoch-nanos representation the storage and query paths key on:
`epochSeconds` above 9,223,372,036 (~year 2262) is rejected, because the conversion would wrap
negative and write a document no overlap query could find. The check is on seconds alone, so at
exactly 9,223,372,036 s a `nanoseconds` value above 854,775,807 still overflows; tightening that
boundary is issue #284.

Three new `AnnotationHandler` settings: `sampleStatusQueryDefaultPageSize` (10000),
`sampleStatusQueryMaxPageSize` (100000, larger requests are clamped), and
`sampleStatusSaveMaxStatuses` (1000000 per request).

### `QuerySpec.sampleStatusSelector` on querySamples

`querySamples` and `querySamplesStream` accept a `sampleStatusSelector` that keeps (INCLUDE) or
drops (EXCLUDE) samples labeled with a matching status at their exact timestamp; it composes with a
`configurationSelector` by intersection. `queryBuckets`/`queryBucketsStream` reject it, since a
whole storage bucket cannot represent per-sample filtering. A status-join failure is reported as an
error, never as "no statuses" — in EXCLUDE mode that would silently return the filtered-out samples.

## Annotation Service query and classification changes (Issues #235, #245)

### BEHAVIOR CHANGE: an empty criteria list is match-all, and every query is bounded (#245)

`queryPvMetadata`, `queryConfigurations`, and `queryConfigurationActivations` previously
**rejected** an empty criteria list, and no match-all criterion existed, so "list everything" was
unaskable. An empty list now matches all records. A supplied criterion must still be well-formed.
Every one of these queries now applies a default limit of 100 when `limit` is unset and returns a
`nextPageToken` when more remain — `queryPvMetadata` in particular previously returned every match
with an always-blank token when `limit` was unset, so a caller that relied on that unbounded read
now needs to page.

**`queryDataSets` and `queryAnnotations` changed the same way**, as part of #248 Phase 1 rather
than #245. Both previously rejected an empty criteria list (`"QueryDataSetsRequest.criteria list
must not be empty"` and its `queryAnnotations` counterpart); both now treat it as match-all and
apply the same default limit of 100. So a request that 1.15.0 rejected outright now succeeds and
returns the first page of the whole collection — worth checking wherever client code relied on
that rejection to catch an unfilled filter.

### BEHAVIOR CHANGE: business-rule failures are rejections, not errors (#235)

Six failures the Annotation Service detected inside its Mongo client were reported as
`RESULT_STATUS_ERROR`; they are client mistakes and now arrive as `RESULT_STATUS_REJECT`:
`saveDataSet`/`saveAnnotation` with an `id` matching no record, `saveConfiguration` changing a
category that has activations, `deleteConfiguration` with activations present, and
`saveConfigurationActivation` naming a missing configuration or overlapping an existing activation.
Messages are unchanged. In the same change, a Mongo lookup failure during a save is now reported as
an error rather than as "not found", and `saveDataSet`/`saveAnnotation` no longer upsert on an
`_id` filter (a document deleted between lookup and write is now a rejection instead of a silently
re-created record under a new id).

## API CHANGE: `DataValue.ValueStatus` removed (Issues dp-grpc #143, dp-service #252)

dp-grpc 1.16.0 removes `DataValue.ValueStatus` and its `StatusCode`/`Severity` enums; the Sample
Status API above is the replacement. No server path ever read the field, so there is no stored
behavior to preserve: archived values that carried it still parse (the field number is reserved and
reads as unknown), and no migration is needed. Clients that set it must regenerate against the new
protos; the client library's `IngestionRequestParams` loses its `valuesStatus` parameter.

## Per-PV bucket span bound; the startup span scan is removed (Issue #232)

### The startup full-collection scan is gone

Services no longer verify the whole `buckets` collection against the configured span limit before
binding their gRPC port. That check (#197) scanned every bucket at startup — hours on an archive in
the tens of millions of buckets, during which the port was unbound and Kubernetes liveness probes
failed. It is replaced by a per-PV statistic maintained at ingestion, so startup cost is now
independent of archive size.

Removed with it: the `BucketSpanVerifier`, the `bucketSpanVerification` marker collection (dropped
by migration v5), and the `DP_BUCKETS_VERIFY_SPANS_ON_STARTUP` setting. A deployment still setting
that variable is unaffected — it is simply no longer read. Any operational procedure that pre-seeds
the `bucketSpanVerification` marker to skip the scan (the "option 0" runbook on issue #257) is
obsolete and should be retired; the marker is deleted at upgrade.

### Query lower bounds are now per PV, not archive-wide

Every bucket time-range query carries a `firstTime` lower bound so the index scan has a floor
instead of reading a PV's full history. That bound was previously sized by the configured span
limit, which had to cover the **longest bucket anywhere in the archive** — so a handful of
over-long buckets on a few PVs imposed that same lookback on every query for every other PV.

The bound is now derived per query from `pvStats.maxBucketSpanSeconds`, the largest span ever
ingested for each PV. As first merged it was taken as the maximum over the PVs the request names;
#274 (below) then partitioned each request by span class so that an outlier PV bounds only its own
retrieval. A PV with normal bucket sizes is no longer penalized by an unrelated PV's outliers, in
the same request or elsewhere in the archive. On an archive where the configured limit had been
raised to accommodate outliers, this is the difference between a lookback measured in weeks and one
measured in seconds for the well-behaved majority.

### BEHAVIOR CHANGE: `Buckets.maxBucketSpanSeconds` is now ingestion-only

`DP_BUCKETS_MAX_BUCKET_SPAN_SECONDS` still rejects over-long frames at ingestion, and is still
validated at startup (non-positive values, and values large enough to overflow the nanosecond
conversion, are rejected). **The query side no longer reads it at all.** Changing it now changes
only what ingestion accepts from that point forward; it has no effect on query behavior, and
raising it no longer widens any query's scan.

A deployment that raised this limit to accommodate outlier buckets can lower it back to its
intended value on the ingestion service after upgrading. Doing so does not invalidate the already
recorded statistics — stored spans are what queries use, and they are unaffected by the setting.

### Schema migration v5 — a second one-time full bucket scan at first startup

The first 1.16.0 service to start against an existing database runs migration v5
(`V5SeedPvStatsMaxBucketSpan`), seeding `pvStats` from the existing archive in one server-side
`$group`/`$merge` pipeline and dropping `bucketSpanVerification`.

**The seed is required for correctness, not an optimization.** A PV with no `pvStats` document is
treated as having a span of zero. Without the seed, every bucket written before the upgrade would
be invisible to any query whose window begins after that bucket's first second — until the PV
happened to ingest a bucket at least as long, which for a retired or slowly sampled PV is never.

Operationally this is a **one-time full scan of `buckets`, in addition to migration v4's** — both
run in the same first startup, so budget roughly twice the v4 estimate: minutes up to a couple of
hours on archives in the tens of millions of buckets. Memory is bounded by the number of distinct
PVs, not the number of buckets. While the elected process migrates, other starting services wait
five minutes on the claim and then exit; under a supervisor they restart and come up once the
migration finishes. Do not clear the claim while the migrating host is alive.

### UPGRADE ORDERING: stop every service, or upgrade ingestion first

A pre-1.16.0 ingestion process that keeps writing after the seed has run produces buckets that no
statistic covers, and **a bucket longer than its PV's recorded span is silently missing from query
results** — not an error. Either stop all services for the upgrade, or upgrade ingestion before the
seed runs. An upgraded ingestion service running against a not-yet-upgraded query service is
harmless.

The same constraint applies permanently to any writer that bypasses ingestion — a direct Mongo
import, or a restore that adds buckets. Such a writer must raise the affected PVs' statistics with
a `$max` upsert on `pvStats`; the one-line recourse, and the reason lowering a value by hand
requires an ingestion restart, are in [`doc/runbooks/schema-migration.md`](../runbooks/schema-migration.md).

SLAC-specific upgrade sequencing, with the site's measured numbers and verification queries, is in
[`doc/runbooks/upgrade-1.16-slac.md`](../runbooks/upgrade-1.16-slac.md).

### Known limitation: between-fragment cost under a ConfigurationSelector (#203)

A `querySamples` or `queryBuckets` request carrying a `ConfigurationSelector` resolves to multiple
retrieval fragments. The index scan for such a request is bounded by the earliest fragment's begin
(minus the PV's span) and the latest fragment's end (see #271 below); the buckets between the
fragments are scanned and discarded by the filter, so the cost is proportional to the time from
the first fragment to the last rather than to the fragments' own extent. #232 shrinks the lookback
below the first fragment to the PV's own span, so these queries get materially faster in absolute
terms, but the between-fragment cost is unchanged and is tracked separately as issue #203.
Requests without a `ConfigurationSelector` resolve to a single interval and are unaffected.

## Bucket queries pinned to the compound index and bounded on both sides (Issue #271)

Follow-on to #232 from a review of the SLAC query-performance reports. The compound bucket index
`(pvName, firstTime.seconds, firstTime.nanos, lastTime.seconds, lastTime.nanos)` is unchanged; what
changed is how reliably every bucket query uses it.

### Every bucket query now hints the compound index

All bucket time-range retrieval — `queryData`, `queryTable` (by name list or pattern), the annotation
data-block export, and the V2 `queryBuckets`/`queryBucketsStream`/`querySamples`/`querySamplesStream`
paths — now passes a `hint` naming the compound index. Previously the planner chose among every
index on the collection, and a long-lived archive carries `pvName`-led indexes the current code
never declared: startup never drops an index, so the shapes retired in beta-1.6.0 and rel-1.15.0
are still present, alongside anything added by hand. Each one is a planner candidate; the planner
trials every candidate's scan before choosing, and on recent-window queries it was measured choosing
a `lastTime`-led index whose plan needs a blocking in-memory sort. With the hint the planner
considers only the shipped index, whose leading `(pvName, firstTime)` both carries the #232 lower
bound and streams the `(pvName, firstTime)` sort with no sort stage.

**BEHAVIOR CHANGE:** if the compound index is missing from `buckets` (on any shard), bucket queries
now fail with a database error naming the hint instead of silently degrading to a collection scan.
The failure is reported to the caller as an error response on every retrieval path — V1
`queryData`/`queryTable`, the annotation data-block export, and the three V2 paths — not as a
stalled stream. Every service creates the index at startup, so this only arises if it is dropped by
hand; restart any service to re-create it. Operators are encouraged to drop the leftover `pvName`-led indexes —
they no longer affect plan choice but still cost a write per ingested bucket and their share of
disk; the SLAC runbook lists them by name.

### The index scan is now bounded above as well as below

The overlap filter now also carries `firstTime.seconds <= endSeconds`, implied by its own
`firstTime < end` half (which, being a `(seconds, nanos)` `$or`, the planner cannot use as an index
bound). Before, whenever the planner chose the single-scan plan, the scan ran from `begin − span`
to the **end of each PV's history** and discarded everything after the window by filter — on a
historical query against a PV that has kept ingesting since, that is most of the PV's archive. The
scan now covers `[begin − span, end]` on `firstTime.seconds` under either plan the planner may
choose. No result changes.

### V2 multi-fragment queries now reach the planner with a bound

For a `ConfigurationSelector` request with more than one retrieval fragment, the per-fragment
bounds sit inside an `$or`, and the planner's single-scan plan — which it chose on the test
fixture with or without the hint — had **no** bound on `firstTime.seconds` at all: each named PV's
entire history was scanned. The query now also carries the two bounds hoisted above the `$or`,
over the earliest fragment's begin and the latest fragment's end. Implied by the branches, so no
result changes; see the #203 note above for the cost that remains.

### Plan-shape test hardened

`MongoBucketQueryPlanTest` now runs against an adversarial index set (the retired `pvName_1` and
`(pvName, firstTime.seconds, firstTime.nanos)` indexes plus a `(pvName, lastTime, firstTime)` one),
asserts every candidate plan is on the shipped index exactly, asserts no blocking sort stage and a
two-sided `firstTime.seconds` interval, and covers the V2 fragment `$or`, the V2 keyset-seek page,
and the pattern path. The sharded plan shape (the SLAC deployment) remains unpinned — no sharded
cluster in CI. A companion test, `MongoSyncQueryClientMissingIndexTest`, pins that a missing hinted
index is reported to the caller as an error on all four retrieval paths rather than throwing inside
the handler's worker thread.

## Service metrics (Issue #212)

Every service now collects and exports metrics — request rates, error rates, latency histograms, a
per-stage breakdown of query handling, MongoDB command durations, handler queue and worker
saturation, gRPC call metrics, and JVM runtime metrics. The operator reference is
[`doc/metrics.md`](../metrics.md), which includes the PromQL for diagnosing a slow query.

### DEPLOYMENT CHANGE: each service now binds a second port, and fails to start if it cannot

Metrics are **on by default**, served on a Prometheus scrape endpoint per service:

| Service          | gRPC port | metrics port |
|------------------|-----------|--------------|
| Ingestion        | 50051     | 9464         |
| Query            | 50052     | 9465         |
| Annotation       | 50053     | 9466         |
| Ingestion Stream | 50054     | 9467         |

(The benchmark servers use 60451 and 60452, so a benchmark still runs on a host running the live
services.)

**If a metrics port cannot be bound the service fails to start**, with an error naming the endpoint
and the cause. This is deliberate — the alternative is a service an operator believes is
instrumented and is not — but it means an upgraded deployment needs these four ports free, or the
settings below changed. Check for a conflict before upgrading: 9464–9467 are in the range some
Prometheus exporters use by convention.

**On Kubernetes**, the metrics port is an ordinary container port. `doc/metrics.md` now carries the
`containerPort`, `ServiceMonitor`, and `prometheus.io/*` annotation forms, and the released image
declares all eight ports (its previous `EXPOSE 8080` named a port no service listens on). Note that
a port collision here is `CrashLoopBackOff` rather than a pod running without metrics, since the
bind failure is deliberate.

Each port is configurable (`DP_INGESTION_SERVER_METRICS_PORT`, `DP_QUERY_SERVER_METRICS_PORT`,
`DP_ANNOTATION_SERVER_METRICS_PORT`, `DP_INGESTION_STREAM_SERVER_METRICS_PORT`), the bind interface
is `DP_TELEMETRY_PROMETHEUS_HOST` (default `0.0.0.0`; set `127.0.0.1` to expose metrics only to a
local scraper), and the whole feature is switched off with `DP_TELEMETRY_ENABLED=false` — which
binds no port and records nothing.

**There is no authentication on the scrape endpoint.** It exposes no data values and no PV names,
but it does reveal request rates, latencies, and collection names; bind it to the loopback interface
on a shared host.

To push to an OpenTelemetry collector instead of being scraped, the standard OTel environment
variables apply with no rebuild: `OTEL_METRICS_EXPORTER=otlp` plus `OTEL_EXPORTER_OTLP_ENDPOINT`.

### New: slow query log

A query whose total handling time reaches `QueryHandler.slowQueryLogThresholdMillis` (default
**1000 ms**, so this is on by default) writes one WARN line to a dedicated logger named
`dp.slowquery`, carrying the per-stage breakdown (resolve / queue / database / process) and the
shape of the request — PV count, first few PV names, time range, page size. It answers "why was
*this* query slow" without a trace backend.

The line goes to the existing root appender unless routed. `log4j2.xml` ships a
`<Logger name="dp.slowquery" level="warn"/>` entry; add an `AppenderRef` with `additivity="false"`
to send these to their own file. Set the threshold to `0` to log every query, or negative to
disable. It is read once at startup.

Also added to `log4j2.xml`: `io.opentelemetry` at `warn`, so the SDK does not log on every export
interval.

### Ingestion latency is now measurable

`dp.ingest.duration` measures from request arrival to the end of handling, **including
persistence**. This is the number the gRPC call duration cannot show: ingestion acknowledges a
request as soon as it is validated and enqueued, so `grpc.server.call.duration` on an ingestion
method covers validation and enqueue only. An operator watching that alone would see a healthy few
milliseconds while the queue behind it fell arbitrarily far behind.

### Note for anyone writing alerts

Two behaviors of `db.client.operation.duration` are not what they look like, both verified against a
real MongoDB and documented in `doc/metrics.md`:

- `error.type` covers only commands the server **refused**. A duplicate key or a failed document
  validation is a write error carried inside the response body of a command the server answered
  successfully, so it never appears here. Use the application-level outcome counters for write
  failures.
- A total database outage makes this metric **go silent** rather than raising an error rate — a
  server-selection failure emits no command events at all. Alert on absence of data.

Likewise, a request rejected by gRPC-layer field validation never reaches the handler, so it is not
counted in `dp.query.requests` — and because the service reports a rejection as an `OK` response
carrying an `ExceptionalResult` rather than as a gRPC error, it is not visible as a non-`OK`
`grpc_status` either. Such requests are still logged with their reason; counting them is
follow-on issue #279.

## querySamples correctness and query-path hardening (Issue #274)

### BUG FIX: unary `querySamples` silently omitted PVs on large pages

Before this release, a unary `querySamples` page was assembled by draining the bucket cursor until
the outgoing message budget tripped. The cursor is ordered by PV, so when the budget tripped partway
through the first PV, every later PV was emitted as all-unset values with no error, and the page
token resumed at the same position — those PVs were never returned. With the default budget this
affected any request whose first PV (in name order) had more than roughly 455,000 samples in the
window: about 7.5 minutes at 1 kHz, 12 hours at 10 Hz, or 5 days at 1 Hz.

Pages are now retrieved in time slices, each covering every selected PV, so every row on a page is
complete across PVs. Behavior changes a client may notice:

- A page ended by the byte budget may hold fewer rows than `limit`; `nextPageToken` resumes at the
  first time slice that did not fit. Page boundaries are still gap-free and duplicate-free.
- A small `limit` now saves server work (previously the whole budget was retrieved regardless).
- New configuration key `QueryHandler.queryV2SamplesInitialSliceSeconds` (default 60), the length
  of the first slice; later slices adapt toward `limit`.

### `querySamplesStream` is now bounded in memory

The stream previously assembled the entire requested window in memory before emitting. It now emits
as slices are retrieved, bounded by the message budget plus one slice. A request over a very long
range no longer risks exhausting the query server's heap.

### Streaming responses now apply outbound flow control

`queryDataStream`, `queryBucketsStream`, and `querySamplesStream` wait for the client to drain the
gRPC transport buffer before sending the next message. A slow client no longer causes the server
to buffer the whole result. New configuration key `QueryHandler.streamReadyTimeoutSeconds` (default
300): how long a response waits for a stalled client before it is abandoned. A cancelled call stops
promptly.

**Operational trade:** the wait happens on the query worker serving the stream, so a slow reader now
occupies one of `QueryHandler.numWorkers` (default 7) for the life of its stream instead of growing
the heap. A site with many long-lived streaming consumers should size the worker count for them;
`dp_handler_workers_active` against `dp_handler_workers_max` shows saturation, and streams cut
short by a cancel or a readiness timeout are counted under the new `dp.outcome=abandoned` value of
`dp.query.requests`.

### Bucket retrieval is partitioned by span class

The #232 per-PV span bound was applied as one maximum over all PVs in a request, and measurement
showed each index key inside that bound is a document fetch. One long-span PV in a request therefore
made every other PV's retrieval fetch that span's worth of history. Requests are now partitioned
into power-of-two span classes, one find per class bounded by its own maximum, merged in sort
order. A request whose PVs share one class is unchanged. The V1 `queryTable` pattern form (no PV
list) keeps a single bound.

### Table assembly hot path

Column-index lookup during `queryTable`/`querySamples` assembly was a linear search per sample; it
is now constant time.

## Query benchmarks (Issue #275)

Five new query benchmark clients cover `queryTable`, `querySamples`, `querySamplesStream`,
`queryBuckets`, and `queryBucketsStream`, and the loader takes options for history depth per PV,
long-span PVs, and fixture reuse (`--skip-load`). See `doc/benchmark-overview.md`, section 6. The
plan-shape test now includes a deep-history case pinning that bucket scan cost is independent of a
PV's history depth.

## Client API layer (`com.ospreydcs.dp.client`)

The convenience client layer shipped in the same jar gained coverage for most of the APIs above,
and several fixes to how it reports failures:

- **New wrappers:** `AnnotationClient.savePvMetadata()` (#224), `saveConfiguration()`,
  `saveConfigurationActivation()`, and `getConfiguration()`; query and get wrappers for PV
  metadata and configuration (#243); the Sample Status API (#238); `getDataSet()`,
  `getAnnotation()`, and `getCalculations()` (#248 Phase 2); and `QueryClient` wrappers for all
  four Query API V2 methods — `queryBuckets`, `queryBucketsStream`, `querySamples`,
  `querySamplesStream` (#244). The V2 wrappers model the `PvSelector` oneof as a sealed type so an
  invalid combination fails to compile, drop `pageToken` on the streaming forms (the server rejects
  it there), and report a streamed `useSerializedColumns` result that spans more than one page as
  fragmented rather than silently mis-assembled.
- **Blank criterion values never reach the server (#243).** A blank `prefix`/`contains` value was
  a silent match-all on the server; every criterion builder now drops blank and null entries, and a
  criterion whose entries are all blank is omitted. This holds for the V2 `metadataQuery` selector
  too. The one deliberate exception is a `configurationCriteria` list from which no criterion
  survives, which is forwarded empty for the server to reject — omitting it would silently widen
  the query.
- **Results expose the service's classification (#230).** `ApiResultBase` carries an
  `ApiResultStatus` and `isReject()`, so a caller can tell a rejection from a service error
  without matching on the message. Server rejection messages are passed through unmodified (#240);
  previously they were prefixed with the observer's class name.
- **Observer fixes:** an observer that received an error no longer leaves its caller waiting until
  the await timeout; an await timeout is reported as an error rather than an empty success; a
  duplicate response and a save response missing its result field are reported as failures
  instead of yielding a default-valued success.
- Column metadata can be attached to ingestion requests built through `IngestionRequestParams`
  (`setColumnMetadata`/`clearColumnMetadata`).

## Dependencies and build

- log4j 2.25.4 → 2.25.5 (Dependabot alert).
- `cisd:jhdf5`, which is not on Maven Central, is vendored under `third-party/cisd-jhdf5/` so a
  build no longer depends on `maven.scijava.org` being up (a 503 on that host broke CI in August).
- GitHub Actions are pinned to commit SHAs; the release image workflow resolves the dp-grpc ref
  from the pom version on release builds. Note the fallback: if `rel-<pom dp-grpc.version>` does
  not exist in dp-grpc and no explicit `dp_grpc_ref` was supplied, the workflow silently builds
  against dp-grpc `main` rather than failing. **Tag dp-grpc `rel-1.16.0` before building the
  release image**, or the published image may be built against a different proto revision than the
  release it is named for. (`release.yml`, which builds the release artifacts, has no such
  fallback — it fails outright when the matching tag is absent.)

