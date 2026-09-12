# dp-service 1.16.0 Release Notes (draft)

This file collects the release-note lines for changes landing in 1.16.0 as they merge; it is a
draft until the release is cut.

## Modernized DataSets and Annotations APIs (Issues dp-grpc #132, dp-service #248)

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
ingested for each PV, taken as the maximum over the PVs the request names. A PV with normal bucket
sizes is no longer penalized by an unrelated PV's outliers. On an archive where the configured
limit had been raised to accommodate outliers, this is the difference between a lookback measured
in weeks and one measured in seconds for the well-behaved majority.

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
requires an ingestion restart, are in [`doc/schema-migration.md`](../schema-migration.md).

SLAC-specific upgrade sequencing, with the site's measured numbers and verification queries, is in
[`doc/upgrade-1.16-slac.md`](../upgrade-1.16-slac.md).

### Known limitation: per-fragment cost under a ConfigurationSelector (#203)

A `querySamples` or `queryBuckets` request carrying a `ConfigurationSelector` resolves to multiple
retrieval fragments, and each fragment contributes its own copy of the lower bound — so index-scan
cost grows linearly with fragment count even though the result set does not. #232 shrinks each
fragment's window substantially (that window is now the PV's own span, not the archive's worst),
so these queries get materially faster in absolute terms, but the multiplier itself is unchanged
and is tracked separately as issue #203. Requests without a `ConfigurationSelector` resolve to a
single interval and are unaffected.

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
the migrating host is alive. See `doc/schema-migration.md` for triage guidance and the migration
inventory.

*(Phases 1 and 2 — the modernized message shapes, entity/audit fields, and new CRUD methods —
are also part of 1.16.0; their notes are collected when this draft is finalized.)*
