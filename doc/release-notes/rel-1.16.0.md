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
