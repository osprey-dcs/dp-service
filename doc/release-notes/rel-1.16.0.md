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

### BEHAVIOR CHANGE: saveAnnotation validates calculations content fully (#248 Phase 4)

`saveAnnotation` now validates the complete shape of a `Calculations` payload: every column of
every type must have a non-blank name and non-empty values, and **its value count must equal the
frame's timestamp count** (for array columns, timestamp count times the dims product;
`SerializedDataColumn` entries carry no countable values and get name checks only). Column names
must be unique across all column types within a frame, and frame names must be unique within the
Calculations object — both are addressing keys for `CalculationsSpec` and provenance links.
Column metadata is checked against the same limits as ingestion.

This narrows what was previously accepted for legacy `DataColumn` lists: a column shorter or
longer than its frame's timestamp axis used to be stored as-is — and a short column would hang a
subsequent tabular export with no response. A frame carrying only typed columns (no legacy
`dataColumns`) is now accepted; previously it was wrongly rejected by a legacy-list-only
emptiness check.

*(Phases 1 and 2 — the modernized message shapes, entity/audit fields, and new CRUD methods —
are also part of 1.16.0; their notes are collected when this draft is finalized.)*
