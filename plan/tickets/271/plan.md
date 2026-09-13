# dp-service #271: pin bucket queries to the compound index; bound the scan on both sides; stale-index upgrade guidance

Epic: #257. Follow-on to #232, from the 2026-09-13 review of the two SLAC query-performance
reports (the 4-minute and the ~30-second bucket queries) and of the earlier session's
`query-performance.md` research summary.

## Overview

No index change. The compound bucket index
`(pvName, firstTime.seconds, firstTime.nanos, lastTime.seconds, lastTime.nanos)` is the right
index for the overlap filter and the `(pvName, firstTime)` sort. This ticket makes every bucket
query use it reliably and makes the index scan cover the query window on both sides:

- **`hint`** the compound index on every bucket retrieval query, through one `bucketFind()`.
- **Upper bound** `firstTime.seconds <= endSeconds` in the shared overlap filter, implied by its
  `firstTime < end` half.
- **Hoisted bounds** above the V2 fragment `$or`, over the fragments' earliest begin and latest end.
- **Plan-shape test** hardened against an adversarial index set, with V2 and pattern coverage.
- **Operator guidance** for SLAC: inventory and drop the leftover `pvName`-led indexes; send the
  shard key.

No proto change, no schema migration (no stored data changes; the index declaration is identical).

## Background: triage findings

### 1. Both slow reports are explained by the lower bound, not the index

The 4-minute query had no `firstTime` lower bound at all (#197 added one). The ~30-second query ran
with the archive-wide 3,700,000 s span that #232 replaces with the per-PV value. The 53 s
`optimizationTimeMillis` seen on #231 came from a hinted, zero-row query with a 2009 begin time
under that span: the multi-plan trial *executes* each candidate's scan and only stops early at 101
results, so for a small result set optimization time ≈ execution time. Reproduced locally (mongo
8.0.6, 200k one-second buckets, exact production filter and sort): 197 ms exec / 105 ms
optimization at span 3,700,000; 1 ms / 1 ms at span 200. It shrinks with #232; it is not a
separate problem. The research summary's "not addressed by #232" claim and its "hard 32 MB sort
failure" claim (8.0 spills at 100 MB with `allowDiskUseByDefault: true`) are both stale.

### 2. Extra `pvName`-led indexes are the residual second-order cost

`MongoClientBase.createMongoIndexesBuckets()` is purely additive and startup never drops an index.
Shapes earlier releases declared and later removed — `pvName_1` (rel-1.15.0, #197 commit
`be6aa25`), and `(pvName, firstTime.seconds, firstTime.nanos)`, `(pvName, firstTime.dateTime)`,
`(pvName, lastTime.dateTime)` (beta-1.6.0, commit `9a2db17`) — are still on any archive created
before those releases, and no release note ever told operators to drop them. SLAC's archive has 8
indexes, 3 not created by current code, plus the `(pvName, lastTime, firstTime)` built during the
August troubleshooting. Measured on the 200k fixture: shipped index alone → 3 candidate plans;
with `pvName_1` and the operator index → 11 candidates, the wide-span historical query slowed from
197 ms to 529 ms, and on recent-window queries the planner picked the operator index with a
blocking `SORT` stage. Hinting the shipped index restored the 3-candidate numbers exactly, with
the `$or` bounds intact.

### 3. The plan test's new coverage found two shapes where the bound never reached the planner

Running the exact production queries under `explain` against the adversarial fixture:

- **V2 fragment `$or`** (`bucketBaseFilterV2`, `executeQuerySamplesV2`, and the keyset-seek page):
  the winning plan was a single `IXSCAN` with `firstTime.seconds: [MinKey, MaxKey]` — each named
  PV's whole history — with or without the hint. The per-fragment bounds inside the `$or` branches
  are index bounds only for the `OR`/`SORT_MERGE` candidates, which the planner did not choose.
  This is pre-existing (#203 was filed on the assumption that each fragment's bound applied).
- **No upper bound anywhere.** The `firstTime < end` half of the overlap predicate is a
  `(seconds, nanos)` `$or` the planner cannot use as a bound, so the single-scan plan ran from
  `begin − span` to the end of each PV's history and discarded the rest by filter (the pre-existing
  test documented this: "the index scan runs from begin − span to the end of the PV's history").
  The `SORT_MERGE` candidate that explodes the `$or` does have an upper bound, so which plan won
  decided whether a historical query scanned the window or the PV's entire later archive.

Both fixes are implied predicates: `firstTime < end ⇒ firstTime.seconds <= endSeconds`, and every
`$or` branch's bounds ⇒ the extremes over all branches. Neither changes any result.
`MongoSyncQueryBucketsV2Test` (result-level) and `MongoQueryFilterBuilderTest` (BSON shape) confirm.

### 4. The pattern path is case-insensitive, which denies the planner a `pvName` prefix range

`executeQueryTable` compiles `PvNamePattern` with `Pattern.CASE_INSENSITIVE`; the plan then bounds
`pvName` to `["", {})` and visits every PV's keys within the `firstTime.seconds` interval. The bound
still applies per PV. Pre-existing API behavior, not changed here; recorded for a future decision.

## Design decisions

- **D1: hint by key pattern, not by name, and share the object with the declaration.**
  `MongoClientBase.BUCKET_QUERY_INDEX_KEYS` is passed to both `createIndex` and `hint`, so the two
  cannot drift; a name would match only the default-named index, and a key pattern matches the index
  however it was named. Rejected: an `indexFilter` (server-side, per-deployment, invisible to code).
- **D2: one `find()` for all bucket retrieval.** `MongoSyncQueryClient.bucketFind()` applies
  filter, sort, and hint; V1 `bucketDocumentQuery` and the three V2 paths call it. Package-private
  `FindIterable` builders (`bucketQueryV2`, `bucketSamplesQueryV2`) mirror the existing V1 split so
  the plan test can `explain()` the exact production queries.
- **D3: a missing index fails loudly.** With a hint, a missing index is a driver error on every
  query, not a fallback to a collection scan. Chosen deliberately: startup re-creates the index, and
  a silent full scan on 33.8M buckets is the four-minute query that started #257.
- **D4: the two `firstTime.seconds` bounds live in one helper.**
  `MongoQueryFilterBuilder.bucketFirstTimeSecondsIndexBounds()` returns the (saturating) lower bound
  and the upper bound as plain top-level predicates; `bucketOverlapsRangeFilter` uses it for one
  window and `fragmentsOverlapFilter` hoists it over many. Rejected: emitting the upper bound only
  in the V2 path (the V1 single-scan plan has the same problem).
- **D5: the plan test asserts "every candidate on the shipped index", not "no rejected plans".**
  The hint restricts the planner to one index, not one plan: it still enumerates and ranks the
  `SORT_MERGE` explosions of the `(seconds, nanos)` `$or`s on that index. The unhinted
  counterfactual pins that the fixture is adversarial (rejected plans on other indexes exist).
- **D6: no automatic index drop.** Reconciling live indexes against the declared set would drop an
  index an operator added deliberately (CLAUDE.md, Schema Migration). Guidance instead, in the SLAC
  runbook, with a guarded mongosh snippet.

## Implementation tasks

- `src/main/java/.../common/mongo/MongoClientBase.java`: `BUCKET_QUERY_INDEX_KEYS`; use it in
  `createMongoIndexesBuckets()`.
- `src/main/java/.../common/mongo/MongoQueryFilterBuilder.java`: `bucketFirstTimeSecondsIndexBounds()`;
  `bucketOverlapsRangeFilter` emits both bounds.
- `src/main/java/.../query/handler/mongo/client/MongoSyncQueryClient.java`: `bucketFind()`,
  `bucketQueryV2()`, `bucketSamplesQueryV2()`, `fragmentsOverlapFilter()`, shared `bucketSort()`;
  V1 and V2 paths routed through them.
- `src/test/java/.../common/mongo/MongoQueryFilterBuilderTest.java`: expected filters carry the
  upper bound.
- `src/test/java/.../query/handler/mongo/client/MongoBucketQueryPlanTest.java`: adversarial index
  set, every-candidate-on-shipped-index and no-`SORT` assertions, two-sided interval assertions,
  V2 fragment/keyset/samples and pattern coverage, hinted-key-exists check, unhinted counterfactual.
- `doc/upgrade-1.16-slac.md`: index inventory and drop step; shard key request; #203 wording.
- `doc/release-notes/rel-1.16.0.md`: #271 section; #203 known-limitation wording.
- `CLAUDE.md`: bounds, `$or` hoist, and hint invariants; plan test description.

## Out of scope

- **#203** — the between-fragment scan cost that remains after the hoisted bounds.
- **#198** — single total-nanos fields; the hint removes the plan-selection cost that partly
  motivated it, and the two-sided bound now covers the window exactly at seconds granularity.
- **#201** — the per-call metadata aggregation over all buckets per PV.
- **#258** — repairing the four GapAct PVs' 42-day buckets (their own span still widens their bound).
- The case-insensitive pattern path (triage 4).
- Sharded plan-shape coverage: the explain walker reads the unsharded shape; CI has no sharded
  cluster. The SLAC shard key is still unknown and is requested in the runbook.

## Dependencies and sequencing

Nothing blocks on this. It should ship in 1.16.0 with #232, since the runbook step and the release
note assume both. It does not depend on #258 or on SLAC dropping the leftover indexes — the hint
makes plan choice independent of them.
