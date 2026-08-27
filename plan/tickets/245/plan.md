# dp-service #245: treat an empty criteria list as match-all on the three annotation queries

## Overview

`queryPvMetadata()`, `queryConfigurations()` and `queryConfigurationActivations()` all reject an empty
criteria list, and no match-all criterion exists — so "list the configurations" and "list the PVs" are
unaskable. osprey-dcs/dp-desktop-app#39 adds explore views over both domains whose natural entry
point is browse-all.

This ticket removes the empty-criteria rejection from all three, and gives `queryPvMetadata` a default
`limit` of 100 so that match-all cannot become an unbounded query.

Scope is three `*Job.java` validation blocks, one limit default in `MongoSyncAnnotationClient`, three
existing tests that flip from reject to success, new paging coverage, and a paired dp-grpc PR for the
proto comments.

## Background: the ticket as filed was wrong

**#245 originally scoped the change to the two configuration queries**, on the stated premise that
`queryPvMetadata()` (and `queryProviders()` on the query service) already treated an empty criteria
set as unconstrained, making them the majority behavior to align toward. Triage during #243 found
that premise false:

- `QueryPvMetadataJob.java:38-43` — rejects `"QueryPvMetadataRequest.criteria list must not be empty"`
- `QueryConfigurationsJob.java:38-43` — rejects `"QueryConfigurationsRequest.criteria list must not be empty"`
- `QueryConfigurationActivationsJob.java:38-43` — rejects `"QueryConfigurationActivationsRequest.criteria must not be empty"`
- `QueryServiceImpl.java:591` — `queryProviders` rejects likewise

All three annotation queries reject, and the proto comments document the rejection for all three.
There is no majority behavior to align with; this is a change on three methods, not an alignment of
two toward a third. The issue description has been corrected and retitled.

`queryPvMetadata` is included because dp-desktop-app#39 task 4's PV Metadata explore view needs the
same browse-all entry point as the configuration view. Had this shipped as originally scoped, that
view would have remained blocked and the omission would have surfaced only downstream.

## Background: the three methods are not symmetric on result size

This is why `queryPvMetadata` cannot simply be added to the list.

| Method | unset `limit` | `nextPageToken` when truncated |
|---|---|---|
| `queryConfigurations` | defaults to 100 (`MongoSyncAnnotationClient.java:962`) | non-empty — truncation discoverable |
| `queryConfigurationActivations` | defaults to 100 (`:1246-1247`) | non-empty — truncation discoverable |
| `queryPvMetadata` | **unbounded** (`:681`, `:704`) | always empty — truncation undetectable |

The two configuration methods are safe by default: they cap at 100, and configurations are
low-cardinality by nature. `queryPvMetadata` is not — it returns everything and materializes all
matches into an `ArrayList` before responding. At facility scale (10^4–10^5 PVs) match-all with an
unset limit is a server memory event, and because `nextPageToken` is only produced when `limit > 0`
(`:713`), the caller cannot even tell it happened.

Relaxing `queryPvMetadata` without a default limit would therefore convert an always-rejected request
into the most expensive query the service can run, silently. **Decided: default 100**, matching the
other two.

## Design decisions

### D1 — Apply the `queryPvMetadata` default limit unconditionally, not only to match-all

The narrower alternative — default to 100 only when the criteria list is empty, leaving
criteria-bearing queries unbounded — was rejected. It would make the page size depend on whether the
caller supplied criteria, so a client that removes its last filter silently switches from "everything"
to "first 100 with a token", which is a surprising coupling between two unrelated request fields.

Unconditional is also what makes the change small. Setting `limit = 100` when unset means the
existing `limit > 0` branches at `:702` and `:713` take the already-correct paged path, and the
unbounded `else` at `:704` becomes dead code to be deleted. The result is that all three methods then
share one shape.

Note this is a behavior change for existing `queryPvMetadata` callers who omit `limit`: they go from
receiving everything to receiving 100 plus a `nextPageToken`. That is the "silent behavior change"
#210 option 2 flags. It is accepted here, for two reasons: the truncation is discoverable via the
token (unlike today's silent unbounded read), and the only in-repo caller is the test suite. Call it
out in the PR description so it is not discovered by a downstream client.

### D2 — This does not preempt #210

#210 owns the unset-`limit` semantic across all five annotation query methods. This ticket sets a
default for one of them because match-all makes the status quo indefensible, not because it is
deciding the general question. If #210 later chooses unbounded across the board, **the match-all
default should survive as a documented exception** — match-all is precisely the case where an
unbounded default cannot be justified. Record that in #210 when this lands.

### D3 — Per-criterion validation is untouched

Only the empty-*list* check is removed. Every per-criterion check stays: a supplied criterion with
blank/empty contents is still a rejection (blank attribute key, empty tag values, missing timeRange
bounds, unset criterion oneof). The distinction is "no filters requested" versus "a filter was
requested but is malformed", and only the former becomes legal.

This matters for the #243 client wrappers, whose builders emit **no criterion** for an omitted filter
rather than an empty one — precisely so an omitted filter reaches the server as match-all rather than
as a malformed criterion.

## Implementation tasks

### Task 1 — Remove the empty-criteria rejection from the three jobs

**Files:**
- `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/job/QueryPvMetadataJob.java`
- `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/job/QueryConfigurationsJob.java`
- `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/job/QueryConfigurationActivationsJob.java`

Delete the `if (request.getCriteriaList().isEmpty()) { ... return; }` block at the top of each
`execute()` (lines 38-43 in each). Leave the per-criterion validation loop that follows unchanged.

Replace each with a brief comment noting that an empty criteria list is match-all by contract, so a
future reader does not restore the check as an apparent omission.

No change is needed in `MongoSyncAnnotationClient` for the filter itself — all three already handle an
empty filter list:

- `:677-679` — `Filters.exists(BSON_KEY_PV_METADATA_PV_NAME)`
- `:958-960` — `Filters.exists(BSON_KEY_CONFIGURATION_NAME)`
- `:1234` — `new org.bson.Document()`

These fallbacks are currently unreachable from the service (the jobs reject first) but are correct;
this ticket makes them live. Verify the two `Filters.exists` forms match every stored document —
`pvName` and `configurationName` are the required natural keys, so they should, but a document
predating the field would be silently excluded from match-all.

### Task 2 — Give `queryPvMetadata` a default limit of 100

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/client/MongoSyncAnnotationClient.java`

At `:681`, replace:

```java
final int limit = request.getLimit() > 0 ? request.getLimit() : 0;
```

with the 100 default, matching `:962`:

```java
final int limit = request.getLimit() > 0 ? request.getLimit() : 100;
```

Then simplify the now-dead unbounded path at `:700-706` — with `limit` always positive, the
`if (limit > 0)` branch is unconditional and the `else { query.into(documents); }` is unreachable:

```java
query.limit(limit + 1).into(documents);
```

And at `:713` the `limit > 0 &&` guard becomes redundant; reduce to `if (documents.size() > limit)`,
matching `:988`.

Define the default as a named constant rather than a third literal `100` — the other two sites
(`:962`, `:1247`) currently hardcode it. Add e.g.
`private static final int DEFAULT_QUERY_LIMIT = 100;` and use it at all three sites, so a future
change to the default cannot land on two of three.

### Task 3 — Flip the three existing rejection tests

**Files:**
- `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/PvMetadataIT.java:130-135`
- `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/ConfigurationIT.java:232-238`
- `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/ConfigurationIT.java:847-853`

Each currently asserts the rejection, e.g.:

```java
@Test
public void testQueryPvMetadataRejectEmptyCriteria() {
    annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
            List.of(), 0, null, true, "criteria list must not be empty", 0);
}
```

Rename to `testQueryXxxEmptyCriteriaMatchesAll` and assert success with the expected record count
against the existing scenario builders (`createQueryConfigurationsScenario()`,
`createQueryActivationsScenario()`, and the PvMetadata equivalent). Keep them adjacent to the
per-criterion rejection tests, which stay as they are.

### Task 4 — New coverage for match-all and the limit default

**Files:** the same three IT files.

- **Match-all + paging enumerates everything.** With more records than the page size, page through
  with an explicit small `limit` using empty criteria, and assert the union equals the full set with
  no duplicates and a blank final `nextPageToken`.
- **Unset `limit` with empty criteria returns the default page size, not everything.** This is the
  regression guard for the hazard this ticket exists to avoid. Requires creating >100 records in one
  test — check the runtime cost of the save path before committing to it; if it is slow, assert
  against a temporarily lowered constant via the same seam the other configurable limits use, rather
  than dropping the test.
- **`queryPvMetadata` with unset `limit` now returns a non-empty `nextPageToken`** when more records
  exist — today it is always blank. Pins D1's behavior change.
- **Per-criterion rejections still reject** — at least one per method (blank attribute key, empty tag
  values), confirming Task 1 removed only the list-level check.

Note `sendAndVerifyQuery*` does not expose `nextPageToken`; the existing pagination tests drop to a
raw stub to see it (`ConfigurationIT.java:387-390` carries a comment about this). Either follow that
pattern or thread the token out of the helpers — the latter would also let
`testQueryConfigurationsPagination` assert its blank final token, which it currently omits.

### Task 5 — Paired dp-grpc PR: proto comments

**File:** `dp-grpc/src/main/proto/annotation.proto`

Three message header comments document the rejection and must change together:

- `QueryPvMetadataRequest` (`:963-984`)
- `QueryConfigurationsRequest` (`:1377-1394`)
- `QueryConfigurationActivationsRequest` (`:1791-1809`)

Each contains:

```
 *   - An empty criteria list is rejected with an ExceptionalResult; at least
 *     one criterion is required.
```

Replace with a statement that an empty criteria list matches all records, and note the default page
size so the bound is discoverable from the API contract. Regenerate with `mvn clean compile` in
dp-grpc; no generated-code change reaches dp-service, since comments do not affect the wire format or
the Java API.

**Sequencing:** the dp-grpc PR is comment-only and can merge in either order relative to the
dp-service PR. Do not let it gate the behavior change, but do not let it be forgotten either — a proto
that documents a rejection the server no longer performs is worse than one that says nothing.

## Out of scope

- **The general unset-`limit` semantic** — #210 owns it. See D2.
- **Keyset paging migration** — #193. This ticket keeps skip/offset; the match-all case inherits
  whatever #193 lands.
- **`queryProviders` on the query service** (`QueryServiceImpl.java:591`) — also rejects empty
  criteria, and was miscited in this ticket's original premise. Out of scope: it is a different
  service with no explore-view consumer. Worth a follow-up ticket for consistency; note it when this
  lands.
- **`queryDataSets` / `queryAnnotations`** — reject empty criteria in `AnnotationServiceImpl` (`:217`,
  `:453`) and have no paging at all. Their modernization is already planned separately (#210 notes it).
- **The silent malformed-`pageToken` reset** — all three queries ignore a bad token and restart at
  page 1. Documented in the #243 wrapper javadoc; a separate ticket if it should change.

## Dependencies and sequencing

- **Independent of #243.** Zero file overlap: #243 is `AnnotationClient` plus new result classes and
  client ITs; this is three jobs, one Mongo client method, and three server ITs. Either can land
  first, in separate PRs. #243's wrappers work against today's server; only the desktop app's
  browse-all views need this ticket.
- **Both should ship in the same release** — dp-desktop-app#39 task 1 needs #243, #244 and #245
  together, since tasks 4–5 need the wrappers *and* browse-all.
- **Paired dp-grpc PR** for Task 5, mergeable in either order.
- Record the D2 note on #210 when this lands, so the match-all default is not later "corrected" into
  whatever general semantic #210 picks.
