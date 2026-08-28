# dp-service #245: treat an empty criteria list as match-all on the three annotation queries

## Overview

`queryPvMetadata()`, `queryConfigurations()` and `queryConfigurationActivations()` all reject an empty
criteria list, and no match-all criterion exists — so "list the configurations" and "list the PVs" are
unaskable. osprey-dcs/dp-desktop-app#39 adds explore views over both domains whose natural entry
point is browse-all.

This ticket removes the empty-criteria rejection from all three, and gives `queryPvMetadata` a default
`limit` of 100 so that match-all cannot become an unbounded query.

Scope is three `*Job.java` validation blocks, one limit default in `MongoSyncAnnotationClient`, six
existing tests that flip from reject to success, new paging coverage, and a paired dp-grpc PR for the
proto comments.

**Re-triaged 2026-08-28, after #243 merged.** Every production-code citation below was re-verified
against the branch and still holds; #243 touched only `AnnotationClient` and two client ITs, so there
is no file overlap. Three things changed: #243 added three client-side pins of the rejection that must
flip alongside the server ones (Task 3 — six tests, not three), those same client wrappers now expose
`nextPageToken` and are the better home for the new paging coverage (Task 4), and dp-grpc's proto line
numbers have moved (Task 5).

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
(`:962`, `:1247`) currently hardcode it. Add
`private static final int DEFAULT_QUERY_LIMIT = 100;` and use it at all three sites, so a future
change to the default cannot land on two of three.

**A plain constant, not a config key.** Making it operator-tunable was considered — the hazard this
ticket addresses lives precisely at facility scale, where an operator might want a different bound —
and rejected for now. It would mean a new key in *both* `application.yml` files (the test resource
shadows the main one) and it edges into the general unset-`limit` semantic that D2 keeps out of this
ticket. Note the hardcoded default in the PR description so the constraint is visible if an operator
later asks for it.

### Task 3 — Flip the six existing rejection tests

Six tests pin the current rejection, not three. The plan originally listed only the server-side ITs;
#243 merged after it was written and added a client-side pin for each of the three methods.

**Server ITs** (`sendAndVerifyQuery*` wrappers):
- `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/PvMetadataIT.java:131` — `testQueryPvMetadataRejectEmptyCriteria`
- `.../ConfigurationIT.java:233` — `testQueryConfigurationsRejectEmptyCriteria`
- `.../ConfigurationIT.java:848` — `testQueryConfigurationActivationsRejectEmptyCriteria`

**Client ITs** (`AnnotationClient` wrappers, added by #243):
- `.../PvMetadataClientIT.java:764` — `testQueryPvMetadataRejectsEmptyCriteria`
- `.../ConfigurationClientIT.java:1167` — `testQueryConfigurationsRejectsEmptyCriteria`
- `.../ConfigurationClientIT.java:1250` — `testQueryConfigurationActivationsRejectsEmptyCriteria`

The client-side three are deliberate pins, not oversights — each carries a javadoc naming this
ticket, e.g. "a silent pass here after #245 merges means the relaxation did not reach
queryPvMetadata, which is exactly the gap #245's triage found." Flipping them is the confirmation
that pin was placed to get; leaving one un-flipped is a silent pass of exactly the kind it warns
about. Delete the now-obsolete "pins the CURRENT server behavior" javadoc when flipping.

A server IT looks like:

```java
@Test
public void testQueryPvMetadataRejectEmptyCriteria() {
    annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
            List.of(), 0, null, true, "criteria list must not be empty", 0);
}
```

Rename all six to `testQueryXxxEmptyCriteriaMatchesAll` and assert success with the expected record
count against the existing scenario builders (`createQueryConfigurationsScenario()`,
`createQueryActivationsScenario()`, and the PvMetadata equivalent). Keep them adjacent to the
per-criterion rejection tests, which stay as they are.

Note the client-side three currently assert `ApiResultStatus.REJECT`, `result.isReject()`, and a null
result list; the flipped form asserts `ApiResultStatus.NONE` and a populated list, matching the
neighboring success tests.

### Task 4 — New coverage for match-all and the limit default

**Put the token-bearing coverage in the client ITs, and the count-bearing coverage in the server
ITs.** The two layers expose different things, and #243 changed the calculus here:

- `sendAndVerifyQuery*` returns a bare `List<...>` and never surfaces `nextPageToken`
  (`GrpcIntegrationAnnotationServiceWrapper.java:846-873` for PV metadata; the other two are the same
  shape). The existing server-side pagination tests drop to a raw stub to see the token
  (`ConfigurationIT.java:387-390` carries a comment about this).
- #243's `Query*ApiResult` records carry `nextPageToken` as a field, and `PvMetadataClientIT:723-751`
  already pages through with it end to end.

So the "thread the token out of the server helpers" option the earlier draft floated is no longer
worth doing for this ticket — the plumbing already exists one layer up. (Threading it out remains a
reasonable standalone cleanup, since it would also let `testQueryConfigurationsPagination` assert its
blank final token, which it currently omits. Out of scope here.)

**Test-database isolation:** `GrpcIntegrationTestBase.setUp()` calls `mongoClient.init()`, which
resets `dp-test`, and `AnnotationIntegrationTestIntermediate` invokes it from `@Before`. Each test
method therefore starts empty, so a match-all count assertion is exactly the number of records that
test saved — no cross-test contamination to design around.

Coverage to add:

- **Match-all + paging enumerates everything** *(client ITs)*. With more records than the page size,
  page through with an explicit small `limit` and empty criteria; assert the union equals the full
  set with no duplicates and a blank final `nextPageToken`. Follow the shape of
  `PvMetadataClientIT.testQueryPvMetadataPagination`.
- **Unset `limit` returns the default page size, not everything** *(client ITs)*. The regression guard
  for the hazard this ticket exists to avoid. Needs 101 records in one test.
- **`queryPvMetadata` with unset `limit` now returns a non-empty `nextPageToken`** when more records
  exist — today it is always blank. Pins D1's behavior change. Folds into the test above.
- **Match-all returns the full scenario set** *(server ITs)* — the flipped Task 3 tests already cover
  this; no additional server-side test needed beyond them.
- **Per-criterion rejections still reject** — at least one per method (blank attribute key, empty tag
  values), confirming Task 1 removed only the list-level check. These already exist
  (e.g. `PvMetadataIT.testQueryPvMetadataRejectBlankAttributeKey`,
  `ConfigurationIT.java:1030`); verify they still pass rather than writing new ones.

**On the cost of the 101-record test:** the earlier draft suggested lowering the default "via the
same seam the other configurable limits use." There is no such seam — `DEFAULT_QUERY_LIMIT` is a
plain constant (Task 2), and the only config-backed page sizes on this handler are the sample-status
ones (`MongoAnnotationHandler.java:30-33`). Making this one config-backed was considered and rejected
along with Task 2's decision to keep it a constant. Just save the 101 records: these are
single-document upserts and the loop costs a few seconds, well within the existing IT budget.

### Task 5 — Paired dp-grpc PR: proto comments

**File:** `dp-grpc/src/main/proto/annotation.proto` (clean at `945d39a`)

Three message header comments document the rejection and must change together. **Line numbers below
are current as of this triage; the earlier draft cited `:963`/`:1377`/`:1791`, which dp-grpc has since
moved past — re-grep rather than trusting either set:**

- `QueryPvMetadataRequest` — comment block at `:1776-1797`, `An empty criteria list is rejected` at `:1790`
- `QueryConfigurationsRequest` — block at `:2189-2207`, rejection line at `:2200`
- `QueryConfigurationActivationsRequest` — block at `:2604-2622`, rejection line at `:2615`

Each contains:

```
 *   - An empty criteria list is rejected with an ExceptionalResult; at least
 *     one criterion is required.
```

**Two edits per block, not one.** Replace that bullet with a statement that an empty criteria list
matches all records. Then — this is the part the earlier draft folded into the first edit and is easy
to miss — extend the `Pagination:` section that immediately follows each block to state the default
page size. Today all three read:

```
 * Pagination:
 *   - limit specifies the maximum number of records to return per response.
```

with no mention of what happens when `limit` is unset. That omission is currently harmless for the
two configuration queries and actively wrong for `queryPvMetadata` after Task 2. The default belongs
in the `Pagination:` section rather than appended to the criteria bullet, because it applies
unconditionally and not only to the match-all case — that is D1, and stating it under the criteria
bullet would re-imply the coupling D1 rejects.

Regenerate with `mvn clean compile` in dp-grpc; no generated-code change reaches dp-service, since
comments affect neither the wire format nor the Java API.

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
- **A deprecation path for `queryPvMetadata`'s unbounded→100 change** (D1). Considered and dropped:
  the only in-repo caller is the test suite, and the only out-of-repo consumer is dp-desktop-app,
  which this project controls end to end and which is a demo rather than a deployed client. A
  downstream client silently receiving 100 PVs instead of all of them would be the real hazard, and
  there is no such client. Still call the change out in the PR description.
- **Threading `nextPageToken` out of the `sendAndVerifyQuery*` server helpers** — see Task 4. Would
  let `testQueryConfigurationsPagination` assert its blank final token, but #243's client wrappers
  already expose the token, so this ticket does not need it.
- **Making `DEFAULT_QUERY_LIMIT` config-backed** — see Task 2.

## Dependencies and sequencing

- **#243 has merged** (`1e21379`, PR #247), so its half of the sequencing question is settled. It
  landed `AnnotationClient` plus result records and two client ITs; this ticket is three jobs, one
  Mongo client method, and six ITs across four files. The only intersection is the three client-side
  rejection pins #243 deliberately left for this ticket to flip (Task 3) — no production-code overlap
  at all.
- **Both should ship in the same release** — dp-desktop-app#39 task 1 needs #243, #244 and #245
  together, since tasks 4–5 need the wrappers *and* browse-all.
- **Paired dp-grpc PR** for Task 5, mergeable in either order.
- Record the D2 note on #210 when this lands, so the match-all default is not later "corrected" into
  whatever general semantic #210 picks.
