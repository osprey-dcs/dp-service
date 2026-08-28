# dp-service #243: AnnotationClient query/get wrappers for PV metadata and machine configuration

## Overview

`AnnotationClient` wraps the save paths for the PV metadata and machine configuration domains, plus
`getConfiguration()`, but none of the query or get methods. All five RPCs below are fully implemented
server-side and covered by integration tests; the only thing missing is the client-layer wrapper, so
the desktop app cannot reach them.

This ticket adds wrapper methods, params records, and `ApiResult` classes for:

| RPC | Wrapper | Result class |
|---|---|---|
| `queryPvMetadata` | `queryPvMetadata(QueryPvMetadataParams)` | `QueryPvMetadataApiResult` |
| `getPvMetadata` | `getPvMetadata(String pvNameOrAlias)` | `GetPvMetadataApiResult` |
| `queryConfigurations` | `queryConfigurations(QueryConfigurationsParams)` | `QueryConfigurationsApiResult` |
| `queryConfigurationActivations` | `queryConfigurationActivations(QueryConfigurationActivationsParams)` | `QueryConfigurationActivationsApiResult` |
| `getConfigurationActivation` | `getConfigurationActivationById(String)` / `getConfigurationActivationByCompositeKey(String, Timestamp)` | `GetConfigurationActivationApiResult` |

Consumers: osprey-dcs/dp-desktop-app#39 (tasks 4–5) and osprey-dcs/dp-desktop-app#36, whose
dp-service piece (server-side activation id collision detection) is absorbed here.

## Background: triage findings

Triage verified the ticket's premises against the code. Four findings shaped the scope.

### 1. Not-found is already REJECT on both getters — nothing to fix from #235

The ticket asked to verify this explicitly. Confirmed: `GetPvMetadataDispatcher.handleResult()` and
`GetConfigurationActivationDispatcher.handleResult()` both call `send*ResponseReject` when the
document is null, matching `GetConfigurationDispatcher`. `RESULT_STATUS_ERROR` is reserved for
backend failures routed through `handleError`, and `MongoSyncAnnotationClient` deliberately does not
catch `MongoException` in the activation finders so that a backend failure cannot be misreported as
"not found" (comment at `MongoSyncAnnotationClient.java:1169-1186`). No server-side change needed.

### 2. Both `getConfigurationActivation` oneof branches are implemented

`GetConfigurationActivationJob.execute()` handles `CLIENTACTIVATIONID`, `COMPOSITEKEY`, and
`KEY_NOT_SET` exhaustively, with per-branch blank/zero validation, backed by two distinct Mongo
lookups. Absorbing dp-desktop-app#36 item 1 is therefore pure wrapper work.

### 3. #245's premise is wrong — the empty-criteria relaxation must cover `queryPvMetadata` too

#245 states that `queryPvMetadata` already treats an empty criteria list as unconstrained and scopes
the relaxation to the two configuration queries. It does not:

- `QueryPvMetadataJob.java:39` rejects `"QueryPvMetadataRequest.criteria list must not be empty"`
- `QueryServiceImpl.java:591` — `queryProviders`, the other cited precedent, rejects too

All three annotation queries reject empty criteria, and the proto comments document the rejection for
all three. There is no majority behavior to align with; the relaxation is a change on three methods,
not two.

This blocks dp-desktop-app#39 task 4, whose PV Metadata explore view needs the same browse-all entry
point as the configuration view. **Resolution: #245 has been extended to cover `queryPvMetadata`,
with its description corrected and its plan written on branch `issue-245-empty-criteria-match-all`.**
#243 does not depend on that landing first — the wrappers pass criteria through unchanged — but the
desktop explore view does.

Note the two are not symmetric on safety, which #245 accounts for: configurations are
low-cardinality and default `limit` to 100, whereas `queryPvMetadata` defaults to **unbounded** and
materializes all matches into an `ArrayList`. Match-all on `queryPvMetadata` without a default limit
is a facility-scale memory event. #245's plan (D1) gives `queryPvMetadata` the same default of 100
**unconditionally** — not only for the newly-legal match-all case — and deletes the unbounded branch
as dead code, so the divergence in finding 4 disappears when it lands.

### 4. The `limit=0` divergence is load-bearing for these wrappers

| Method | `limit=0` behavior | `nextPageToken` when truncated |
|---|---|---|
| `queryPvMetadata` | unbounded — returns everything | always `""` — truncation is undetectable |
| `queryConfigurations` | silently caps at 100 | non-empty — discoverable by paging |
| `queryConfigurationActivations` | silently caps at 100 | non-empty — discoverable by paging |

The dangerous case is `queryPvMetadata`, where a caller omitting `limit` pulls the whole collection
with no signal that it did so. The ticket's note that "desktop callers will always pass an explicit
limit" is therefore a real contract, not a passing remark, and the wrapper javadoc must state it.

**This table describes today's server, not the steady state.** #245 collapses all three rows to
"defaults to 100, truncation discoverable via `nextPageToken`". #243 and #245 may land in either
order (see Dependencies), so the wrapper javadoc must be written to be true under both: state that an
omitted `limit` yields a *server-chosen* default page size, and that callers should always pass an
explicit limit and always check `nextPageToken`. Do not document the current unbounded behavior as
the contract — that sentence would be wrong the day #245 merges.

Note also that #210, which previously owned this divergence, is **closed** — deferred out of 1.15.0
without deciding the general unset-`limit` semantic. #245 is what actually resolves the
`queryPvMetadata` case; nothing in this ticket should defer to #210.

Related, and also documented rather than changed here: all three methods **silently swallow a
malformed `pageToken`** and reset to page 0 (`logger.warn("invalid page token, ignoring")`). This is
deliberate and documented in CLAUDE.md, contrasting with the sample status API, which rejects. A
caller that corrupts a token silently restarts pagination rather than seeing an error.

## Design decisions

### D1 — Ergonomic params records, with `send*(Request)` retained as the escape hatch

The proto's combining semantics are richer than a flat record can express: criteria AND across the
top-level list, OR within a single criterion. So "tagged A **and** B" requires two separate
`TagsCriterion` entries, and a flat `List<String> tags` field can only ever mean OR.

Rather than choose between ergonomics and expressiveness, the class keeps the three-method shape it
already uses everywhere (`querySampleStatuses` / `sendQuerySampleStatuses` /
`buildQuerySampleStatusesRequest`):

- `queryXxx(params)` — ergonomic, covers the common cases
- `sendQueryXxx(request)` — raw request, full expressive power
- `buildQueryXxxRequest(params)` — public static, so a caller can start ergonomic and mutate

A future consumer with an exotic query never hits a wall; it drops to `send*`. This makes the
ergonomic form a superset of the thin alternative rather than a competing option.

### D2 — Sub-records that mirror the proto instead of flattening it

To keep the ergonomic form from silently making legitimate queries unaskable:

- **`TextMatch(List<String> exact, List<String> prefix, List<String> contains)`** — mirrors
  `PvNameCriterion` / `AliasesCriterion` / configuration `NameCriterion` exactly. One record serves
  all three.
- **`AttributeCriterion(String key, List<String> values)`** — preserves the full proto shape; a null
  or empty `values` means key-only existence search. Params take `List<AttributeCriterion>`, so
  attributes need no escape hatch.
- **`List<String> tagsAnyOf`** — one `TagsCriterion`, OR semantics, named so the semantics are
  visible at the call site.

`AttributesUtility.attributeMapFromList()` / `attributeListFromMap()` are **not** usable here: a
`Map<String,String>` cannot express multiple values per key or a key-only search.

The only things the ergonomic form cannot express are AND-of-tags and AND-of-name-matches, both
genuinely unusual. Both are reachable via `send*`, and the javadoc must say so explicitly.

### D3 — Naming: `tagsAnyOf`, not `tags`

`SavePvMetadataParams.tags()` means "the record's tags"; a query field named `tags` would mean "match
any of these" — the same name with different semantics in the same class. The suffix is worth the
mild awkwardness.

### D4 — Two named methods for the activation getter, not one overloaded pair

`getConfigurationActivation` keys off a proto `oneof`. Rather than a single wrapper taking nullable
arguments and inferring which arm to set — which would make "both supplied" and "neither supplied"
silent client-side ambiguities — expose two explicitly named methods, mirroring the two test wrappers
already in `GrpcIntegrationAnnotationServiceWrapper`
(`sendAndVerifyGetConfigurationActivationById` / `...ByCompositeKey`).

## Implementation tasks

### Task 1 — Shared criterion sub-records

**File:** `src/main/java/com/ospreydcs/dp/client/AnnotationClient.java`

Two small public records per D2, **nested as static records in `AnnotationClient`** alongside
`SavePvMetadataParams` and the other params records. A `com.ospreydcs.dp.client.params` package was
considered and rejected: no such package exists today, and every params record in the client layer is
currently nested in its client class. Revisit extraction if `QueryClient` (#244) needs them too. Each with a javadoc stating its OR semantics, and for
`AttributeCriterion`, that empty `values` means key-only existence search. Null-tolerant: a null list
is treated as empty by the builders.


### Task 2 — `ApiResult` classes

**New files** in `src/main/java/com/ospreydcs/dp/client/result/`:

- `QueryPvMetadataApiResult` — `List<PvMetadata> pvMetadata`, `String nextPageToken`
- `GetPvMetadataApiResult` — `PvMetadata pvMetadata`
- `QueryConfigurationsApiResult` — `List<Configuration> configurations`, `String nextPageToken`
- `QueryConfigurationActivationsApiResult` — `List<ConfigurationActivation> configurationActivations`,
  `String nextPageToken`
- `GetConfigurationActivationApiResult` — `ConfigurationActivation configurationActivation`

Each follows the established three-constructor shape (see `QuerySampleStatusesApiResult` for the
paged form and `GetConfigurationApiResult` for the single-record form):

1. `(boolean isError, String errorMessage)` — legacy/local failure
2. `(boolean isError, String errorMessage, ApiResultStatus apiResultStatus)` — service-reported failure
3. success constructor taking the payload

Paged results normalize a null `nextPageToken` to `""`, as `QuerySampleStatusesApiResult` does.

### Task 3 — Response observers

**File:** `src/main/java/com/ospreydcs/dp/client/AnnotationClient.java`

Five static observers extending `ApiResponseObserverBase<XxxResponse>`, each implementing
`hasExceptionalResult`, `getExceptionalResult`, and `handleResult`, following
`GetConfigurationResponseObserver` and `QuerySampleStatusesResponseObserver`.

Watch the response field naming — it is not uniform across the three query responses:

| Response | Result accessor | Repeated field |
|---|---|---|
| `QueryPvMetadataResponse` | `getPvMetadataResult()` (**not** `getQueryPvMetadataResult()`) | `getPvMetadataList()` |
| `QueryConfigurationsResponse` | `getQueryConfigurationsResult()` | `getConfigurationsList()` |
| `QueryConfigurationActivationsResponse` | `getQueryConfigurationActivationsResult()` | `getConfigurationActivationsList()` |

Each `handleResult` must call `recordFailure(...)` and return false when the expected result message
is absent, as the existing observers do.

### Task 4 — Params records and request builders

**File:** `src/main/java/com/ospreydcs/dp/client/AnnotationClient.java`

```java
public record QueryPvMetadataParams(
        TextMatch pvName,
        TextMatch aliases,
        List<String> tagsAnyOf,
        List<AttributeCriterion> attributes,
        int limit,
        String pageToken) {}

public record QueryConfigurationsParams(
        TextMatch name,
        List<String> categoryAnyOf,
        List<String> tagsAnyOf,
        List<AttributeCriterion> attributes,
        List<String> parentAnyOf,
        int limit,
        String pageToken) {}

public record QueryConfigurationActivationsParams(
        Timestamp activeAt,               // TimestampCriterion
        Timestamp rangeStart,             // TimeRangeCriterion — both or neither
        Timestamp rangeEnd,
        List<String> configurationNameAnyOf,
        List<String> clientActivationIdAnyOf,
        List<String> categoryAnyOf,
        List<String> tagsAnyOf,
        List<AttributeCriterion> attributes,
        int limit,
        String pageToken) {}
```

Corresponding public static `buildXxxRequest(params)` methods. Builder rules, matching
`buildQuerySampleStatusesRequest`:

- A null or empty field contributes **no criterion** — it is not an empty criterion. This matters:
  the server rejects an empty `TagsCriterion.values`, so emitting one for an empty list would turn an
  omitted filter into a rejection.
- `limit` set only when `> 0`; `pageToken` set only when non-null and non-blank.
- `TimeRangeCriterion` requires both bounds — the server rejects a missing or zero-valued start or
  end. If exactly one of `rangeStart`/`rangeEnd` is supplied, the builder must not emit a partial
  criterion; document that both are required.
- Note the server's zero-timestamp idiom: `epochSeconds == 0 && nanoseconds == 0` is treated as
  "unspecified", so a legitimate query at Unix epoch 0 is rejected. Document, do not work around.

Also add `buildGetPvMetadataRequest(String)`, `buildGetConfigurationActivationByIdRequest(String)`,
and `buildGetConfigurationActivationByCompositeKeyRequest(String, Timestamp)`, following
`buildGetConfigurationRequest`.

### Task 5 — `send*` and public wrapper methods

**File:** `src/main/java/com/ospreydcs/dp/client/AnnotationClient.java`

For each of the five, the standard pair, following `sendGetConfiguration` / `getConfiguration`:

```java
public XxxApiResult sendXxx(XxxRequest request) {
    // newStub, observer, dispatch on a new Thread, await
    // isError -> new XxxApiResult(true, getErrorMessage(), getApiResultStatus())
    // else    -> new XxxApiResult(payload...)
}
public XxxApiResult xxx(XxxParams params) { return sendXxx(buildXxxRequest(params)); }
```

Javadoc requirements — each of these is a triage finding that must survive into the API surface:

- **Both getters:** a missing record is reported as a rejection, not an empty success. Callers using
  the getter as an existence check branch on `isReject()`. Note that a malformed request produces the
  same status (see `ApiResultStatus.REJECT`), so validate before relying on that reading.
- **All three query methods:** an empty result is a normal success with an empty list, not a
  rejection.
- **All three query methods, on `limit`:** omitting `limit` yields a *server-chosen default page
  size*; callers should always pass an explicit limit and always check `nextPageToken`. Word it that
  way deliberately — it is true both today and after #245, whereas naming today's per-method behavior
  would go stale on merge. Do **not** state that `queryPvMetadata` returns everything when `limit` is
  unset: #245 changes exactly that.
- **A backend failure is not an empty result.** All three `executeQuery*` methods return `null` on a
  `MongoException`, which the jobs convert to `handleError(...)` → `RESULT_STATUS_ERROR` →
  `ApiResultStatus.ERROR`. Callers must not read a failed query as "nothing matched" — that is the
  #235 inversion in client-layer form. Validation failures, by contrast, arrive as
  `ApiResultStatus.REJECT` via `handleValidationError`.
- **All three query methods:** page tokens are opaque; do not parse or construct them. A malformed
  token is silently ignored server-side and pagination restarts at the first page.
- **Params limits:** AND-of-tags and AND-of-name-matches are not expressible through the params
  record; use `sendXxx(request)` with a hand-built request.

### Task 6 — Fix the stale `getConfiguration()` javadoc

**File:** `src/main/java/com/ospreydcs/dp/client/AnnotationClient.java`

The current javadoc says a caller "cannot presently tell 'does not exist' from 'the service is
unreachable' without inspecting resultStatus.msg" and that surfacing the status is "tracked
separately". #240 landed `apiResultStatus` / `isReject()`, so this is now wrong. Replace that
paragraph with the `isReject()` guidance the new getters will use, so all three getters document the
same contract.

### Task 7 — Tests

**Existing file:** `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/PvMetadataClientIT.java` (276 lines, 7 tests)
**Existing file:** `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/ConfigurationClientIT.java` (656 lines, 19 tests)

Both files already exist and cover the save-path wrappers; extend them rather than creating new ones.
Note the source root is `src/test/integration/java` (failsafe), not `src/test/java`.

**Method-name collision — `PvMetadataClientIT` only.** It already has
`testBuildRequestOmitsUnsuppliedOptionalFields` and `testBuildRequestPopulatesSuppliedFields` for the
*save* request builder (`:53`, `:73`), so the new query-builder tests cannot reuse those names.
Follow the convention `ConfigurationClientIT` already uses for exactly this reason — it prefixes by
subject (`testBuildConfigurationRequest*` / `testBuildActivationRequest*` / `testBuildGetConfigurationRequest`)
— and name the new ones `testBuildQueryPvMetadataRequest*` / `testBuildGetPvMetadataRequest*`.
Renaming the two existing save tests to match is optional and out of scope here.

These test the **wrapper layer**, not the RPCs — `PvMetadataIT` (544 lines) and `ConfigurationIT`
(1223 lines) already cover the RPCs thoroughly, including both activation oneof branches and the
not-found REJECT messages. Do not duplicate that coverage.

Build-request tests (no server, following `testBuildRequestOmitsUnsuppliedOptionalFields` /
`testBuildRequestPopulatesSuppliedFields`):

- Unsupplied optional fields emit **no criterion** — assert `getCriteriaCount()`, and specifically
  that an empty `tagsAnyOf` does not emit an empty `TagsCriterion`.
- Each supplied field maps to the right criterion type with the right values.
- `TextMatch` populates exact/prefix/contains independently and in combination.
- `AttributeCriterion` with values, and with empty values (key-only).
- `limit <= 0` and blank `pageToken` left unset.
- `TimeRangeCriterion` emitted only when both bounds are supplied.
- Both activation get-request builders set the correct oneof arm.

Integration tests against the running service:

- Each query wrapper: success with results, and empty-result-is-success.
- Paging round-trip through the wrapper: first page returns a non-empty `nextPageToken`, feeding it
  back returns the remainder, and the final page's token is blank. Assert the blank final token —
  `testQueryConfigurationsPagination` in `ConfigurationIT` omits this check, so end-of-pagination is
  currently unverified for configurations.
- Both getters: success, and not-found asserting `isReject()` **and** `ApiResultStatus.REJECT` — not
  merely `isError()`, per the #235 lesson that naming and wire status can diverge silently.
- `getConfigurationActivation` by both id and composite key, each with a not-found case.
- A validation rejection surfaces as `ApiResultStatus.REJECT` (e.g. blank `pvNameOrAlias`).
- A backend failure surfaces as `ApiResultStatus.ERROR`, distinct from the REJECT case above —
  companion to `ConfigurationClientIT`'s existing `testApiResultStatusRejectOn*` tests, and the
  client-layer guard for the #235 lesson that the two must not be conflated.
- **Empty criteria is rejected today** (`"criteria list must not be empty"`), surfacing as
  `ApiResultStatus.REJECT`. Pin it for each of the three query wrappers. #245 flips this to
  match-all, so these assertions are the signal that it landed — expect to update them, and treat a
  silent pass after #245 merges as a bug in that ticket rather than in this one.

The new client wrappers return `nextPageToken` directly, so these tests need none of the raw-stub
workaround that `ConfigurationIT.java:387-390` apologizes for.

## Out of scope

- Streaming wrappers — no consumer, and the query RPCs here are unary only.
- `deletePvMetadata` / `deleteConfiguration` / `deleteConfigurationActivation` wrappers — not
  requested by either consumer.
- Changing the silent-pageToken-reset behavior — documented here, a separate ticket if it should
  change.
- The unset-`limit` semantic for `queryPvMetadata` — decided and fixed in #245 (its D1 applies the
  default of 100 unconditionally). #210, which originally owned this question, is closed without
  deciding it, so do not defer to it.
- The empty-criteria relaxation itself — that is #245, extended per finding 3.

## Dependencies and sequencing

- **#243 does not block on #245.** The wrappers pass criteria through unchanged, so they compile and
  test against today's server. Only the desktop app's browse-all explore views need #245.
- **#245 is a source-of-truth for two things this plan asserts**, and its plan lives on branch
  `issue-245-empty-criteria-match-all`, not on `main`. Whichever ticket lands second must reconcile:
  (a) the empty-criteria rejection tests added under Task 7 flip to match-all assertions, and (b) the
  `limit` javadoc must remain accurate — it is worded to be true under both, so it should need no
  change, and if it does, that is the signal it was worded too specifically.
- **#235 has landed** (merge `e2ea5fd`), so `isReject()` is reliable on these paths.
- dp-desktop-app#39 task 1 covers #243, #244, and #245 together; #244 (`QueryClient.querySamples`) is
  independent of this ticket.
