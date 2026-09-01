# Plan: remove `DataValue.ValueStatus` usage (issue #252)

- **Ticket**: [osprey-dcs/dp-service#252](https://github.com/osprey-dcs/dp-service/issues/252)
- **Upstream proto change**: [osprey-dcs/dp-grpc#143](https://github.com/osprey-dcs/dp-grpc/issues/143),
  merged as [dp-grpc#146](https://github.com/osprey-dcs/dp-grpc/pull/146) on 2026-08-28
- **Epic**: osprey-dcs/data-platform#83
- **Sibling**: #248 — the dp-grpc #132 datasets/annotations modernization, the other half of the
  same breaking dp-grpc release. Deliberately not folded in; see [Scope boundary](#scope-boundary).

## Overview

`DataValue.ValueStatus` carried acquisition-time EPICS-style severity, status code, and message
per sample, embedded inside the value. dp-grpc #143 removed it; the Sample Status API (#238) is
the designated replacement and is already implemented here.

This ticket deletes dp-service's remaining references. It is a removal, not a migration: no
server path ever read `valueStatus` — it rode opaquely inside serialized `DataValue`s — so there
is no behavior to preserve and no stored data to convert.

## Background / triage findings

Verified against `1e21379` (dp-service `main`) with dp-grpc `main` (`0cecdea`) installed locally.

### `main` does not currently compile

CI pins `DP_GRPC_REF: 'main'` (`.github/workflows/ci.yml:20`), so every open PR fails at the
`build-and-test` compile step regardless of its own content. Reproduced by compiling unmodified
`origin/main` against dp-grpc `main`:

```
IngestionClient.java:[118,35]            cannot find symbol   <- this ticket
IngestionClient.java:[269,36]            cannot find symbol   <- this ticket
AnnotationClient.java:[243,70]           cannot find symbol   <- #248
AnnotationClient.java:[268,63]           cannot find symbol   <- #248
QueryAnnotationsApiResult.java:[10,65]   cannot find symbol   <- #248
QueryAnnotationsApiResult.java:[22,85]   cannot find symbol   <- #248
AnnotationDocument.java:[115,54]         cannot find symbol   <- #248
```

`javac` halts after the main-source phase, so the test-source references are absent from that
output but break `test-compile` once main compiles. **This ticket alone therefore does not turn
CI green** — it removes its half of the break. That is stated plainly here so the PR is not
mistaken for a CI fix that failed to work.

### The two halves of the dp-grpc release are not comparable in size

Worth recording because the natural instinct on a red CI is to fix everything at once:

| | This ticket (#143) | #248 (#132) |
|---|---|---|
| Production files | 1 | 4+ |
| Open design questions | 0 | 4 |
| New RPCs | 0 | 5 (+2 deferred stubs) |
| Supersedes | — | #210, #211, #214 |
| Migration | none | annotations text index rebuild |

#132's dp-service handoff document runs 13 sections and explicitly leaves page-size values,
token scope, rejection-message detail, and orphaned-calculations cleanup undecided. #143's
blast radius is fully enumerated upstream and settled.

### `Annotation` was moved, not deleted — a correction worth recording

Initial triage of the CI failure described the annotation half as "`Annotation` was removed like
`ValueStatus`". That is wrong, and the distinction matters for whoever picks up #248:
`QueryAnnotationsResponse.AnnotationsResult.Annotation` was **hoisted to a top-level
`Annotation`** message by #132 and simultaneously reshaped — `comment` → `description`, the
embedded `repeated DataSet dataSets` dropped, and `calculationsId` / `createdTime` /
`updatedTime` / `modifiedBy` added, with field numbers reassigned. It is not a requalification
sweep. Only `ValueStatus` was genuinely deleted.

### Upstream's reference counts undercount, and its file list is incomplete

dp-grpc's handoff §12 lists four test files with 15 / 10 / 6 / 1 references. Two corrections
found while implementing, recorded so the next reader does not re-derive them:

- `IngestionTestBase` has **11** references, not 6. The extra five are in a hand-written
  `equals` / `hashCode` / `toString` triple and two dimension assertions inside
  `buildIngestionRequest()` — easy to miss by grepping for the type name alone, since these
  mention only the lowercase field.
- The four named files are only the *direct* references. Removing the parameter from
  `IngestionTestBase`'s sole constructor breaks **62 call sites across 26 more files** (Task
  3b). This is the bulk of the diff and was not scoped upstream.

The `grep -rn "ValueStatus"` acceptance check would have caught neither: the first is a
lowercase-only reference, and the second produces an arity error rather than a symbol error.

### `IngestionClient` already has a `valuesStatus`-free constructor

`IngestionClient.IngestionRequestParams` declares three constructors (`:130`, `:223`, `:255`).
The 13-arg one at `:223` delegates to the 14-arg one at `:255`, passing `null` for
`valuesStatus`. Removing the 14-arg overload makes the 13-arg signature the full constructor, so
**9 of the 10 call sites in the tree need no edit**; only `IngestionClientTest:51` passes the
parameter.

## Design decisions

**D1 — Remove the constructor parameter rather than retaining a deprecated overload.**
Dropping `valuesStatus` from the 14-arg `IngestionRequestParams` constructor is a source-breaking
change to a public client class. Accepted per dp-grpc #143 D1 and #132 D1: the client API has no
production consumers outside this repo and the demo app, and the same release breaks far more
than this. Retaining an ignored parameter would be worse than breaking — a caller could keep
passing status data that is now silently discarded, which is the wrong-answer-not-an-error
failure mode this repo treats as the serious one.

**D2 — Delete `QueryDataValueStatusIT` outright rather than adapting it.**
Its entire subject is the ingest → store → query round-trip of `ValueStatus` content; every
assertion is about status values surviving that trip. With the field gone there is no reduced
version that tests anything the general ingestion ITs do not already cover. Adapting it would
leave a test named for a mechanism that no longer exists. The equivalent coverage for the
replacement mechanism lives in the sample-status ITs from #238.

**D3 — Drop the size-consistency validation with the field.**
`IngestionRequestParams`'s constructor validates `valuesStatus` dimensions against `values`
(`:287-305`), with a comment explaining it converts a would-be `IndexOutOfBoundsException` from
deep inside `buildIngestionRequest()` into an argument error at the call site. That rationale
dies with the indexing code it protected; the block is removed rather than generalized.

**D4 — No `reserved`-style placeholder in the BSON documents.**
Nothing was ever persisted. `valueStatus` lived inside the serialized `DataValue` protobuf, and
archived blobs containing field 15 still parse under the new schema as an unknown field. dp-grpc
#143 D2 reserves both the field number and the name upstream, which is where that protection
belongs; dp-service needs no counterpart.

## Implementation tasks

### Task 1 — `src/main/java/com/ospreydcs/dp/client/IngestionClient.java`

The only production reference.

- remove the `valuesStatus` field from `IngestionRequestParams` (`:118`)
- delete the 14-arg constructor (`:255-306`), promoting the 13-arg one at `:223` to be the full
  constructor: inline its body rather than leaving it delegating to a removed overload
- remove the `setValueStatus` block in `buildIngestionRequest()` (`:600-603`)
- drop the now-unused `DataValue` import if nothing else in the file needs it

### Task 2 — `src/test/integration/java/.../integration/query/QueryDataValueStatusIT.java`

Delete the file (179 lines, 15 references). Per D2.

### Task 3 — `src/test/java/.../service/ingest/IngestionTestBase.java`

- remove the `valuesStatus` params field (`:61`), its accessor (`:158-160`), and the constructor
  parameter and assignment (`:96`, `:110`)
- remove the `setValueStatus` call in `buildIngestionRequest()` (`:531-532`)
- update the field-group comment at `:58` that names `ValueStatus`
- remove `valuesStatus` from the hand-written `equals`, `hashCode`, and `toString` (`:295`,
  `:315`, `:335`), and the two dimension assertions in `buildIngestionRequest()` (`:470-471`,
  `:477-478`)

### Task 3b — the 62 call sites of `IngestionTestBase.IngestionRequestParams` (not in the
upstream triage)

`IngestionTestBase.IngestionRequestParams` has a **single** constructor, unlike
`IngestionClient`'s three, so removing its parameter breaks every call site: **62 calls across 26
files**, spread over `integration/ingest/`, `integration/v2api/`, and four unit-test classes.
Each drops one positional `null` argument; only `MongoIngestionHandlerTestBase:328` needed
hand-editing, because a trailing comment on the preceding argument defeated automated
splitting. No call site passed a non-null `valuesStatus`.

### Task 4 — `src/test/java/com/ospreydcs/dp/client/IngestionClientTest.java`

- remove the `valuesStatus` parameter from the shared `buildParams` helper (`:48`, `:51`)
- delete the `ValueStatus`-specific test cases (around `:82-140`, `:322`)
- keep the assertion that a built `DataValue` carries no status only if it can be expressed
  without the removed API; otherwise drop it with its test

### Task 5 — `src/test/java/.../service/query/QueryTestBase.java`

Comment only (`:898`) — reword to drop the `ValueStatus` mention.

### Task 6 — verification

- `grep -rn "ValueStatus" src/` returns nothing
- `mvn clean test-compile` against dp-grpc `main` leaves only the #248 failures listed above
- ingestion unit and integration tests pass unchanged apart from the files above

## Scope boundary

Everything belonging to dp-grpc #132 is **out of scope** and owned by #248: the `Annotation`
hoist and reshape, `comment` → `description`, the dropped `dataSets` / `calculations`
embedding, the five new RPCs, paging and opaque tokens, AND-combining criteria, typed
calculations columns, and the annotations text-index rebuild.

Sample Status API changes are out of scope; `ValueStatus`'s replacement shipped under #238.

## Dependencies and sequencing

Depends on dp-grpc `main` (`0cecdea` or later) being what dp-service builds against — already
true in CI, which pins `DP_GRPC_REF: 'main'`.

Does **not** depend on #248, and #248 does not depend on this: the two touch disjoint files.
They can land in either order and be reviewed independently. `main` compiles only once both
have landed.
