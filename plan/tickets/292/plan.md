# dp-service #292: recover a schema migration claim whose holder has gone away

## Overview

A process killed while it holds the schema migration claim leaves the claim held forever: every
later startup waits five minutes, exits, and repeats until an operator clears the marker by hand.
This ticket puts a heartbeat lease on the claim, so a waiting process takes over a claim whose
holder has stopped, and resumes from the recorded version.

Three things bound the takeover, and they are as much the deliverable as the takeover itself:

- A migration that **fails** keeps its claim and waits for an operator, as today.
- A takeover does not start while an operation of a previous claim is **still running on the
  server**.
- A claim is taken over at most **three times per schema version**; after that an operator must
  look.

For operators of any deployment where a migrating process can be killed (a Kubernetes probe, a node
drain, an OOM kill, a host reboot). Reported from the SLAC 1.16.0 upgrade (2026-09-22).

#293 (bind the port early, serve health, initialize in the background) lands in the same release
and is what stops the probe kills in the first place. This ticket is its prerequisite.

## Background: triage findings

The ticket was AI-drafted. Triage verified its premises against the code and three of its design
assumptions against MongoDB 8.0.6 (standalone, a scratch database, since dropped). The issue
description has been updated to match this section.

### Premises that hold

- **Nothing recovers a claim.** `SchemaVersionMarker.claimForMigration()` sets `migrating`,
  `migratingSince` and `migratingHost` (`SchemaVersionMarker.java:178-198`); only `releaseClaim()`
  clears them (`:264`), and `SchemaMigrationRunner.migrateOrWait()` calls it only after
  `applyPending()` returns (`SchemaMigrationRunner.java:288-290`). A waiter polls to one deadline
  and throws (`:298`).
- **A graceful stop leaves the claim too.** The shutdown hook is registered after `initService_()`
  returns (`GrpcServerBase.java:163`), so SIGTERM during a migration ends the JVM with the claim
  held, exactly as SIGKILL does.
- **Keeping the claim after a failure is deliberate** (`applyPending()` Javadoc,
  `SchemaMigrationRunner.java:352-356`).
- **Resuming from the recorded version is safe.** `recordApplied()` advances the version after each
  migration, and the runbook's manual clear already resumes that way.
- **No schema version change.** The new marker fields are additive; `SCHEMA_VERSION` stays 5 and no
  migration ships with this ticket.

### 1. A killed migrator's operations run to completion on the server (measured)

A slow `updateMany` and a slow `$group`/`$merge` aggregate were started from two clients, and both
clients were then killed with `kill -9`. Both operations stayed in `$currentOp` and finished: 600 of
600 documents updated, the `$merge` output written. The ticket listed this under "at minimum,
document it".

It needs more than documentation. With automatic takeover, each kill adds a full scan beside the
orphaned one. Under the pattern that produced the report (a probe killing the pod every few
minutes), that is one more concurrent scan of a 1.34-billion-bucket archive per kill. Today's stuck
claim is the only thing that prevents the pile-up. See D5.

Not measured: a server on which the operation is not in a JavaScript `sleep` (the fixture used one
to make the operations slow), and a sharded cluster. Task 9 covers the second.

### 2. The service user can see its own orphaned operations without a new privilege (measured)

As a user holding only `readWrite` on the database, `$currentOp` with `allUsers: false` returned
the orphaned `updateMany` of a killed client, with its `command.comment` and `ns`.
`allUsers: true` failed with `Unauthorized`. Every dp-service process connects as the same user, so
the own-operations form is sufficient and needs no `inprog` grant.

### 3. A server-time staleness filter treats a missing heartbeat as stale (measured)

The ticket preferred comparing the heartbeat with `$$NOW` in the query.
`{$expr: {$lt: ["$migratingHeartbeat", {$subtract: ["$$NOW", n]}]}}` matched a marker that had no
`migratingHeartbeat` field, because a missing value sorts below any date. That is the "take over a
live 1.16.0 migrator" case the ticket itself warns about, produced by its preferred approach unless
the filter also requires the field. D2 avoids any clock comparison instead.

The other mechanics worked as the ticket proposed: a compare-and-swap takeover filtered on the
observed heartbeat value let exactly one of two attempts win, and a write filtered on a displaced
token matched 0.

### 4. A failure flag cannot cover a failure that ends the process

The ticket has a failed migration write `migrationFailed` before its heartbeat stops. The image
sets `-XX:+ExitOnOutOfMemoryError`, and `applyPending()` catches only `DpException` and
`RuntimeException` (`SchemaMigrationRunner.java:371,378`). A migration that deterministically runs
the process out of memory writes no flag, is indistinguishable from a kill, and would be taken over
and re-run without end. See D4.

### 5. #292 alone does not end the reported incident

The process that takes over is killed by the same liveness probe. v4's `updateMany` makes progress
across attempts; v5's `$group`/`$merge` starts over each time. #293 is the fix for the kills. With
only #292, the takeover cap (D4) is reached and an operator clears the claim, which is no worse
than today. (#293's background cites a 35M-bucket archive; the figure is 1.34 billion.)

## Design decisions

### D1. Marker fields

Added while a claim is held, all removed by `releaseClaim()`:

| Field | Type | Meaning |
|---|---|---|
| `claimToken` | string (random UUID) | Identifies one claim or takeover. Every marker write by the holder filters on it. |
| `heartbeatSeq` | int64 | Incremented by each heartbeat. This is what a waiter watches. |
| `migratingHeartbeat` | date | Server time of the last heartbeat (`$currentDate`). For operators only; no code compares it. |
| `takeoverCount` | int32 | Takeovers since the version last advanced. |

Written by a failed migration and removed only by the operator's manual clear:

| Field | Type | Meaning |
|---|---|---|
| `migrationFailed` | bool | The holder's migration threw. Never taken over. |
| `migrationFailedVersion` | int32 | The migration that failed. |
| `migrationFailedMessage` | string | Its message, through `SecretRedactor`, truncated to 1000 characters. |

`migrating`, `migratingSince` and `migratingHost` keep their meaning; a takeover rewrites the last
two. `MarkerState` grows the new fields.

### D2. Staleness is what the waiter observes, not a timestamp comparison

A waiter records the `(claimToken, heartbeatSeq)` pair it reads and the time on its own monotonic
clock (`System.nanoTime()`). When a later poll reads a different pair it starts again. When the
same pair has been read for `staleAfter`, the claim is stale.

- No clock is compared across hosts, so skew cannot cause or prevent a takeover.
- A claim with no `claimToken` (a 1.16.0 holder) has nothing to watch and is never stale: the
  waiter behaves as it does today.
- The cost: a waiter always waits one full `staleAfter` (60 s) before taking over, even from a
  holder that died days ago.

*Rejected:* `$$NOW` in the heartbeat and in the takeover filter (the ticket's preference). It needs
a guard for the missing field (finding 3) and makes correctness depend on the filter being written
exactly right, for no benefit over observation.

### D3. Takeover is a compare-and-swap on the observed pair

`SchemaVersionMarker.takeOverClaim()` is one `findOneAndUpdate`:

- filter: `_id`, `migrating: true`, `claimToken` and `heartbeatSeq` equal to the observed pair,
  `migrationFailed` not true, `takeoverCount` below the cap (missing counts as 0);
- update: a new `claimToken`, `heartbeatSeq: 0`, `migratingHeartbeat` by `$currentDate`,
  `migratingHost` and `migratingSince` for the new holder, `$inc takeoverCount`.

A null result is an ordinary refused claim: another waiter won, or the holder was alive after all
and its heartbeat moved. The winner resumes from the marker's recorded `version`, read from the
returned document, not from an earlier read.

### D4. A claim is taken over at most three times per version

`MAX_TAKEOVERS_PER_VERSION = 3`, a constant. `takeoverCount` starts at 0 on a fresh claim, is
incremented by each takeover, and is reset to 0 by `recordApplied()`, since an advanced version is
progress. A waiter that finds a stale claim at the cap throws at once, with a message that says the
migration from version N was abandoned by four processes in a row and points at the runbook. The
manual clear resets the count.

This is what stops the loop of finding 4, and it bounds the cost of anything else that kills
migrators repeatedly.

- *Rejected: no cap.* A migration that kills its process every time is retried forever, each retry
  a full scan.
- *Rejected: a configuration key.* The recourse at the cap is an operator reading the log, and a
  deployment that needs a larger number has a problem the number does not fix.
- *Accepted cost:* a deployment whose migrators are killed by a probe reaches the cap and needs the
  manual clear, as it does today, until #293 is deployed.

**`Error`s are not flagged; the cap covers them.** `migrationFailed` is written for a `DpException`
or `RuntimeException` out of `Migration.apply()`. An `Error` propagates with no flag: a fatal one
usually ends the process before any catch could write, and one rule is easier to hold than a
classification of `Error`s in the runner.

### D5. A takeover waits for the previous claim's operations to end

Every operation a migration issues carries the comment `dp-schema-migration:<claimToken>` (D6).
Before attempting a takeover, the waiter runs, on the `admin` database,
`$currentOp {allUsers: false}` matched on a `command.comment` beginning `dp-schema-migration:` and
an `ns` in this database. While that returns anything, the waiter does not take over; it keeps
polling against its one deadline. The timeout message for this case names the orphaned operation,
its running time and the `killOp` an operator can use.

- **If `$currentOp` fails, the takeover is refused**, and the log says why. The process then waits
  and times out as it does today. Proceeding without the check would restore the pile-up on exactly
  the deployments where nobody can see it.
- **The check and the takeover are not atomic.** A holder that was stalled, not dead, can issue a
  new operation after the check. Every migration tolerates that (D8); the check removes the
  repeated case, not every case.
- **Only a takeover checks.** An ordinary claim follows a release, which follows the holder's
  operations returning. The one other route to an ordinary claim is the operator's manual clear, and
  the runbook gains the same `$currentOp` query as its first step.

*Rejected: `killOp` on the orphan.* v4's orphaned `updateMany` is doing useful work that the
takeover would only repeat, and `killOp` on another connection's operation needs a privilege the
service user does not have.

*Rejected: document it and rely on the cap.* Four concurrent full scans of `buckets` is still an
outage for the query service.

### D6. Migrations receive the claim's comment through a context argument

`Migration.apply(MongoDatabase)` becomes `apply(MongoDatabase, MigrationContext)`, where
`MigrationContext` is a record carrying `operationComment()`. Each migration passes it to every
`find`, `updateOne`, `updateMany` and `aggregate` it issues (`.comment(String)` on the iterable or
options class; driver 5.4.0). Index and collection drops take no comment in this driver and
finish at once.

A new migration that forgets the comment would silently fall outside D5, so a test pins it (Task
8): each shipped migration runs against a client with a `CommandListener`, and every `find`,
`update` and `aggregate` command it starts must carry the comment.

*Rejected: a default `apply` overload so existing migrations need no change.* The point is that no
migration may omit it; an overload that makes omission the default defeats that.

### D7. Every marker write by the holder filters on `claimToken`

`claimForMigration()` and `takeOverClaim()` return a `Claim` (the token and the version claimed
from). `recordApplied()`, `releaseClaim()`, the heartbeat and `markFailed()` take it and add
`claimToken` to their filters; all already test, or will test, `matchedCount`.

A write that matches nothing re-reads the marker to say which of two things happened:

- marker absent: today's "the marker document is gone" error, unchanged;
- marker present under another token: `ClaimLostException` (a `DpException` subclass). The holder
  was displaced. It stops, writes no failure flag (it cannot: that write is fenced too), and the
  process exits with a message that says another process took the migration over. It must not
  reuse the "claim has been left in place" text.

Fencing covers the marker only. A displaced holder's data operation continues until its next
marker write; see D8.

### D8. Migrations must be safe to run late and concurrently, not only idempotent

After a takeover, the displaced holder's operation and the new holder's can run at once, and a
stalled holder's migration N can finish after the new holder has applied N+1 and services have
started. The `Migration` Javadoc and the runbook's developer section state the stronger rule. The
five shipped migrations meet it, and Task 7 records why for each in its Javadoc:

- v1 `$rename` filtered on the old field; v4 `updateMany` filtered on the absent key: a late run
  matches nothing new.
- v2, v3: per-document rewrites to a canonical form the save path already produces.
- v5: `$merge` with `$max`, monotone against concurrent ingestion by design.

### D9. The heartbeat is a daemon thread that outlives nothing

`ClaimHeartbeat` starts when a claim or takeover is won and is stopped in a `finally` around
`applyPending()` and the release.

- **The `finally` is required.** If the migrating thread ends with an `Error` while some non-daemon
  thread keeps the JVM alive, a heartbeat stopped only on the normal paths would keep a dead claim
  fresh indefinitely. (Not verified whether such a thread exists during init; the `finally` makes
  it not matter.)
- **Each beat is under its own `catch (Throwable)`**, logged, and the loop continues: a heartbeat
  thread that dies silently makes a live migrator look dead.
- **A fenced beat** (matched 0, marker present under another token) logs at error, sets a
  `claimLost` flag the runner checks before each migration, and stops beating. It does not
  interrupt the running operation: per finding 1 the server-side operation would continue anyway.
- On failure the order is: write `migrationFailed`, then stop the heartbeat.
- The beat is `$inc heartbeatSeq`, `$currentDate migratingHeartbeat`, filtered on the token, on the
  runner's `MongoDatabase` handle. A beat delayed past `staleAfter` (a majority write that cannot
  be acknowledged) can lead to a takeover from a live holder; D7 and D8 make that safe.

### D10. Mixed versions

- A 1.16.0 holder writes no token and is never taken over (D2). A stuck 1.16.0 claim still needs
  the manual clear.
- A 1.16.0 waiter beside a new holder waits and times out as it always did.
- **Accepted limitation:** fields left on the marker by a manual clear that followed the *old*
  runbook text, followed by a 1.16.0 process claiming, would show a new waiter a token whose
  sequence never moves. It requires a rollback to 1.16.0 in the middle of a pre-v5 migration
  started by a newer build, and its consequence is a concurrent run of idempotent migrations. The
  runbook's clear is updated to unset every field; no code guards this.

### D11. The claim wait keeps its one deadline

`migrateOrWait()` remains a single loop with one absolute deadline (CLAUDE.md, "The claim wait is
bounded by one deadline"). Staleness observation, the orphan check and takeover attempts all happen
inside it. A waiter that wins a takeover stops being bound by the deadline, like any claimant.
Whether a waiter should wait indefinitely for a live holder is #293's decision.

### D12. Configuration

Two keys under `MongoClient`, in both `application.yml` files:

| Key | Default | Environment |
|---|---|---|
| `schemaMigrationHeartbeatIntervalSeconds` | 10 | `DP_MONGO_SCHEMA_MIGRATION_HEARTBEAT_INTERVAL_SECONDS` |
| `schemaMigrationStaleAfterSeconds` | 60 | `DP_MONGO_SCHEMA_MIGRATION_STALE_AFTER_SECONDS` |

The runner validates them when a migration is pending and throws `DpException` if the interval is
below 1, if `staleAfter` is below three intervals (one slow beat must not read as a dead holder),
or if `staleAfter` is not below the claim wait timeout (a takeover could then never happen, which
would disable the feature silently). The tests pass millisecond values through the existing
protected constructor.

## Implementation tasks

### Task 1. `SchemaVersionMarker`

`common/mongo/migration/SchemaVersionMarker.java`

- Field constants for D1; extend `MarkerState` and `read()`.
- `record Claim(String token, int fromVersion)`.
- `claimForMigration()` returns `Claim` or null; also sets `claimToken`, `heartbeatSeq: 0`,
  `migratingHeartbeat`, `takeoverCount: 0`, and unsets nothing else.
- `takeOverClaim(database, observedToken, observedSeq, host, maxTakeovers)` per D3.
- `heartbeat(database, claim)` returns whether it matched.
- `recordApplied(database, claim, version, description)`: token filter, `takeoverCount: 0`.
- `releaseClaim(database, claim)`: token filter; unsets every D1 claim field.
- `markFailed(database, claim, version, message)`: token filter; redact and truncate.
- A private helper that turns `matchedCount == 0` into "marker gone" or `ClaimLostException` (D7).
- `findOrphanedOperations(database)` per D5, returning comment, `ns`, `opid` and running seconds;
  `MongoException` becomes `DpException`.

### Task 2. `ClaimLostException`

`common/mongo/migration/ClaimLostException.java`, extends `DpException`.

### Task 3. `ClaimHeartbeat`

`common/mongo/migration/ClaimHeartbeat.java`: `start()`, `stop()` (idempotent, joins briefly),
`isClaimLost()`, per D9.

### Task 4. `MigrationContext` and the `Migration` signature

`common/mongo/migration/MigrationContext.java`, `Migration.java` (signature, and the D8 rule in the
Javadoc).

### Task 5. `SchemaMigrationRunner`

- Read and validate the two keys (D12); constructor seam gains heartbeat interval and stale-after
  in milliseconds.
- `migrateOrWait()`: on a refused claim, read the marker and, in order:
  1. `migrationFailed`: throw at once, naming the failed version and stored message;
  2. update the observed pair (D2);
  3. if stale and at the cap: throw (D4);
  4. if stale: orphan check (D5), then `takeOverClaim()`;
  5. otherwise the existing deadline, sleep and re-read.
- A won claim or takeover runs through one method: start heartbeat, `applyPending(claim)`, release,
  with the heartbeat stopped in `finally`, and `markFailed()` in the catch for a migration failure
  only (not for `ClaimLostException`, not for a marker-write failure).
- `applyPending()` checks `heartbeat.isClaimLost()` before each migration and passes the context.
- `describeHolder()` adds the last heartbeat time and takeover count; timeout messages distinguish
  a live holder, an orphaned operation, and a legacy claim with no heartbeat.

### Task 6. Configuration files

`src/main/resources/application.yml` and `src/test/resources/application.yml`: both keys, with
comments in the main file.

### Task 7. Migrations

`V1`–`V5`: new signature, the comment on every `find`/`updateOne`/`updateMany`/`aggregate`, and a
Javadoc sentence on late and concurrent runs (D8). Update the existing per-migration tests for the
signature.

### Task 8. Tests

`SchemaMigrationRunnerTest` and a new `SchemaVersionMarkerLeaseTest`, against `dp-test`, with
shortened intervals:

- A claim whose heartbeat stops and has no failure flag is taken over; migration resumes from the
  recorded version; `takeoverCount` is 1 and returns to 0 after the next `recordApplied`.
- A claim with a moving heartbeat is not taken over; the waiter returns when the holder releases.
- A claim marked `migrationFailed` is never taken over, and the waiter throws without waiting for
  the deadline, naming the failed version.
- A claim with no `claimToken` (1.16.0 shape) is not taken over; the old timeout applies.
- A stale claim at the cap is not taken over; the waiter throws at once.
- A stale claim with a running operation carrying the migration comment is not taken over until
  that operation ends (a slow commented `updateMany` on a scratch collection in `dp-test`).
- A displaced holder's `recordApplied()`, `releaseClaim()`, `heartbeat()` and `markFailed()` leave
  the marker unchanged and report `ClaimLostException`; with the marker deleted they report "gone".
- Two waiters racing a takeover: exactly one wins.
- A failing migration leaves `migrationFailed`, version and message set, with the claim held; a
  secret-bearing URI in the message is redacted.
- The heartbeat stops when a migration throws an `Error` (a test migration throwing
  `StackOverflowError`): `heartbeatSeq` does not move afterwards, and no failure flag is written.
- A heartbeat whose write throws keeps beating (Mockito collection delegating to the real one, as
  `PvStatsMaxSpanUpdaterTest` does).
- Configuration validation: each of the three rejections in D12.
- **Comment coverage (D6):** every shipped migration, run through a `CommandListener` client,
  issues no `find`, `update` or `aggregate` without the comment.

Each new test is run once with the behavior it pins reverted, to confirm it fails.

### Task 9. Sharded verification of the orphan check

Against a throwaway sharded cluster (one mongos, one shard is enough): confirm that
`$currentOp {allUsers: false}` through mongos, as a `readWrite` user, reports an orphaned
`updateMany` and `$merge` aggregate with the comment and an `ns` the D5 match accepts. If the shape
differs (shard-prefixed `opid`, a different `ns`, or the own-user form unavailable through mongos),
adjust the match in Task 1 and record the result in CLAUDE.md. If own-user `$currentOp` cannot work
through mongos at all, stop and raise it: D5's fail-closed rule would then disable takeover on the
primary deployment.

### Task 10. Documentation

- `doc/runbooks/schema-migration.md`: the lease, the new fields in "Reading the current state", the
  startup table, the three outcomes of a held claim (live, vanished, failed), the cap, the orphan
  check and its `$currentOp`/`killOp` commands, the manual clear (all fields; check for orphans
  first), the v4/v5 notes, the Configuration table, and the D8 rule in "Adding a migration".
- `doc/running.md`: the two environment variables.
- `CLAUDE.md` § Schema Migration: every marker write filters on the claim token; staleness is
  observed, never compared; takeover waits for orphaned operations; the cap; the heartbeat's
  `finally`; migrations carry the comment and must be safe late and concurrently.
- `doc/release-notes/NEXT.md`: a section for #292.

## Out of scope

- Binding the port early, health service, the `UNAVAILABLE` gate, and whether a waiter waits
  indefinitely for a live holder: #293.
- A standalone migration entry point (a Kubernetes Job or Helm hook): not planned.
- Recovering a stuck claim left by a 1.16.0 process: remains the manual clear (D10).
- Topology misdetection through mongos, which decides the write concern the heartbeat uses: #291.
- Making v5 resumable across takeovers: no ticket; raise one only if #293 does not remove the need.

## Dependencies and sequencing

- **#293 depends on this ticket**, not the reverse: its indefinite wait relies on a vanished holder
  being detected. Both ship in the same release.
- **No dp-grpc change**, and no schema migration.
- **Task 9 does not block Tasks 1–8**, but it must finish before merge: its result can change the
  `$currentOp` match and, in the worst case, the D5 decision.
- Task 4 precedes Task 7; Task 1 precedes Tasks 3 and 5.
- Upgrade order is unchanged: stop every service before a schema upgrade. The lease coordinates
  starting processes, not running ones.
