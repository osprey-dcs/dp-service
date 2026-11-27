# dp-service #293: bind early, serve gRPC health, and initialize in the background

## Overview

Today a service binds its gRPC port only after initialization, and initialization includes the
schema migrations. On a large archive that is hours with nothing listening, and a Kubernetes probe
kills the pod. This ticket makes every service:

- bind its gRPC port immediately and serve the standard gRPC health service (`grpc.health.v1`);
- run initialization, migrations included, on a background thread;
- report `NOT_SERVING` and reject every other RPC with gRPC status `UNAVAILABLE`, saying why,
  until initialization succeeds;
- exit non-zero if initialization fails, as startup does today;
- wait for a migration another process is running for as long as that migration shows signs of
  life, where today it exits after five minutes and relies on being restarted.

For operators of any deployment that probes its services, and for clients, which get a retryable
status with a reason where they got a refused connection. Reported from the SLAC 1.16.0 upgrade
(2026-09-22). Part of osprey-dcs/data-platform#103, with #292 (merged) and #331 (follow-on).

## Background: triage findings

The ticket was AI-drafted. Triage read its premises against the code at `109234d`. Nothing in this
section was measured against a running system; the two things that need measuring are tasks
(Task 8). The issue description has been updated to match this section.

### Premises that hold

- **The gRPC port is bound after initialization.** `GrpcServerBase.start()` calls
  `DpTelemetry.init()`, then `initService_()`, then builds and starts the server
  (`GrpcServerBase.java:94-150`). `initService_()` reaches `MongoClientBase.init()`, which runs
  the migrations and then creates indexes. The metrics port is bound first and answers throughout.
- **No health service exists.** Nothing in `src/main` references `HealthStatusManager` or
  `grpc.health`.
- **`grpc-services` is on the classpath** at 1.65.1, through dp-grpc's `grpc-all` (`mvn
  dependency:tree`). The jar carries two `HealthStatusManager` classes;
  `io.grpc.services.HealthStatusManager` is the deprecated one.
- **The shutdown hook is registered last** (`GrpcServerBase.java:163`), so a SIGTERM during a
  migration ends the JVM with no cleanup.
- **A waiting service exits after five minutes.** `migrateOrWait()` computes one deadline at entry
  (`SchemaMigrationRunner.java:419`) and throws at it (`:569`).
  `validateLeaseTimings()` requires `staleAfter` below that wait (`:387`).
- **#292 is merged** (PR #329). The runner and marker code this ticket changes is final.

### 1. A gRPC liveness probe would recreate the incident

The ticket recommended a gRPC liveness probe and said it "passes as soon as the port is bound". A
Kubernetes gRPC probe calls `grpc.health.v1.Health/Check` and succeeds only when the response is
`SERVING`. Against a service that reports `NOT_SERVING` for the whole migration, the kubelet kills
the migrating pod: the incident this ticket exists to end, reintroduced by its own guidance.

This is Kubernetes' documented behaviour, not something measured here; Task 8 measures it. See D3.

### 2. Readiness alone does not stop the old pod

Raised by SLAC in the issue (comment of 2026-11-27): under a `RollingUpdate`, Kubernetes keeps the
old pod until the new one is ready, so the old pod serves against a database being migrated. With
readiness passing only on `SERVING` that is as true after this ticket as before it. Making
readiness pass during the migration does not fix it either: the new pod claims the migration at
startup, while the old one is still running and draining.

Only the rollout strategy gives the ordering "old instances stop, then the migration begins". The
ticket did not mention rollout strategy. See D9 and #331.

### 3. Three waits have no end the lease can supply, not two

The ticket named a claim with no token (a 1.16.0 holder) and a stale claim whose orphaned
operation is still running. The third: when the orphaned-operation check itself fails, the
takeover is refused for as long as it keeps failing (`SchemaMigrationRunner.java:524-534`).
Through mongos that is every deployment without the `inprog` grant, which is the shape of the
deployment that reported this. See D6.

### 4. Shutdown during initialization fails today

Each `serviceImpl.init(handler)` assigns `this.handler` before it calls `handler.init()`
(`QueryServiceImpl.java:28-30`, and the same in the other three). `QueueHandlerBase.init()`
creates `executorService` only after `init_()`, which is where the migration runs, has returned
(`QueueHandlerBase.java:256-265`). `fini()` calls `executorService.shutdown()` unguarded (`:308`).
With the hook registered early, a SIGTERM during a migration would go `stopServer()` →
`finiService_()` → `serviceImpl.fini()` → `handler.fini()` and throw `NullPointerException` on
the hook thread, skipping `DpTelemetry.shutdown()`.

Nothing closes the Mongo client: `MongoClientBase.fini()` only logs. So a deliberate stop of a
running migration has no existing close to build on, and leaving it alone really does leave its
server-side operation running (#292 finding 1).

### 5. Three services migrate or wait; one does not

`IngestionStreamHandler.init_()` returns true and the handler has no Mongo client. That service's
initialization is immediate. It gets the health service and the gate like the others; D6 does not
apply to it. It is a gRPC client of the ingestion service, but only per subscription, not at
initialization, and a refused connection already reaches a gRPC client as `UNAVAILABLE`, so what it
sees from a migrating ingestion service does not change status.

### 6. Six `main()` methods

`BenchmarkIngestionGrpcServer` and `BenchmarkQueryGrpcServer` have their own
`start(); blockUntilShutdown();`, as the four services do. The failure exit must not be six
copies. See D5.

### 7. The ticket's startup test has no seam

"Startup with a migration held by a slow test migration" cannot be written as described:
`MongoSyncClient.runSchemaMigrations()` constructs the runner with the production migration list.
No test starts a server through `GrpcServerBase` today (the test `application.yml` says so). See
D10.

## Design decisions

### D1. `start()` binds first and initializes on a thread

`GrpcServerBase.start()` becomes:

1. `DpTelemetry.init()`. Still first, and still before `initService_()`: instruments bind lazily
   to whatever meter exists.
2. Register the initialization gauges (D8).
3. Build the server with the health service (D3) and the gated service (D2), and start it. A bind
   failure shuts telemetry down and throws, as today.
4. Register the shutdown hook.
5. Start the thread `dp-init-<service>`, non-daemon, which calls `initService_()`.

`start()` returns as soon as the thread is started. A lifecycle field records where the process
is: `INITIALIZING`, `READY`, `FAILED`, `STOPPING`. Every transition is made under one lock, and
the rules are:

- `INITIALIZING` → `READY`: the init thread, when `initService_()` returns true. It opens the
  gate, sets health to `SERVING`, and logs "service ready" with the elapsed time, in that order.
- `INITIALIZING` → `FAILED`: the init thread, on a false return or any `Throwable`. See D5.
- any → `STOPPING`: the shutdown hook.
- An init thread that finishes and finds `STOPPING` does not open the gate. It calls
  `finiService_()` itself, for whatever it built.

`finiService_()` is called by `stopServer()` only when the state it found was `READY`. In
`INITIALIZING` the handler is half-built and belongs to the init thread (finding 4), so the hook
leaves it alone; the JVM halts when the hooks finish, whatever the init thread is doing.
`blockUntilShutdown()` follows the same rule. `QueueHandlerBase.fini()` gets a null guard on
`executorService` regardless, so that the defect in finding 4 is not one refactoring away.

**Rejected:** initializing the handler before the bind and only the Mongo client afterwards. The
handlers build their clients inside `init_()`, in three different classes, and the split would be
a larger change for the same outcome.

### D2. The gate is a `ServerInterceptor` on the service, closed until `READY`

`ReadinessGate` (`common/grpc`) is applied with `ServerInterceptors.intercept(serviceImpl, gate)`;
the health service is added unwrapped. While closed it ends every call with
`Status.UNAVAILABLE` and a description, and returns a no-op listener, so no call reaches a service
implementation whose handler does not exist. Open, it is one volatile read per call. It stays
installed for the life of the process, which is what #331 needs.

`UNAVAILABLE`, not the repo's usual OK response carrying an `ExceptionalResult`: it is the status
clients and retry policies read as "retry later", and it applies to every method, the streaming
ones included, with no per-method response builder.

**The description says which state the process is in**, from a supplier read at each rejected
call:

| State | Description |
|---|---|
| initializing, no migration seen | `service is initializing` |
| this process is migrating | `service is initializing: applying schema migration version N (target M)` |
| waiting on another process | `service is initializing: waiting for a schema migration running in another process` |

The holder's `migratingHost` is not put in the description: it is a pid and a host name, and the
description goes to every client. It is in the log.

The state comes from `InitializationStatus` (D7).

### D3. Health: the standard names, and one that is always `SERVING`

`io.grpc.protobuf.services.HealthStatusManager`, one per server. `grpc-services` is declared in
`pom.xml` at `${io.grpc.version}`, as `grpc-opentelemetry` is, so the health service does not
depend on what `grpc-all` happens to contain.

| Health service name | Status |
|---|---|
| `""` (the server) | `NOT_SERVING` until `READY`, then `SERVING` |
| the service's full name, from its `ServiceDescriptor` | the same |
| `liveness` | `SERVING` from the bind onwards |

`stopServer()` calls `enterTerminalState()` first, so a draining pod leaves the endpoints.

The third name exists because of finding 1. A deployment that wants a gRPC liveness probe names it
(`grpc: {port: …, service: liveness}`); `tcpSocket` on the gRPC port is the simpler equivalent and
the one the documentation leads with. `liveness` reports only that the process is up and
answering, which is what liveness means; it is `SERVING` during a migration and during a wait.

**Rejected:** reporting `SERVING` for `""` during initialization and relying on the gate. Health
would then say the opposite of what every RPC returns.

### D4. On SIGTERM during a migration, the claim is left to the lease

The hook stops the server and telemetry and does not touch the migration. The heartbeat is a
daemon thread and stops with the JVM; the next process sees a stale claim, waits for the orphaned
operation to end (D6), and takes over, which spends one of the three takeovers for that version.

The alternative is to stop deliberately: kill the migration's in-flight operation, wait for it to
end, and release the claim. Rejected for this ticket:

- **Releasing without killing is unsafe.** An ordinary claim of a released marker runs no
  orphaned-operation check (`SchemaMigrationRunner.java:459-471`); only a takeover does. A
  release that left the operation running would restore the pile-up of scans #292's D5 exists to
  prevent.
- **Interrupting the init thread is worse than doing nothing.** The driver turns an interrupt into
  a `RuntimeException`, which `applyPending()` would mark as a failed migration, and a marked claim
  waits for an operator.
- **Killing needs a path that does not exist and a privilege that may not.** A new
  runner-level abort, a classification of "aborted" beside "failed", and `killOp` through mongos,
  where the `inprog` grant is already the thing sharded deployments lack.
- **Little is lost by not doing it.** v4's orphaned `updateMany` finishes on the server and its
  work stands. v5's `$merge` also finishes. A killed v5 would have lost its progress.

The cost accepted: three SIGTERMs of the migrating process within one schema version reach the
cap and need an operator. With the probes no longer killing it, a SIGTERM during a migration is a
node drain or an operator's restart.

### D5. A failed initialization exits non-zero, from one place

On a false return or any `Throwable` out of `initService_()`, the init thread logs it, moves to
`FAILED`, and shuts the server down. `main()` learns of it when the server terminates.

The six `main()` methods become one line: `server.run()`, a new method on `GrpcServerBase` that
calls `start()`, then `blockUntilShutdown()`, and then, in state `FAILED`, `exitProcess(1)`.
`exitProcess()` calls `System.exit()` and is overridable for the test only, as
`QueueHandlerBase.exitOnFatalError()` is.

`System.exit()`, not a throw out of `main()`: a throw ends the process only if no non-daemon thread
remains, and an exit status that depends on which threads a failed initialization happened to
start is not a contract. It runs the shutdown hooks; in `FAILED` the hook has nothing left to do
for the service (D1), and the init thread has already ended.

`start()` still throws for what fails before the thread starts: the metrics bind and the gRPC
bind.

**The anti-pattern this guards against changes shape.** Before, it was a process serving from an
uninitialized handler (#254). Now it is a process that is alive, bound, and `NOT_SERVING` forever.
A supervisor that does not probe readiness, which is every deployment outside Kubernetes, would
never see that.

### D6. The claim wait is bounded by time without a sign of life

`migrateOrWait()` keeps one loop and one deadline variable. The deadline is no longer fixed at
entry: it is pushed out to "now plus the claim wait" whenever the waiter observes one of

- the watched `(claimToken, heartbeatSeq)` pair changing (a live holder; every 10 s by default);
- the recorded schema version advancing;
- the orphaned-operation check succeeding and finding an operation still running.

Nothing else moves it. In particular a failed claim attempt does not, so the per-attempt timeout
CLAUDE.md warns against cannot come back.

What that does to each state:

| State of the marker | Before | After |
|---|---|---|
| live holder, heartbeat advancing | exit at 5 min, restart, repeat | waits until it finishes |
| `migrationFailed` | exit at once | unchanged |
| stale, at the takeover cap | exit at once | unchanged |
| version newer than the binary | exit at once | unchanged |
| stale, orphaned operation still running | exit at 5 min, restart, repeat | waits until it ends, then takes over |
| stale, orphan check failing | exit at 5 min | unchanged: exit 5 min after the last sign of life |
| claim with no token (1.16.0 holder) | exit at 5 min | unchanged |
| unclaimed but never claimable (churn) | exit at 5 min | unchanged |

The orphaned operation counts as a sign of life because the wait it causes is finite: it is a scan
that ends. This is also what makes D4 coherent: a SIGTERM'd migrator leaves exactly this state, and
the next pod should sit `NOT_SERVING` through it, not crash-loop through it. The risk accepted is
an operation that never ends (blocked, not scanning); the log and the gauge (D8) show it, and
`killOp` ends it.

The three states that keep the deadline are the ones where no observation distinguishes "slow"
from "never": they exit with today's messages, which name the cause and the runbook.

`validateLeaseTimings()` keeps its third check. `staleAfter` must still be below the claim wait,
now for the reason that a claim must be seen to go stale before the deadline that follows the last
heartbeat.

While waiting on an orphan the runner currently logs at `info` on every check, every five seconds.
Over a wait of hours that is noise; it logs on the first check and then once a minute.

**Rejected:** an unconditional indefinite wait, as the ticket proposed. On a sharded deployment
without `inprog` a dead holder would then never be reported to anyone.

### D7. `InitializationStatus` is static

The gate's description (D2) and the gauges (D8) need to know whether this process is migrating or
waiting, and the runner that knows is four constructors below the server. `InitializationStatus`
(`common/server`) is a static holder of one immutable record: phase (`STARTING`, `MIGRATING`,
`WAITING`), the migration version and target, and when the phase began. The runner sets it; the
gate and the gauges read it.

Static state is what `DpTelemetry` avoids for `GlobalOpenTelemetry`, for the reason that the
integration tests build many servers in one JVM. It is acceptable here because the value describes
the database, not the server: every service in one JVM reads the same marker. A stale value is
harmless once a gate is open, since nothing reads the description then. `resetForTest()` exists
for the runner's tests.

**Rejected:** a listener passed server → service → handler → client → runner. Three handler
factories and their test doubles change to carry a value only one class produces.

### D8. Three gauges, no new attribute

All carry `dp.service` only, and are declared on `DpMetrics`:

| Instrument | Value |
|---|---|
| `dp.service.ready` | 1 once `READY`, else 0 |
| `dp.service.init.elapsed` (`s`) | seconds since `start()` while initializing; the final initialization time once ready |
| `dp.schema.migration.active` | 1 while this process holds the migration claim, else 0 |

A hung migration is `ready` 0 with `elapsed` climbing, on a pod whose probes pass. `active`
separates the migrator from the waiters. The phase is not an attribute: D8 of #212 closes the
vocabulary, and a numeric enum is a value operators cannot read without the table.

They are registered after `DpTelemetry.init()` and before the server is built, and closed in
`stopServer()` before `DpTelemetry.shutdown()`.

Gated calls are counted by gRPC's own instrumentation with `grpc_status="UNAVAILABLE"`, and by no
dp instrument, since no handler is entered. `doc/metrics.md` says so.

### D9. Probe and rollout guidance

Documented in `doc/running.md`:

- **`livenessProbe`:** `tcpSocket` on the gRPC port, or `grpc` with `service: liveness`. Never
  `grpc` with no service name (finding 1).
- **`readinessProbe`:** `grpc` with no service name. Passes only once `SERVING`.
- **`startupProbe`:** not needed. If one is kept it must be the liveness form, not the readiness
  form, or it kills the pod at `failureThreshold × periodSeconds`.
- **Rollout for a schema-changing upgrade:** `strategy: Recreate`, or scale to zero first
  (finding 2). A `RollingUpdate` leaves the old pod serving for the whole migration. The release
  note of any release that changes the schema says so.
- **A probe that still kills the migrator reaches #292's cap.** The fourth process exits for an
  operator. That is intended.

Readiness means "able to serve". The alternative reading of SLAC's comment, readiness passing
while the pod migrates so that the old pod is retired sooner, is rejected: it puts a pod that
rejects every call behind the Service, and still does not order the old pod's stop before the
migration's start.

### D10. Integration tests keep synchronous initialization; the startup path gets its own test

`GrpcIntegrationServiceWrapperBase` is unchanged. It serves a Mockito mock of the service on an
in-process server after initializing synchronously, and routing the sixty IT classes through a
background start would add a readiness wait to every test for no coverage the new test lacks.

`GrpcServerBaseStartupTest` drives the real `start()` with a small `GrpcServerBase` subclass whose
`initService_()` blocks on a latch and returns what the test tells it to, on ephemeral ports
(`getBoundPort()` is added for it). That covers the bind order, the gate, health, the failure
exit, and shutdown during initialization, with no migration fixture. The wait policy (D6) is
covered where the runner's other wait tests are, in `SchemaMigrationLeaseTest`.

## Implementation tasks

### Task 1. `pom.xml`

- Declare `io.grpc:grpc-services` at `${io.grpc.version}`, with the comment `grpc-opentelemetry`
  carries.

### Task 2. `ReadinessGate` and `InitializationStatus`

- `common/grpc/ReadinessGate.java`: the interceptor of D2. `open()`, `isOpen()`, and a
  `Supplier<String>` for the description.
- `common/server/InitializationStatus.java`: D7. `set…()` for each phase, `current()`,
  `describe()` producing the D2 strings, `resetForTest()`.

### Task 3. `GrpcServerBase`

- Reorder `start()` per D1; add the lifecycle state and its lock, the init thread, `run()`,
  `exitProcess()`, `getBoundPort()`.
- Health per D3: the manager, the three names, `enterTerminalState()` in `stopServer()`.
- `stopServer()` and `blockUntilShutdown()` call `finiService_()` only from `READY` (D1).
- The init thread's body is under `catch (Throwable)`; a fatal error still ends the process, by
  the same exit.
- Rewrite the Javadoc on `initService_()` and the comments in `start()`: several describe the
  order this ticket changes.

### Task 4. The six `main()` methods

- `IngestionGrpcServer`, `QueryGrpcServer`, `AnnotationGrpcServer`, `IngestionStreamGrpcServer`,
  `BenchmarkIngestionGrpcServer`, `BenchmarkQueryGrpcServer`: `server.run()`.

### Task 5. `QueueHandlerBase`

- Null guard on `executorService` in `fini()` (finding 4).

### Task 6. `SchemaMigrationRunner`

- The deadline of D6: one variable, extended at the three observations, with a comment at each
  naming it as a sign of life.
- Set `InitializationStatus`: `MIGRATING` with the version before each `apply()`, `WAITING` when
  the loop first finds a claim it does not hold, back to `STARTING` on return.
- Throttle the orphan-wait log (D6).
- Reword `describeTimeout()`: "timed out after 300s" becomes time since the last sign of life.
- Update the class and `migrateOrWait()` Javadoc.

### Task 7. `DpMetrics`

- The three gauges of D8, with registration helpers returning `AutoCloseable`, as
  `registerHandlerWorkersMax()` does.

### Task 8. Tests

Each new test is run once with its fix reverted, to see it fail.

- **`ReadinessGateTest`:** closed, a unary and a streaming call each end `UNAVAILABLE` with the
  description and never reach the service; open, both pass.
- **`GrpcServerBaseStartupTest`** (D10):
  - while `initService_()` is blocked: the port is bound; health is `NOT_SERVING` for `""` and
    the service name and `SERVING` for `liveness`; an RPC gets `UNAVAILABLE`; `dp.service.ready`
    is 0;
  - after it returns true: health is `SERVING`, the RPC succeeds, `dp.service.ready` is 1;
  - when it returns false, and when it throws: `exitProcess(1)` is called, and both ports can be
    bound again;
  - `stopServer()` while it is blocked: no exception, `finiService_()` is not called by the
    stopping thread, and the gate does not open when it is then released.
- **`SchemaMigrationLeaseTest`:** a waiter outlasts the claim wait while the holder's heartbeat
  advances; a waiter outlasts it while an orphaned operation is reported and takes over when it
  ends; a waiter on a token-less claim, and one whose orphan check fails, still exit at the claim
  wait. The existing timeout tests that assumed a fixed deadline are updated, not deleted.
- **`QueueHandlerBase`:** `fini()` on a handler whose `init_()` has not returned does not throw.
- **Measured, not automated, and recorded in this plan before merge:**
  - a kubelet (kind or minikube) running the documented probes against a service held in
    initialization: the liveness forms pass, the readiness form fails, and a `grpc` liveness probe
    with no service name kills the pod (finding 1);
  - the shaded jar serves the health service (`grpc_health_probe` or `grpcurl` against a service
    started from `target/dp-service-*-shaded.jar`), since the health service is the first thing
    here loaded from `grpc-services`.

### Task 9. Documentation

- `doc/running.md`: the health service and its three names, D9 in full, and what `UNAVAILABLE`
  during startup means for a client.
- `doc/runbooks/schema-migration.md`: waiting services now stay up `NOT_SERVING`; rewrite the
  "timed out after 300s" section, the v4 and v5 notes ("waiting services will time out and must
  be restarted"), the probe bullet under the takeover cap, and "This does not stop a probe from
  killing the migrating pod".
- `doc/runbooks/upgrade-1.16-slac.md`: leave as the record of that upgrade; add one line pointing
  at the new behaviour.
- `doc/metrics.md`: the three gauges; gated calls and `grpc_status`.
- `doc/release-notes/NEXT.md`: a section for this ticket, including the changed exit behaviour of
  waiting services and the probe guidance.
- `CLAUDE.md`: "Startup failure stops the server" (the background path, `run()`, the new
  anti-pattern); "The claim wait is bounded by one deadline" (D6); "Initialization order is
  load-bearing" (unchanged rule, new position); a short section for the gate, the health names
  and why `liveness` exists.
- `src/test/resources/application.yml`: the comment saying no test starts a server through
  `GrpcServerBase`.

## Out of scope

- A running service noticing a migration and stopping: #331.
- Stopping a migration deliberately on SIGTERM (D4): no ticket; raise one if takeovers spent on
  routine restarts become a problem in practice.
- A standalone migration entry point (a Kubernetes Job or Helm hook): not planned. Kubernetes is
  optional for this project, and this ticket gives every deployment the same behaviour.
- Kubernetes manifests: this repo ships none. The guidance is documentation.
- Topology misdetection through mongos: #291.
- `serviceImpl.init()` ignoring a false return from `handler.start()`, in all four services. No
  handler's `start()` can return false today. No ticket.

## Dependencies and sequencing

- **#292 is merged.** Nothing blocks this ticket.
- **No dp-grpc change**: the health service's protos are in `grpc-services`. No schema migration,
  and `SCHEMA_VERSION` stays 5.
- **No new configuration keys.** The claim wait remains a constant.
- Task 2 precedes Tasks 3 and 6; Task 3 precedes Task 4. Tasks 5, 6 and 7 are independent of
  Task 3 except that Task 3 registers Task 7's gauges.
- The two measurements in Task 8 do not block the other tasks, but must be done before merge:
  the first can change D3 and D9.
- **#331 depends on this ticket**, for the gate and the health service.
- Upgrade order is unchanged: stop every service before a schema-changing upgrade. The gate
  protects the starting process, not the ones already running.
