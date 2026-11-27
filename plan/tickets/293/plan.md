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

Only the upgrade procedure gives the ordering "old instances stop, then the migration begins", and
a rollout strategy is not enough of one. `strategy: Recreate` orders old before new within one
Deployment. With each service in its own Deployment, the new pod of one service claims the
migration while the old pods of the others are still serving, and an old ingestion process writing
after the v5 seed is the case CLAUDE.md records as unsafe. The ticket did not mention rollout at
all. See D9 and #331.

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

### 8. The handler registers a shutdown hook of its own

`QueueHandlerBase.init()` ends by registering a JVM shutdown hook that calls `fini()`
(`QueueHandlerBase.java:277`). Two consequences for this ticket:

- `Runtime.addShutdownHook()` throws `IllegalStateException` once shutdown has begun. An init
  thread that finishes its migration after a SIGTERM would throw there, after the executor and its
  workers exist, and never reach the rule D1 gives for that case.
- On a SIGTERM in normal service the handler's hook runs beside the server's, not after it. The
  handler stops while the gRPC server is still inside its 30 s drain, so the drain D3 relies on
  when it takes a stopping pod out of the endpoints does not exist today. This predates the
  ticket.

See D11.

### 9. `HealthStatusManager` starts `SERVING`, and its terminal state covers every name

Read from the 1.65.1 bytecode, not from documentation: the health service's constructor records
`SERVING` for `""`, so a manager that has not been told otherwise passes a readiness probe from the
moment the port is bound. And `enterTerminalState()` sets every registered name to `NOT_SERVING`
and ignores later changes, which includes a name meant to be always `SERVING`. See D3.

### 10. What depends on the `inprog` grant, and what does not

SLAC is the only deployment and reported the incident, so the plan is measured against a sharded
cluster reached through mongos, where the orphaned-operation check needs the `inprog` action
(#292). Whether SLAC's service user holds it is **not known**: the startup line that reports it
exists only from #292 on, and SLAC runs 1.16.0.

| | Needs `inprog` through mongos |
|---|---|
| the migrating pod is not killed by a probe (D1, D3) | no |
| the other services wait while the migrator's heartbeat advances (D6) | no |
| a migrator that dies anyway (node drain, OOM kill, a deleted pod) is taken over | **yes** |

The incident as reported is the first two rows. Without the grant the third row is what it is
today: the claim is cleared by hand. The grant is therefore a deliverable of this ticket's
documentation and a question to SLAC, not an assumption. See D8, D9 and Dependencies.

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
- `INITIALIZING` or `READY` → `STOPPING`: the shutdown hook.
- **`FAILED` is terminal.** The hook finds it and leaves it: `System.exit()` runs the hook, and
  `run()` decides the exit status from this field (D5).
- An init thread that finishes and finds `STOPPING` does not open the gate. If `initService_()`
  returned true it calls `finiService_()` itself, for what it built. If it returned false or
  threw, it logs and ends: the JVM is already halting, the state stays `STOPPING`, and it must
  not call `System.exit()`, which blocks for ever when called during shutdown.

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
| `liveness` | `SERVING` from the bind until `stopServer()` |

**All three are set before the server is started, not after.** The manager starts with `""` at
`SERVING` (finding 9), so setting it after the bind leaves a moment in which a readiness probe
passes on a service with no handler. One method builds the manager with the three statuses and
returns it for the builder, so that the order cannot be got wrong at the call site and the test
can check the method's result without racing a bind.

`stopServer()` calls `enterTerminalState()` first, so a draining pod leaves the endpoints (and,
with D11, has a handler to drain into). That turns `liveness` to `NOT_SERVING` as well (finding
9), for the length of the drain. A stopping process has no use for a liveness probe, and Task 8
measures that a kubelet does not act on one for a terminating pod; if it does, `stopServer()`
sets the two serving names to `NOT_SERVING` by hand and leaves `liveness` alone.

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

On a false return or any `Throwable` out of `initService_()`, the init thread moves to `FAILED`
and calls `server.shutdown()`. `main()` learns of it when the server terminates.

**Those two steps are in a `finally`, and the log call is not what they wait on.** The whole
failure signal crosses from the init thread to `main()` through the server terminating. If
logging the failure threw first (it formats an arbitrary exception and allocates, the PR #318
case), the init thread would end with the server bound and `main()` parked: alive, bound and
`NOT_SERVING` for ever, which is the anti-pattern this decision exists to prevent. So the log
call is under its own `catch (Throwable)`, and the init thread does nothing else on this path:
no telemetry shutdown, no `finiService_()`.

**A fatal error halts; it does not take the exit path.** For a `Throwable` that
`QueueHandlerBase.isFatal()` accepts, the init thread halts the process with status 3, from a
`finally` after the log call, through an overridable method as the workers do. An
`OutOfMemoryError` is not local to the thread that reports it, so a handoff to another thread and
an orderly exit are the wrong things to depend on, and one process should not report the same
condition with two statuses.

The six `main()` methods become one line: `server.run()`, a new method on `GrpcServerBase` that
calls `start()`, then `blockUntilShutdown()`, and then, in state `FAILED`, closes the gauges,
shuts telemetry down, and calls `exitProcess(1)`. `exitProcess()` calls `System.exit()` and is
overridable for the test only, as `QueueHandlerBase.exitOnFatalError()` is.

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

**"Changing" is judged against the last pair this waiter read, kept in variables of its own.**
The loop's existing `watchedToken`/`watchedSeq` cannot serve: they are reset to null at three
places (a refused claim, a refused takeover, a claim with no token) to restart the staleness
watch, so the existing "differs from the watched pair" test is also true the first time any claim
is seen and every time the same dead pair is seen again after a reset. A deadline extended there
would be extended by a holder that has shown nothing. The rule is:

- the first pair read after entry, or after the marker was unclaimed, extends nothing: the
  deadline already allows a full claim wait from that moment;
- a different `heartbeatSeq` under the same token extends it;
- a different token extends it once per token, which a takeover by another waiter needs. Claims
  that follow one another without the version advancing are bounded by #292's cap of three, so
  this cannot run on.

The version is compared with the last version read in the same way.

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

A failed read of the marker still ends the wait with a `DpException`, as today. A wait of hours
polls the marker thousands of times, so one election can end a waiter; it exits non-zero, is
restarted, and joins the wait again. Retrying the read inside the loop is not worth a second
bound to reason about. The runbook says to expect it.

**Rejected:** an unconditional indefinite wait, as the ticket proposed. On a sharded deployment
without `inprog` a dead holder would then never be reported to anyone.

### D7. `InitializationStatus` is static

The gate's description (D2) and the gauges (D8) need to know whether this process is migrating or
waiting, and the runner that knows is four constructors below the server. `InitializationStatus`
(`common/server`) is a static holder of one immutable record: phase (`STARTING`, `MIGRATING`,
`WAITING`), the migration version and target, when the phase began, and whether the startup
orphaned-operation check could be run (unknown, yes, no; D8). The runner sets it; the gate and
the gauges read it.

Static state is what `DpTelemetry` avoids for `GlobalOpenTelemetry`, for the reason that the
integration tests build many servers in one JVM. It is acceptable here because the value describes
the database, not the server: every service in one JVM reads the same marker. A stale value is
harmless once a gate is open, since nothing reads the description then. `resetForTest()` exists
for the runner's tests.

**Rejected:** a listener passed server → service → handler → client → runner. Three handler
factories and their test doubles change to carry a value only one class produces.

### D8. Four gauges, no new attribute

All carry `dp.service` only, and are declared on `DpMetrics`:

| Instrument | Value |
|---|---|
| `dp.service.ready` | 1 once `READY`, else 0 |
| `dp.service.init.elapsed` (`s`) | seconds since `start()` while initializing; the final initialization time once ready |
| `dp.schema.migration.active` | 1 while this process holds the migration claim, else 0 |
| `dp.schema.migration.takeover.enabled` | 1 if the startup orphaned-operation check ran, 0 if it was refused |

A hung migration is `ready` 0 with `elapsed` climbing, on a pod whose probes pass. `active`
separates the migrator from the waiters. The phase is not an attribute: D8 of #212 closes the
vocabulary, and a numeric enum is a value operators cannot read without the table.

`takeover.enabled` is the result of `probeOrphanCheck()`, which #292 already runs at every start
and reports in one `warn` line. A missing `inprog` grant decides whether a dead migrator recovers
by itself (finding 10), and a log line read once is a poor place to keep that: the gauge can be
alerted on, on an ordinary day. It reports nothing until the check has run, so that "not yet
known" is not read as "refused", and the ingestion stream service, which has no Mongo client,
never reports it.

They are registered after `DpTelemetry.init()` and before the server is built, and closed in
`stopServer()` before `DpTelemetry.shutdown()`.

Gated calls are counted by gRPC's own instrumentation with `grpc_status="UNAVAILABLE"`, and by no
dp instrument, since no handler is entered. `doc/metrics.md` says so.

### D9. Probe guidance, the `inprog` grant, and an upgrade procedure

Documented in `doc/running.md`, with a complete example of the three probes as YAML, since a
description of a probe is easier to get wrong than a copy of one:

- **`livenessProbe`:** `tcpSocket` on the gRPC port, or `grpc` with `service: liveness`. Never
  `grpc` with no service name (finding 1).
- **`readinessProbe`:** `grpc` with no service name. Passes only once `SERVING`.
- **`startupProbe`:** not needed. If one is kept it must be the liveness form, not the readiness
  form, or it kills the pod at `failureThreshold × periodSeconds`.
- `grpc` probes are built into the kubelet from Kubernetes 1.24; before that, `tcpSocket` for
  liveness and an exec of `grpc_health_probe` for readiness.
- **A probe that still kills the migrator reaches #292's cap.** The fourth process exits for an
  operator. That is intended.

**The `inprog` grant is a stated prerequisite on a sharded deployment**, in `doc/running.md`
beside the probes and in the release note, not only in the runbook where #292 put it. Without it
the probes still stop killing the migrator, but a migrator that dies for any other reason leaves a
claim to clear by hand (finding 10). The text gives the role (already in
`doc/runbooks/schema-migration.md`), the `warn` line, and the gauge of D8 as the way to confirm
it on a running service.

**The upgrade procedure for a schema-changing release is written out as steps**, in
`doc/runbooks/schema-migration.md`, for Kubernetes and in general terms:

1. Confirm `dp.schema.migration.takeover.enabled` is 1 on the running services (sharded only).
2. Stop every dp service: scale all four Deployments to zero and wait for the pods to be gone.
3. Deploy the new version and scale up. One pod claims the migration; the others come up
   `NOT_SERVING` and wait.
4. Watch `dp.schema.migration.active`, `dp.service.init.elapsed` and the migrator's log. Do not
   restart the migrating pod: each restart spends a takeover (D4).
5. The upgrade is complete when `dp.service.ready` is 1 on every service.

Step 2 is the rule and `strategy: Recreate` does not replace it (finding 2): it stops the old pods
of one Deployment, not of the other three. A `RollingUpdate` leaves even the same service's old
pod serving for the whole migration. The release note of any release that changes the schema
points at the procedure.

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

The test's cases split by how telemetry is set up, because `DpTelemetry.init()` does nothing once
`initForTest()` has run. The cases that read a gauge install an in-memory reader first, and
`start()`'s own `init()` is then ignored and binds no metrics port. The cases that assert the
ports are released install nothing, so that `start()` binds a real metrics port, and assert on
that port and the gRPC port. Asserting port release in a case that used `initForTest()` would
pass without a metrics port ever having been bound.

### D11. The handler's own shutdown hook is removed

`QueueHandlerBase.init()` no longer registers a shutdown hook (finding 8). `stopServer()` already
calls `finiService_()`, after the server has drained and before telemetry shuts down, which is the
order CLAUDE.md requires; the handler's hook only ever ran the drain early and beside it.

- An init thread finishing after a SIGTERM no longer throws from `addShutdownHook()`.
- A stopping service drains: the server stops taking calls, in-flight calls finish against a live
  handler, then the handler stops. D3's `enterTerminalState()` then means something.
- One behaviour changes and the release note says so. The ingestion service rejects requests
  when `handler.getShutdownRequested()` is set (`IngestionServiceImpl.java:229`, `:275`, `:407`,
  and `IngestDataStreamRequestObserver.java:35`). Today that is from the instant of the SIGTERM;
  after this it is from the end of the server's drain, so messages on a stream already open are
  accepted and persisted for up to 30 s longer, not rejected.

The only callers of a handler's `init()` outside the four service implementations are two unit
tests, which call `fini()` themselves. The integration-test wrappers build many handlers in one
JVM and each has been leaving a hook registered for the life of that JVM.

**Rejected:** keeping the hook and catching `IllegalStateException` around its registration. It
fixes the first point and leaves the service without a drain.

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
- Health per D3: one method that builds the manager with all three names set, called before the
  builder is built; `enterTerminalState()` in `stopServer()`.
- `stopServer()` and `blockUntilShutdown()` call `finiService_()` only from `READY` (D1).
  `FAILED` is terminal, and the hook does not overwrite it.
- The init thread's body is under `catch (Throwable)`. The move to `FAILED` and
  `server.shutdown()` are in a `finally`; the log call is under its own catch; a fatal error
  halts with status 3 through an overridable method (D5). In `STOPPING` it never calls
  `System.exit()` (D1).
- `run()` closes the gauges and shuts telemetry down before `exitProcess(1)`.
- Rewrite the Javadoc on `initService_()` and the comments in `start()`: several describe the
  order this ticket changes.

### Task 4. The six `main()` methods

- `IngestionGrpcServer`, `QueryGrpcServer`, `AnnotationGrpcServer`, `IngestionStreamGrpcServer`,
  `BenchmarkIngestionGrpcServer`, `BenchmarkQueryGrpcServer`: `server.run()`.

### Task 5. `QueueHandlerBase`

- Null guard on `executorService` in `fini()` (finding 4).
- Remove the shutdown hook registered at the end of `init()` (D11). Confirm, before deleting it,
  that every `fini()` an integration-test wrapper relies on is called explicitly.

### Task 6. `SchemaMigrationRunner`

- The deadline of D6: one variable, extended at the three observations, with a comment at each
  naming it as a sign of life. The last pair and version read are held in their own variables,
  apart from `watchedToken`/`watchedSeq`, and the first pair read extends nothing.
- Record the result of `probeOrphanCheck()` in `InitializationStatus` (D8).
- Set `InitializationStatus`: `MIGRATING` with the version before each `apply()`, `WAITING` when
  the loop first finds a claim it does not hold, back to `STARTING` on return.
- Throttle the orphan-wait log (D6).
- Reword `describeTimeout()`: "timed out after 300s" becomes time since the last sign of life.
- Update the class and `migrateOrWait()` Javadoc.

### Task 7. `DpMetrics`

- The four gauges of D8, with registration helpers returning `AutoCloseable`, as
  `registerHandlerWorkersMax()` does. `takeover.enabled` records nothing while the result is
  unknown.

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
    bound again (run without `initForTest()`, D10);
  - when it throws an exception whose `toString()` throws: the server still terminates and
    `exitProcess(1)` is still called (D5);
  - when it throws a fatal error: the halt method is called with status 3;
  - `stopServer()` while it is blocked: no exception, `finiService_()` is not called by the
    stopping thread, and the gate does not open when it is then released; released to fail, it
    does not call `exitProcess()`;
  - `stopServer()` after a failure leaves the state `FAILED`;
  - the health manager as built, before any server exists, reports `NOT_SERVING` for `""` and the
    service name and `SERVING` for `liveness` (D3).
- **`SchemaMigrationLeaseTest`:** a waiter outlasts the claim wait while the holder's heartbeat
  advances; a waiter outlasts it while an orphaned operation is reported and takes over when it
  ends; a waiter on a token-less claim, and one whose orphan check fails, still exit at the claim
  wait; a waiter on a claim whose heartbeat never moves exits one claim wait after entry, the
  resets of the staleness watch notwithstanding (D6). The existing timeout tests that assumed a
  fixed deadline are updated, not deleted.
- **`QueueHandlerBase`:** `fini()` on a handler whose `init_()` has not returned does not throw;
  `init()` registers no shutdown hook (D11).
- **`takeover.enabled`:** 1 after a runner whose check succeeds, 0 after one whose check is
  refused, and no point before either has run.
- **Measured, not automated, and recorded in this plan before merge:**
  - a kubelet (kind or minikube) running the documented probes against a service held in
    initialization: the liveness forms pass, the readiness form fails, and a `grpc` liveness probe
    with no service name kills the pod (finding 1); and what a `grpc` liveness probe on
    `liveness` does to a terminating pod once `enterTerminalState()` has run (D3);
  - the shaded jar serves the health service (`grpc_health_probe` or `grpcurl` against a service
    started from `target/dp-service-*-shaded.jar`), since the health service is the first thing
    here loaded from `grpc-services`.

### Task 9. Documentation

- `doc/running.md`: the health service and its three names, the probes of D9 with the example
  YAML, the `inprog` prerequisite, and what `UNAVAILABLE` during startup means for a client.
- `doc/runbooks/schema-migration.md`: the upgrade procedure of D9; waiting services now stay up
  `NOT_SERVING`; a waiter that exits on a failed marker read rejoins on restart (D6); rewrite the
  "timed out after 300s" section, the v4 and v5 notes ("waiting services will time out and must
  be restarted"), the probe bullet under the takeover cap, and "This does not stop a probe from
  killing the migrating pod".
- `doc/runbooks/upgrade-1.16-slac.md`: leave as the record of that upgrade; add one line pointing
  at the new behaviour.
- `doc/metrics.md`: the four gauges, with an alert on `takeover.enabled` 0; gated calls and
  `grpc_status`.
- `doc/release-notes/NEXT.md`: a section for this ticket, including the changed exit behaviour of
  waiting services, the probe guidance, the `inprog` prerequisite, the ingestion service's
  changed behaviour during a stop (D11), and that this release should be deployed before any
  release that changes the schema.
- `CLAUDE.md`: "Startup failure stops the server" (the background path, `run()`, the new
  anti-pattern, the `finally` and the halt of D5); "The claim wait is bounded by one deadline"
  (D6, and why the sign of life is not the staleness watch); "Initialization order is
  load-bearing" (unchanged rule, new position); a short section for the gate, the health names,
  why `liveness` exists and why the statuses are set before the bind; the handler no longer has a
  shutdown hook (D11).
- `src/test/resources/application.yml`: the comment saying no test starts a server through
  `GrpcServerBase`.

## Out of scope

- A running service noticing a migration and stopping: #331.
- Stopping a migration deliberately on SIGTERM (D4): no ticket; raise one if takeovers spent on
  routine restarts become a problem in practice.
- A standalone migration entry point (a Kubernetes Job or Helm hook): not in this ticket, and not
  a substitute for it. A Job has no probes and is not tied to a service pod's lifecycle, but a
  Job pod drained partway leaves the same stale claim and needs the same `inprog` grant to be
  taken over, and without this ticket the services still have nothing correct to probe and still
  crash-loop if started while the Job runs. A possible follow-on once SLAC has run the procedure
  of D9; no ticket yet.
- Kubernetes manifests: this repo ships none. The guidance is documentation.
- Topology misdetection through mongos: #291.
- `serviceImpl.init()` ignoring a false return from `handler.start()`, in all four services. No
  handler's `start()` can return false today. No ticket.

## Dependencies and sequencing

- **#292 is merged.** Nothing blocks this ticket.
- **This ships in a release before any release that changes the schema, not in the same one.**
  It changes no schema itself, so what a deployment gets from it at once is the health service,
  correct probes and a reason on `UNAVAILABLE`. The protection of a long migration is worth
  something only if the probes, the `inprog` grant and the upgrade procedure are in place and
  have been run on an ordinary restart before the upgrade that needs them. Do not add a
  migration to the release that carries this ticket.
- **Open question to SLAC, to ask now and not at the upgrade:** does the service user hold
  `inprog` through mongos (finding 10)? The answer does not change the design. It decides whether
  the grant is a step in their next deployment.
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
