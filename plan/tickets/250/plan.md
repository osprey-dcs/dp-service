# dp-service #250: Gate merges on the integration suite, and the remaining CI cleanup from #213

- **Ticket**: [osprey-dcs/dp-service#250](https://github.com/osprey-dcs/dp-service/issues/250), which
  now also carries the remaining items from [#213](https://github.com/osprey-dcs/dp-service/issues/213).
  #213 is closed as superseded.
- **Sibling**: [#221](https://github.com/osprey-dcs/dp-service/issues/221) (Sigstore signing) owns
  #213 items 3, 8, and the release-workflow half of item 5, and restructures `release.yml` and
  `release-image.yml`. See Dependencies and sequencing.
- **Status**: triaged and planned 2026-09-23; not implemented.

## Overview

CI runs `mvn -B test`, which is Surefire only. Surefire excludes `**/*IT.java` (`pom.xml:285-290`),
and Failsafe binds the integration tests to `verify`, so **no integration test gates a merge**. Most
of this repo's important coverage lives in those tests: the annotation CRUD surface, Query V2,
every column type, the export framework, and the regression guards for several invariants
CLAUDE.md calls silent-wrong-answer risks.

This ticket makes the full integration suite a merge gate, cheaply enough to run on every PR. It
also finishes the CI and release hygiene that #213 collected. It is for everyone merging to
`main`, and for whoever cuts the next release.

## Background: triage findings

Checked on 2026-09-23 against `main` (`44901a8`), the `rel-1.16.0` release run (`35150611846`),
and local runs against MongoDB 8. The core premise of #250 holds. Most of its sizing does not,
and the correction removes the design problem #250 was built around.

### What holds

| Claim | Verified |
|---|---|
| CI runs `mvn -B test`, and no `*IT` runs | Yes: `ci.yml:93-95`; Surefire excludes `*IT` (`pom.xml:288-290`) |
| The CI step name implies ITs run | Yes: "Run Maven tests (integration/unit)", `ci.yml:93` |
| IT base classes start and stop the full service stack per test method | Yes: `GrpcIntegrationTestBase.setUp()`/`tearDown()` (`:54-113`), called from `@Before`/`@After` |
| #213 item 2: the release re-runs the whole suite at tag time | Yes: `release.yml:102`, `mvn -B clean verify` |
| #213 item 4: `ci.yml` requests `packages: write` it never uses | Yes: `ci.yml:10`; no step pushes anything |
| #213 item 5 (CI half): no `concurrency` group | Yes |
| #213 item 6: the MongoDB wait loop is duplicated, spelled two ways | Yes: `ci.yml:78-91`, `release.yml:81-93`, `release-image.yml:143-156` |
| #213 item 7: the dp-grpc tag is not checked for existence up front | Yes: `release.yml:64-70` fails at checkout |

### 1. The suite's state is known, and it is green: 60 classes, 344 tests, 0 failures

#250's first proposed step is to "run the full suite once manually on a CI runner to find what
already fails and how long it really takes." The release workflow already does this on every tag.
`rel-1.16.0` ran `mvn -B clean verify` on a standard `ubuntu-latest` runner:

- **Unit tests:** 631 run, 0 failures.
- **Integration tests:** 60 classes, 344 tests, 0 failures, 1 skipped. #250 said 52 classes and
  about 280 tests.
- **Integration phase:** 1483 s summed across classes, about 25 minutes. The whole `verify` took
  26:03. #250 extrapolated "well over an hour".

The 1.15.0 release runs were green too. The suite is not in an unknown state. It has passed at
every release. It just runs later than a merge gate would.

### 2. `BenchmarkIntegrationIT` is already excluded

#250 asks for it to be excluded explicitly. It is already `@Ignore`d (`BenchmarkIntegrationIT.java:36`,
"doesn't provide unique test coverage"), and it is the one skipped test above.

### 3. The per-test overhead is a shutdown wait, not fixture work

#250 attributes the ~4 s per test to fixture setup and proposes moving it to `@BeforeClass`. That
change would need to be justified class by class, because `MongoTestClient.init()` cleans the
database between tests. The measured cause is somewhere else.

`QueueHandlerBase.QueueWorker.run()` (`QueueHandlerBase.java:117-131`) polls its queue with a
**1-second timeout** (`POLL_TIMEOUT_SECONDS = 1`, `:20`). It checks `shutdownRequested` only between
polls. `fini()` sets the flag and then waits in `awaitTermination`, so every idle worker holds the
shutdown for up to a full second. `tearDown()` shuts down four handlers in sequence
(`GrpcIntegrationTestBase.java:94-97`). That comes to about 4 s per test, doing nothing.

Measured locally against MongoDB 8 with `PvMetadataIT` (28 tests):

| Worker poll timeout | Class time | Per test | Result |
|---|---|---|---|
| 1 s (current) | 116.6 s | 4.16 s | 28/28 pass. CI measured 117.3 s for the same class |
| 100 ms | 16.6 s | 0.59 s | 28/28 pass |

Full suite with the 100 ms timeout (`mvn -B clean verify`, local): **5:32 total**, with 631 unit tests
and 344 integration tests, 0 failures, 1 skipped. The integration phase summed to **303.9 s across
60 classes**, against 1483 s in the release run. That comparison crosses machines, but the one
class measured both ways matched to within a second (117.3 s on CI, 116.6 s locally), so the
roughly 5× reduction should carry over to the runner. Task 2 confirms it there.

This removes the need for #250's curated subset. The subset existed only to fit a budget that the
shutdown wait was using up. It also avoids the risk in the `@BeforeClass` route, which would have
given up per-test isolation to save time that was never going to fixtures.

### 4. The release and image builds depend on the host the vendored jhdf5 exists to avoid

CLAUDE.md, "Vendored dependency: `cisd:jhdf5`", says deleting the vendored install step "breaks CI
on every pull request." That step exists only in `ci.yml:62-66`. Three other build paths still
resolve jhdf5 from `maven.scijava.org`:

- **`release.yml`** has no install step. `rel-1.16.0` succeeded because SciJava happened to be up.
- **`release-image.yml`** runs `mvn -B test` on the runner (`release-image.yml:160`) before it
  builds the image, with no install step ahead of it. It fails there, before the Docker build
  starts.
- **The `Dockerfile`** runs `mvn -B -DskipTests package` inside BuildKit (`Dockerfile:24`), where no
  runner step can pre-install anything. So `release-image.yml` fails whenever SciJava serves 503 for
  JARs, the exact failure recorded for 2026-08-27. The vendored jar is already in the build context
  (`COPY . /build/app`); it just is not installed there.

A release is the worst time to find this out: the `rel-*` tag is already public when it fails.

### 5. `main` never runs CI, so no PR can reuse a warm cache

`ci.yml` triggers only on `pull_request` (`ci.yml:3-6`). CLAUDE.md's jhdf5 section explains the
consequence: Actions caches are scoped per ref, a PR can read only its own caches and the default
branch's, and "every cache in this repo was created on a `refs/pull/NNN/merge` ref and none on
`main`." So every PR's first run resolves every Maven dependency cold. It also means nothing checks
`main` after a merge. Two PRs that each pass alone can break once both are merged, and nothing runs
to catch it.

### 6. Two workflows carry a dead Docker-layer cache step

`ci.yml:38-45` and `release-image.yml:74-81` restore and save `/tmp/.buildx-cache`, but nothing
reads or writes it. `ci.yml` builds no image, and `build-push-action` has no `cache-from`/`cache-to`.
`ci.yml:18` also declares an unused `IMAGE_NAME`.

### 7. CLAUDE.md's "Continuous Integration" section is wrong in three of four bullets

`CLAUDE.md:1472-1477` says CI triggers on "pushes/PRs to main/master; manual workflow dispatch",
uses a "MongoDB 8.0 service container", and uploads "Surefire and Failsafe test reports". In fact
it triggers on PRs only, starts MongoDB with `docker compose`, and uploads only the compose logs
and `docker ps` output. On a test failure, the reports a reviewer needs are not uploaded.

### 8. No check is required to merge, so CI is not a gate today

`main` has no branch protection (`GET /branches/main/protection` returns 404), and its only
ruleset (`15223477`) carries `deletion`, `non_fast_forward`, and `copilot_code_review` rules, with
no `required_status_checks`. A PR whose `build-and-test` run fails can be merged. Switching CI to
`verify` alone would therefore make the integration suite *visible* on every PR without making it
a gate, which is the thing #250 asks for.

### Other facts the design depends on

- **CI and the release test against different dp-grpc code.** CI checks out dp-grpc `main`
  (`ci.yml:20`). The release checks out `rel-${VERSION}` (`release.yml:69`). That is deliberate:
  during a cycle, dp-service may depend on unreleased dp-grpc changes. But it means a green CI run
  does not prove a green release build. This matters for #213 item 2 (D4).
- **The compose file already declares a MongoDB healthcheck** (`docker-compose.yaml:21-26`), so
  `docker compose up -d --wait` can replace the polling loop outright (#213 item 6).

## Design decisions

### D1 — Fix the shutdown wait with a shorter poll timeout, in production code

Change `QueueWorker`'s poll from 1 s to 100 ms. Express it in milliseconds, with a comment that
says the value bounds how long an idle worker delays `fini()`.

This is a production change, and it is safe as one. It shortens production shutdown by the same
seconds. The idle cost is 10 wakeups per second per worker: 28 workers across the four services
at the default `numWorkers: 7`, which is negligible. Job execution, the capacity-1 queue, and
`fini()`'s drain order are unchanged.

One existing behavior carries over unchanged: a worker exits its loop as soon as it sees
`shutdownRequested`, without draining the queue, so a job enqueued but not yet taken when `fini()`
runs is dropped without a response. That was already true at 1 s. A worker blocked in `poll()`
still receives a job the moment it is put, whatever the timeout, so the shorter poll changes only
how soon an *idle* worker notices the flag. It does not widen the window in which a queued job can
be stranded. The full suite passing at 100 ms (triage 3) is consistent with that. Draining the
queue on shutdown would be a separate change and is not needed here.

*Rejected:*

- **`shutdownNow()` to interrupt the workers.** It also interrupts in-flight jobs. CLAUDE.md's
  telemetry section depends on `fini()` letting running jobs finish ("Telemetry shuts down after
  the handler drains").
- **A poison-pill job per worker, so workers can block in `take()`.** That adds a second
  `requestQueue.put` site, and CLAUDE.md names `enqueueJob` as the only one ("`requestQueue.put`
  appears nowhere else — worth grepping for"). It also has to handle the capacity-1 queue while
  workers are busy. It is more code to save the last 100 ms.
- **`@BeforeClass` fixtures** (#250's proposal). The time is not spent in fixtures (triage 3), and
  the change would give up per-test database isolation class by class.

### D2 — Run the whole suite on every PR, not a curated subset

CI runs `mvn -B verify`. Every integration test gates every merge. #250's warning against a bare
`test` → `verify` swap was right given its numbers. With D1 the whole `verify` measured 5:32 locally, well under
the 10–15 minute budget #250 set for a subset (triage 3). A subset would need an owner, would silently leave
coverage out of the gate, and would need re-choosing every time a test is added.

The budget check is still worth having, in a different form: the CI run time is reported in the
PR that makes this change. If a later change pushes CI far past that, it should be treated as a
regression.

No nightly full-suite job, because the per-PR run already is the full suite.

### D3 — `main` gets CI runs too

Add `push: branches: [main]` to `ci.yml`. That catches breakage from combined merges (triage 5).
It also creates caches on `main`, which every PR can then restore: Maven dependencies, and the
dp-grpc build if it is ever cached.

Concurrency (#213 item 5, CI half):

```yaml
concurrency:
  group: ci-${{ github.event_name == 'pull_request' && github.ref || github.sha }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

A new push to a PR cancels that PR's superseded run. Two details are load-bearing:

- **PR runs key on `github.ref`** (`refs/pull/<n>/merge`), which is unique per PR. `github.head_ref`
  is only the source branch name, so two fork PRs from same-named branches would share a group and
  cancel each other's runs.
- **Push runs key on the commit SHA, so they never share a group.** `cancel-in-progress: false` is
  not enough on its own: a group holds one running and one *pending* run, and a newer pending run
  replaces an older one. Keyed on `github.ref`, three merges landing during one run would leave the
  middle commit untested, which is exactly the combined-merge breakage this trigger exists to catch.
  Each `main` run is the only check of the commit it tests.

### D4 — Keep `mvn verify` at release time (resolves #213 item 2 as "won't do")

#213 item 2 proposes that the release skip the suite and rely on CI instead. After this ticket,
that trade looks different:

- **CI and the release test different dp-grpc code** (see Other facts). The release run is the
  only test of the dp-grpc tag the release actually ships against. Under #221 it is also the build
  whose jar is signed.
- **The cost goes away with D1.** A re-run of a few minutes is cheap insurance.
- **Most of the "public tag, no release" risk is handled by #221's `workflow_dispatch` rehearsal,**
  which runs the same build before the tag exists.

Item 2's remaining concern, a slow failure after the tag is already public, is covered by #213
item 7 (D6) and the rehearsal. So item 2 is closed as "won't do", with this reasoning recorded
here.

### D5 — The vendored jhdf5 is installed on every build path

- `release.yml`: add the same `install:install-file` step as `ci.yml`, before the dp-service build.
- `release-image.yml`: the same step, before the runner-side `mvn -B test`.
- `Dockerfile`: run `mvn -B install:install-file -Dfile=third-party/cisd-jhdf5/… -DpomFile=…`
  in the builder stage, after `COPY . /build/app` and before `mvn -B -DskipTests package`.
- CLAUDE.md's jhdf5 section: replace "a CI step" with the full list of places the jar is installed.
  The next person to add a build path then knows it needs the step too.

*Rejected:* a Maven `file://` repository under `third-party/` declared in the POM. That would cover
every build path automatically, but it changes how every developer's local build resolves the
dependency. The install step already works in `ci.yml` and is documented.

### D6 — The mechanical items from #213

- **Item 4:** `ci.yml` drops `packages: write` and keeps `contents: read`.
- **Item 6:** `docker compose -f docker-compose.yaml up -d --wait` replaces all three polling loops.
  The existing timeout logic is replaced by the healthcheck's own `retries`/`start_period`, plus
  `--wait-timeout 120`, so a stuck container still fails in bounded time. The timeout must stay
  above the time the healthcheck needs to reach a verdict, `start_period + interval × retries`
  (10 s + 10 s × 5 = 60 s in `docker-compose.yaml:21-26`). Below that, a slow start that would
  have turned healthy fails on the timeout instead, and the error reads as a hang rather than as an
  unhealthy container. Whoever changes the healthcheck should re-check the timeout. The first
  healthy result arrives about one `interval` (10 s) after start, so a healthy run waits no longer
  than it does with the current 2 s loop. On failure,
  `docker compose logs mongodb` still runs, because that is what makes the failure diagnosable.
- **Item 7:** a step before any build that runs
  `git ls-remote --exit-code https://github.com/osprey-dcs/dp-grpc.git refs/tags/<ref>` and fails
  with a message naming the missing tag and the dependency order (dp-grpc is tagged first). In
  #221's `release.yml` this goes in the dp-grpc ref resolution step (#221 D6), for releases only.
- **Dead steps** (triage 6): delete the two "Cache Docker layers" steps and `IMAGE_NAME`.
- **Rename the CI step** to what it runs: "Unit and integration tests (`mvn verify`)".
- **Upload the test reports.** Add `target/surefire-reports` and `target/failsafe-reports` to the
  uploaded artifact, `if: always()`. With ITs gating merges, "which IT failed and why" has to be
  answerable from the PR.

### D7 — Make `build-and-test` a required status check on `main`

Add a `required_status_checks` rule to the existing `main` ruleset (`15223477`), requiring the
`build-and-test` check from the `CI` workflow. Without it, D2 makes the suite run on every PR but
not gate anything (triage 8). Add it to the existing ruleset rather than creating a second one, so
the rules governing `main` stay in one place.

- **Enable it only after the `verify` switch has run green on `main`.** Required first, a PR that
  predates the switch still reports the `mvn test` result under the same check name, and a flaky
  IT found during Task 2's watch period would block every open PR before it can be fixed.
- **The job id is the contract.** The rule matches the check by name, which is the job id
  `build-and-test`. D6 renames the *step*, which is safe; renaming the *job* would leave the rule
  waiting on a check that never reports, and every PR would sit pending. CLAUDE.md records this
  (Task 6).
- **Leave "require branches to be up to date" off.** The `main` push run (D3) already catches
  combined-merge breakage after the fact, and requiring up-to-date branches would force a rebase
  and a full re-run of every open PR after each merge.

*Rejected:* classic branch protection. The repo already governs `main` through a ruleset, and
splitting the rules across two mechanisms makes it harder to see what applies.

## Implementation tasks

### Task 1 — Poll timeout (`QueueHandlerBase`)

Replace `POLL_TIMEOUT_SECONDS = 1` with a millisecond constant (100), and use it at `:125`. Add a
comment on the constant saying it bounds how long an idle worker delays `fini()`, why that matters
(per-test shutdown in the ITs, and process shutdown), and why an interrupt or a sentinel is not
used instead (D1). Update the worker excerpt in `README.md:120-122`, which repeats the
`poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS)` line, and grep the repo for any other reference to
`POLL_TIMEOUT_SECONDS`.

### Task 2 — Measure, then make CI run `verify`

Run the full suite locally before and after Task 1 and put both totals in the PR. Then, in
`ci.yml`: switch to `mvn -B verify`, rename the step, and upload the Surefire/Failsafe reports
(D6). **The first CI run on this PR is the real measurement**, because it is the first time
integration tests run under `ci.yml`'s environment. Record its duration in the PR description.

Watch for flakiness in the first several runs. The ITs have passed only at release time so far,
and a PR runner is shared. Any test that fails intermittently gets fixed or a follow-up ticket in
this PR. It must not be excluded from the gate without saying so.

### Task 3 — `ci.yml` triggers, permissions, cleanup

Add the `push` trigger and `concurrency` (D3). Drop `packages: write`. Replace the wait loop with
`--wait` and delete the dead cache step and `IMAGE_NAME` (D6).

### Task 4 — Vendored jhdf5 on every build path (D5)

Add the install step to `release.yml`, `release-image.yml` (before its runner-side `mvn -B test`),
and the `Dockerfile`. Test the Dockerfile change with a
`release-image.yml` dry-run dispatch. To show it no longer depends on SciJava, build the image
locally once with `maven.scijava.org` unreachable (for example `--add-host maven.scijava.org:127.0.0.1`)
and confirm it succeeds. Update CLAUDE.md's jhdf5 section.

*Found during implementation:* that test failed on `cisd:base:18.09.0`, not jhdf5. It is declared
directly in `pom.xml`, jhdf5's POM depends on it, and it is also SciJava-only (404 on Central). It
kept resolving during the 2026-08-27 outage, so the original vendoring missed it. It is vendored
in `third-party/cisd-base/`, and every build path runs `third-party/install-vendored.sh` rather
than a per-path copy of the install commands.

### Task 5 — Release workflow items (#213 items 6, 7)

Apply `--wait` to `release.yml` and `release-image.yml`, and add the dp-grpc tag existence check to
`release.yml` (D6). These edit the workflows #221 is restructuring, so they land **after** #221's
two PRs and apply to the new job layout.

### Task 6 — CLAUDE.md

Rewrite the "Continuous Integration" section to match reality: triggers (PRs and `main`),
MongoDB via `docker compose up --wait`, the full `verify` gate, the uploaded artifacts, and the
concurrency rules. Add an invariant paragraph for D1: the poll timeout is what bounds per-test
teardown, and a slow suite should be checked for this first. Add a line to "Testing Strategy" noting that the ITs gate every merge.
Record D7's constraint: the ruleset requires the check by the job id `build-and-test`, so
renaming that job must update the ruleset in the same change.

### Task 7 — Require the check (D7)

After PR (a) has merged and its `main` push run is green, add the `required_status_checks` rule to
ruleset `15223477` requiring `build-and-test`. This is a repository settings change, made by a
repo admin in the ruleset UI or with `gh api --method PUT repos/osprey-dcs/dp-service/rulesets/15223477`
(the PUT replaces the full rule list, so include the three existing rules). Confirm it on the next
PR: its merge button must be blocked while `build-and-test` is pending. Record the date in #250
when it is enabled.

## Out of scope

- **#213 item 3** (image trigger to `rel-*`), **item 8** (signed `SHA256SUMS`), **item 5
  (release half)** (`concurrency` on release workflows): owned by #221.
- **#213 item 1** (stale action pins): already done by Dependabot (`.github/dependabot.yml`).
- **#213 item 2** (skip the suite at tag time): decided against. See D4.
- **The one-shutdown-hook-per-`init()` in `QueueHandlerBase`.** Each IT test registers four JVM
  shutdown hooks that keep their handlers reachable until the Failsafe JVM exits. It is harmless at
  344 tests and unrelated to run time. Worth a look only if the IT JVM's memory becomes a problem.
- **Running ITs in parallel** (Failsafe `forkCount`). Every class shares the `dp-test` database, so
  it would need per-fork databases first. It is unnecessary after D1.
- **Pinning CI's dp-grpc ref to the POM's `dp-grpc.version`.** CI builds against dp-grpc `main`
  deliberately (see Other facts).

## Dependencies and sequencing

- **Tasks 1–3 (the suite on every PR) have no dependencies.** They touch only `QueueHandlerBase`,
  `README.md`, `ci.yml`, and CLAUDE.md, none of which #221 edits, so they can land before, during,
  or after #221. Do them first, and Task 7 right after: every later PR, #221's included, then
  merges behind the integration suite.
- **Task 4 also has no dependencies, but should land before the next `rel-*` tag.** Otherwise the
  next release depends on SciJava being up. It is a one-step addition to each file, so it rebases
  easily onto #221's restructure if #221 lands first. #221 Task 7 also edits the `Dockerfile`'s
  builder stage (it replaces the dp-grpc `git clone … || git clone …` fallback). The two edits are
  a few lines apart, so whichever lands second rebases by hand.
- **Task 5 waits for #221.** It edits step bodies inside #221's new job layout.
- **Task 7 waits for PR (a) and one green `main` run**, and for the first few PR runs under
  `verify` to show no flakiness (Task 2). It does not wait for Tasks 4 or 5, or for #221. Until it
  lands, the suite is visible on every PR but not enforced.
- **Suggested PRs:** (a) Tasks 1–3 plus the matching CLAUDE.md changes; (b) Task 4; (c) Task 5
  after #221. Task 7 is a settings change, not a PR.
