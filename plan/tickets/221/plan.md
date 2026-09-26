# dp-service #221: Sign release artifacts and the container image with keyless Sigstore

- **Ticket**: [osprey-dcs/dp-service#221](https://github.com/osprey-dcs/dp-service/issues/221)
- **Reference implementation**: [osprey-dcs/dp-grpc#137](https://github.com/osprey-dcs/dp-grpc/issues/137),
  merged in dp-grpc PR #155 (workflow) and PR #156 (`NEXT.md` release notes). Its
  [`plan/tickets/137/plan.md`](https://github.com/osprey-dcs/dp-grpc/blob/main/plan/tickets/137/plan.md)
  and `.github/workflows/release.yml` are the verified shape this plan ports. dp-grpc decision
  numbers are cited as **grpc-D*n***.
- **Sibling**: [osprey-dcs/dp-desktop-app#24](https://github.com/osprey-dcs/dp-desktop-app/issues/24),
  independent.
- **Overlaps**: [#213](https://github.com/osprey-dcs/dp-service/issues/213), now closed and split
  between this ticket and [#250](https://github.com/osprey-dcs/dp-service/issues/250). See triage finding 2.
- **Status**: triaged and planned 2026-09-23. PR 1 (Tasks 1–6) implemented and rehearsed
  2026-09-24, merged as #298. PR 2 (Tasks 7–8) implemented 2026-09-24 and rehearsed 2026-09-25.

## Overview

The dp-service release page carries `dp-service-<version>.jar` and a `.sha256` written by the same
job, with the same token, to the same page. The checksum proves integrity, not origin. The
container image `ghcr.io/osprey-dcs/dp-service` has neither.

This ticket delivers:

1. **A signed release page.** One `SHA256SUMS` signed with `cosign sign-blob`, replacing the
   per-jar `.sha256`. Signing binds the jar to the repo, workflow, tag, and source commit.
2. **A signed container image.** `cosign sign` by digest in `release-image.yml`. The signature is
   stored in the registry beside the image, so `cosign verify` finds it with no extra file and an
   admission controller can enforce it.
3. **Verification instructions** in `README.env`, and in the release notes, which are the release
   body.

It is for anyone who downloads the jar from the release page or pulls the image, and for the
customer's supply-chain review. dp-grpc already signs, dp-python-lib signs, and dp-desktop-app#24
is pending.

## Background: triage findings

Each premise in the ticket was checked against `main` (`44901a8`) and the published `rel-1.16.0`
on 2026-09-23. Most hold. Six findings change the scope or the design.

### What holds

| Ticket claim | Verified |
|---|---|
| `release.yml` is one job with `contents: write` throughout | Yes: `release.yml:8-12`, single `build-and-release` job, no `id-token` |
| That job runs `mvn clean verify` with MongoDB | Yes: `release.yml:77-102`. The `rel-1.16.0` run took 27m34s |
| One jar plus one `.sha256` is published | Yes: `rel-1.16.0` has exactly `dp-service-1.16.0.jar` and `dp-service-1.16.0.jar.sha256` |
| Nothing is published to a Maven repository | Yes: `pom.xml` has no `distributionManagement`, `maven-deploy-plugin`, or `maven-gpg-plugin` |
| dp-grpc is consumed by a source build at the matching tag | Yes: `release.yml:64-75` |
| `sigstore/cosign-installer@6f9f177…` is v4.1.2 | Yes: tag `v4.1.2` → `6f9f17788090df1f26f669e9d70d6ae9567deba6`. Still the latest release |
| `release-image.yml` pushes `:rel-X.Y.Z` and `:latest` and has a `dry_run` gate | Yes: `release-image.yml:26-30, 52, 185-191` |
| The notes check already runs before the build | Yes: `release.yml:46` vs `:102`. dp-grpc had to move it (grpc-D8); here it only needs a release-only guard |

### 1. The target version is stale

The ticket targets `rel-1.16.0`. That release shipped unsigned on 2026-09-16. As in dp-grpc#137
(grpc-D6), this now targets the next release. Do not re-cut 1.16.0.

### 2. "Distinct from #213 … No overlap" is wrong

When #221 was triaged, #213 was open. Three of its items cover this ticket's work directly:

- **Item 8** proposes a single `SHA256SUMS` and Sigstore signing. That is this ticket.
- **Item 3**: `release-image.yml` triggers on every tag (`tags: '*'`, `release-image.yml:4-6`), and
  every run moves `:latest`. This matters more once images are signed (finding 4).
- **Item 5**: no `concurrency` group. It asks for `cancel-in-progress: false` on the release
  workflow, which the dp-grpc shape already includes.

**Resolution:** #221 absorbs item 8, item 3, and the release-workflow half of item 5. #213 was
then closed as superseded (2026-09-23). Items 4, 6, 7, and the CI half of 5 moved to #250. Item 2
(skip the suite at tag time) was declined there (#250 plan D4). Item 1 (stale pins) was already done
by Dependabot (`.github/dependabot.yml`).

### 3. The published checksum is broken, as it was in dp-grpc

`release.yml:120-123` runs `sha256sum release/dp-service-${VERSION}.jar` from the repo root.
The published `rel-1.16.0` checksum is:

```
c8ca12d6…fdee6  release/dp-service-1.16.0.jar
```

The jar lands in a flat download directory, so `sha256sum -c` fails with "No such file or
directory". `README.env:34` documents that exact command. dp-grpc had the same defect and fixed it
by generating from inside `release/`. Port that fix, not the old line.

### 4. The image workflow tells a release apart by ref type, the mistake dp-grpc#137 review caught

In dp-grpc, Copilot's review of PR #155 found that a `workflow_dispatch` can target a tag, so
`GITHUB_REF_TYPE == tag` does not mean "this is a release." dp-grpc now defines `IS_RELEASE` once
from the event (commit `7f28923`). `release-image.yml` has the same mistake twice:

- `:latest` is pushed when `github.ref_type == 'tag'` (`release-image.yml:191`). A dispatch against
  any tag with `dry_run: false` moves `:latest`.
- `IMAGE_TAG` falls back on `github.ref_type == 'tag'` (`:47`).

Combined with the `'*'` trigger (finding 2), this makes dp-grpc's documented rehearsal workaround
dangerous here. dp-grpc's `release.yml` says to rehearse against "a fresh throwaway tag". In
dp-service, **pushing any tag, throwaway or not, runs `release-image.yml` as a real publish.** It
pushes the image, moves `:latest`, and after this ticket, signs it. Until the trigger is narrowed,
never push a throwaway tag in this repo.

### 5. The image digest is not exposed yet

The ticket says "the `push` step already emits [a digest]." `docker/build-push-action` does output
`digest`, but the step at `release-image.yml:177` has no `id`, so nothing can read it. This is a
small fix.

### 6. The image's dp-grpc ref is neither strict on release nor tag-first on dispatch

`release-image.yml:83-100` resolves the dp-grpc ref differently from `release.yml`, in both
directions:

- **On a dispatch it never reaches the POM-derived tag.** `dp_grpc_ref` defaults to `'main'`
  (`:21`), so `CANDIDATE` is `main`, which always exists, and the `rel-$WANT` branch is dead code
  unless the caller explicitly clears the input.
- **On a release it is lenient where `release.yml` is strict.** On a tag push `CANDIDATE` is the
  tag name. If dp-grpc has no matching `rel-X`, the resolver falls back to `rel-$WANT` and then to
  `main`, and the image is built and published anyway. `release.yml` fails that case, which is the
  dependency-order check.

Behind the resolver, the `Dockerfile` has a second silent fallback: `git clone --depth 1 --branch
"$DP_GRPC_REF" … || git clone …` builds dp-grpc's default branch whenever the first clone fails.
`--branch` accepts only branch and tag names, so it also fails, and falls back, for any commit
SHA. The image and the jar can therefore be built against different dp-grpc source with nothing
in either log flagging it as an error. D2 and D6 close both gaps.

### Other facts the design depends on

- **The image does not contain the release-page jar.** The `Dockerfile` rebuilds dp-service from
  source with `-DskipTests`, inside BuildKit, against a dp-grpc ref that `release-image.yml`
  resolves on its own (`:83-100`). The two signatures attest two separate builds. See Out of scope.
- **A rehearsal cannot reuse the release's dp-grpc ref.** The release checks out dp-grpc at
  `rel-${VERSION}` (`release.yml:69`). A dispatch has no `rel-` tag.
- **A dispatch on the pre-change `release.yml` is impossible.** dp-grpc found that GitHub checks the
  `workflow_dispatch` trigger against the workflow file at the target ref (HTTP 422 otherwise), so
  no existing tag can be rehearsed. dp-grpc's commit `24372b1` also says the run then executes the
  default branch's copy. That contradicts GitHub's documentation, which says the run uses the
  target ref's copy. The difference decides whether a pre-merge rehearsal tests the PR's workflow,
  so settle it in the rehearsal (Task 4). Do not carry the claim forward.
  **Settled 2026-09-24 (Task 4):** the run executes the target ref's copy. The dispatch against
  the PR 1 branch (run `36067531985`) ran the branch's `build`/`sign`/`publish` jobs, not
  `main`'s single `build-and-release`. GitHub's documentation is right; dp-grpc's `24372b1`
  comment is wrong.

## Design decisions

These are carried over from dp-grpc#137 unchanged: Sigstore rather than GPG (grpc-D1), `cosign`
rather than the Python action (grpc-D2), one signed `SHA256SUMS` (grpc-D3), a `workflow_dispatch`
rehearsal that cannot publish (grpc-D5), the asset rename called out in the notes (grpc-D7), and
notes as the release body with verification in `README.env` (grpc-D9, confirmed on this ticket
2026-09-23). Their reasoning is in the dp-grpc plan and is not repeated here. The decisions below
are the ones specific to dp-service.

### D1 — Three jobs, not two: the OIDC token never shares a job with project code

dp-grpc splits build-and-sign (`id-token: write`) from publish (`contents: write`). The ticket
argues for the split because the single job runs `mvn clean verify`, which executes project code.
But the two-job sketch keeps `mvn clean verify` in the job that holds `id-token: write`, so it
misses its own point.

When a job has `id-token: write`, the runner puts `ACTIONS_ID_TOKEN_REQUEST_URL` and
`ACTIONS_ID_TOKEN_REQUEST_TOKEN` in the environment of **every step**. In dp-service that step
list includes a 28-minute test suite, every Maven plugin and test dependency, and a dp-grpc build
from another repo. Any of them could mint a Sigstore certificate as
`release.yml@refs/tags/rel-X` and sign bytes of its choosing.

So dp-service uses three jobs:

| Job | Permissions | Runs |
|---|---|---|
| `build` | `contents: read` | checkout, version, notes check, dp-grpc, MongoDB, `mvn clean verify`, prepare jar, stage notes, upload |
| `sign` | `contents: read`, `id-token: write` | download, `SHA256SUMS`, cosign, upload signatures. **No checkout, no project code** |
| `publish` | `contents: write` | download both artifacts, `action-gh-release`. Release-only gate |

**The limit, stated so it is not oversold:** code that runs in `build` can still tamper with the
jar before `sign` checksums it. No job split prevents that. The split does two things. The signing
identity cannot be used on anything except what `build` handed over. And the token is in scope for
seconds, not half an hour. The cost is one short job and one extra artifact hop.

*Rejected:* dp-grpc's two-job shape copied exactly. That would be consistent across repos, but it
keeps the exposure the ticket names as its reason for splitting. dp-grpc runs only
`mvn -B package` in its signing job, so its exposure is smaller, but it is the same kind.
Back-porting three jobs to dp-grpc and dp-desktop-app is a follow-on, not a blocker.

### D2 — The image workflow uses the same rule: tests run apart from the push-and-sign job

`release-image.yml` already runs `mvn test` in the job holding `packages: write`, and signing
would add `id-token: write`. Split it:

| Job | Permissions | Runs |
|---|---|---|
| `test` | `contents: read` | checkout, resolve dp-grpc ref to a commit, dp-grpc, MongoDB, `mvn test`; outputs both commits |
| `publish-image` | `contents: read`, `packages: write`, `id-token: write` | checkout **at `test`'s commit**, login, build-push with `test`'s dp-grpc commit, `cosign sign` |

`publish-image` needs `packages: write` and `id-token: write` together, because `cosign sign`
writes the signature to the registry. That is acceptable because `publish-image` runs no project
code on the runner. The Maven build in the `Dockerfile` runs inside BuildKit, which does not
inherit the runner's environment, so it never sees the request token. The rehearsal should confirm
this: a `RUN env` in a throwaway Dockerfile must not show `ACTIONS_ID_TOKEN_*`.

`publish-image` `needs: test`, so an image is never pushed from a commit whose tests failed. That is
how the current `if: success()` behaves today.

**The source must be pinned across the split, or `needs: test` gates the wrong thing.** Today's
single job checks out once, so tests and image see the same source. Two jobs each check out a ref
*name*. On a dispatch against a branch, the branch can move between `test` and `publish-image`. If
each job resolved dp-grpc on its own, `main` could move too. `needs: test` would then pass for
source other than what gets built, pushed, and signed. So:

- `test` outputs `commit` (`git rev-parse HEAD` after checkout) and `dp_grpc_commit` (the resolved
  dp-grpc ref, dereferenced to a commit with `git ls-remote`, logged next to the ref name it came
  from). For a tag, use the peeled `refs/tags/<tag>^{}` line. An annotated tag's own line is the
  tag object's SHA, not the commit's.
- `publish-image` checks out `needs.test.outputs.commit` and passes
  `needs.test.outputs.dp_grpc_commit` as the `DP_GRPC_REF` build arg. It does not resolve anything
  itself.

**That pulls one `Dockerfile` change into scope.** `git clone --branch <sha>` fails, and the
`|| git clone …` fallback would then silently build dp-grpc `main` (triage finding 6). The clone
becomes `git init` + `git fetch --depth 1 origin "$DP_GRPC_REF"` + `git checkout FETCH_HEAD`,
with **no fallback**. GitHub serves fetch-by-SHA for reachable commits. A local `docker build`
that passes a branch or tag name still works, because `fetch` accepts names too. An unresolvable
ref now fails the build instead of changing what it builds. That fallback is exactly what the
image signature would otherwise vouch for without knowing it.

**What `needs: test` does and does not prove.** It proves the source commit passed `mvn test`. It
does not prove the image was tested. The image is a separate `-DskipTests` build inside BuildKit,
as it is today. The workflow comments should say "gated on the source's tests", not "tested
image".

The `Cache Docker layers` step (`release-image.yml:74-81`) is dropped rather than carried into
either job. `build-push-action` has no `cache-from`/`cache-to`, so the restored directory is never
read, and a cache restore has no place in the job that holds `id-token: write`.

*Rejected:* adding `id-token: write` to the existing single job. It is the smallest diff, but it
puts both the signing token and the registry-write token in scope during `mvn test`.

### D3 — `IS_RELEASE` is one expression per workflow, from the event, as in dp-grpc

Each workflow uses one expression to say whether a run is a real release. It is the same
expression the publish gate uses. In `release.yml` the text has to appear twice: as the `build`
job's `IS_RELEASE` env, and literally in `publish.if`, because a job-level `if:` cannot read the
`env` context. That repetition is deliberate. The alternative that avoids it, a `build` job output
consumed as `needs.build.outputs.is_release`, would let the job that runs project code decide
whether publishing happens. The gate must depend only on the event. A comment at each copy names
the other one.

- `release.yml`: `github.event_name == 'push' && startsWith(github.ref, 'refs/tags/rel-')`.
  Version derivation, the notes check, notes staging, and the `publish` job all key off it.
- `release-image.yml`: the same expression. `:latest` is pushed **only** when `IS_RELEASE`, not on
  `github.ref_type == 'tag'`, and `IMAGE_TAG`'s tag-name fallback keys off it too. `DRY_RUN` keeps
  its current meaning, and signing is gated on `DRY_RUN != 'true'`, the same gate as the push.
  The `:latest` and `IMAGE_TAG` changes are hazard fixes that stand on their own without signing,
  so they ship in PR 1 (Task 2). Signing reuses the expression in PR 2.

A non-dry-run dispatch therefore pushes **and signs** an image from a branch. That is deliberate.
Every pushed image is signed, and the signing identity records the ref it came from. The
verification identity in `README.env` (D5) accepts only `refs/tags/rel-`, so a signature from a
branch or dispatch run is real but fails the published check. That is the intended result.

### D4 — Narrow `release-image.yml` to `rel-*` tags (from #213 item 3)

The trigger becomes `tags: ['rel-*']`, matching `release.yml`. The reasons are:

- It removes the throwaway-tag hazard in triage finding 4 for every commit from PR 1 on. That hazard
  is independent of signing, so it lands in PR 1 (Task 2), not with image signing. It is removed
  only going forward: a push runs the workflow file at the tagged commit, so a tag on an older
  commit still publishes under the old `'*'` trigger.
- Once images are signed, the published verification identity is pinned to `refs/tags/rel-`. An
  image from a `v1.14`-style tag push would be signed, pushed, and moved to `:latest`, and would
  then fail verification.

The existing `v1.12` and `v1.13` tags are not re-run by this change. It only affects future
pushes. Anyone who relied on a non-`rel` tag producing an image loses that in PR 1, and `NEXT.md`
says so.

### D5 — Two verification identities, one per workflow file

The jar is signed by `release.yml`. The image is signed by `release-image.yml`. They share a
repository, but their certificate identities differ:

```
^https://github.com/osprey-dcs/dp-service/\.github/workflows/release\.yml@refs/tags/rel-
^https://github.com/osprey-dcs/dp-service/\.github/workflows/release-image\.yml@refs/tags/rel-
```

This is the copy-paste hazard the ticket comment warns about across three repos, now happening
inside one repo. A wrong pattern either fails every legitimate verification, or, if loosened to
get past that, proves nothing. `README.env` gives each command under its own heading. Task 4 and
Task 7 check **both** patterns against real signatures, including the failing case.

**Amended in #298's review (2026-09-24).** The published commands do not use the open-ended
`rel-` patterns above. Two gaps:

- **Cross-version substitution.** A pattern accepting any `rel-` tag verifies an older release's
  genuine `SHA256SUMS` and bundle substituted for a newer one's. The published command is therefore
  an exact `--certificate-identity …/release.yml@refs/tags/rel-<version>`, and the reader fills in
  the version they downloaded.
- **Dispatch against a tag.** Every tag from PR 1 on carries the `workflow_dispatch` trigger, so a
  dispatch whose `--ref` is `rel-<version>` signs under the release's own identity. The only
  certificate field that differs is the workflow trigger. The published command adds
  `--certificate-github-workflow-trigger push`, and `release.yml` refuses a dispatch against a tag
  ref as well. Verified locally with cosign v3.1.3 against the Task 4 rehearsal bundle: with the
  exact branch identity it passes; adding `--certificate-github-workflow-trigger push` fails with
  `expected GithubWorkflowTrigger to be "push", got "workflow_dispatch"`.

PR 2 owes the image command the same form: an exact `release-image.yml@refs/tags/rel-<version>`
identity plus the trigger flag. A non-dry-run dispatch of `release-image.yml` against a tag would
otherwise produce a signed image that passes the published check (see D3).

### D6 — Rehearsal version and dp-grpc ref come from the POM

As in dp-grpc, a rehearsal takes `VERSION` from `project.version`. Otherwise `${GITHUB_REF_NAME#rel-}`
gives `main`, and the rehearsal builds `dp-service-main.jar`, a filename no release uses.

On a rehearsal, dp-grpc is checked out at `rel-<dp-grpc.version>` if that tag exists, and at `main`
if it does not. The fallback is logged. A **release** keeps the strict `rel-${VERSION}` checkout.
Failing there when dp-grpc has not been tagged is the dependency-order check, so do not soften it.
An explicit up-front existence check for that ref belongs to #250 (from #213 item 7).

`release-image.yml` does **not** already resolve this way, contrary to this plan's first draft
(triage finding 6). PR 2 brings its resolver, which moves into `test` (D2), to the same rules:

- **Release:** strictly `rel-${VERSION}`. No fallback to `rel-$WANT` or `main`, so the image and
  the jar are both built against the dp-grpc release their tag names, or neither is published.
- **Dispatch:** an explicit `dp_grpc_ref` if the caller gave one (still an error if it does not
  exist), otherwise `rel-<dp-grpc.version>`, otherwise `main`, logged. The input's default changes
  from `'main'` to `''`. That is what makes the POM-derived tag reachable, and a caller who wants
  `main` can still type it.

### D7 — Release-note content goes in `NEXT.md`, and dp-service adopts that convention

dp-grpc PR #155's first draft added `doc/release-notes/rel-1.17.0.md` and had to back it out.
`release.yml` resolves the notes path strictly from `GITHUB_REF_NAME`, so a file with a guessed
version is stranded, and it asserted contents the release had not settled. dp-grpc PR #156 then
adopted a version-less `doc/release-notes/NEXT.md`. Sections accumulate in it as tickets land, and
it is renamed at cut time.

dp-service has the same notes mechanism and no such file. This ticket adopts the convention and
seeds `NEXT.md` with #221's section, plus the "Cutting the release" checklist taken from dp-grpc
and adapted to this repo. CLAUDE.md "Releases" records the convention.

*Rejected:* a `plan/tickets/221/release-note-fragment.md` pasted in at release time. dp-grpc tried
that and retired it within a day, because it depends on someone remembering to paste it.

## Target workflow shapes

### `release.yml`

Every `uses:` is SHA-pinned. The `upload-artifact`/`download-artifact` pins are dp-grpc's, and
each must be re-resolved against its tag when implementing.

```yaml
on:
  push:
    tags: ['rel-*']
  workflow_dispatch:        # rehearsal; cannot publish (see publish.if)

concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false

permissions:
  contents: read

jobs:
  build:
    permissions: { contents: read }
    env:
      IS_RELEASE: ${{ github.event_name == 'push' && startsWith(github.ref, 'refs/tags/rel-') }}
    steps:
      # checkout; setup-java
      # Derive version: tag on release, POM on rehearsal (D6)
      # Verify release notes exist: if IS_RELEASE (unchanged otherwise)
      # Resolve dp-grpc ref: rel-${VERSION} on release; POM-derived with fallback on rehearsal (D6)
      # checkout dp-grpc; mvn install; MongoDB up + wait; mvn clean verify; tear down (always)
      # Prepare release artifacts; verify they exist
      # Stage notes: real file on release, placeholder on rehearsal (dp-grpc pattern)
      # upload-artifact "build-outputs": jar + RELEASE_NOTES.md, if-no-files-found: error

  sign:
    needs: build
    permissions: { contents: read, id-token: write }
    steps:
      # download "build-outputs" into release/ -- NO checkout
      # Generate SHA256SUMS, working-directory: release (bare filenames; triage finding 3)
      # cosign-installer; cosign sign-blob --yes --bundle SHA256SUMS.cosign.bundle SHA256SUMS
      # upload-artifact "signatures": SHA256SUMS + bundle, if-no-files-found: error

  publish:
    needs: [build, sign]
    if: github.event_name == 'push' && startsWith(github.ref, 'refs/tags/rel-')
    permissions: { contents: write }
    steps:
      # download both artifacts into release/
      # action-gh-release: jar, SHA256SUMS, bundle; body_path release/RELEASE_NOTES.md;
      #   fail_on_unmatched_files: true; overwrite_files: true (with dp-grpc's stale-.sha256 comment)
```

`VERSION` has to reach `sign` and `publish`. Pass it as a `build` job output, not by re-deriving it,
because a rehearsal's version comes from the POM and `sign` has no checkout to read it from.
On a rehearsal that value is repository content, and `sign` holds `id-token: write`. So `sign` and
`publish` read it into a step `env:` (`VERSION: ${{ needs.build.outputs.version }}`) and use
`"$VERSION"` in shell. They never expand `${{ needs.build.outputs.version }}` inside a `run:`
script, which would splice it into the script as code. `build` also rejects a version that doesn't
match `^[0-9A-Za-z.+-]+$` before exporting it, since it becomes a filename.

Cosign is pinned twice. The action is SHA-pinned, and the step passes `cosign-release:` explicitly
rather than taking the installer's default. `cosign-installer` v4.x installs cosign v3.x by
default, and an unpinned default can move under a Dependabot bump. The signature format and
storage that Task 4 and Task 7 observe should stay true for later runs. Both workflows pin the same
cosign version, and `README.env` states it as the version the signatures were produced with.

Published assets become:

```
dp-service-<version>.jar
SHA256SUMS
SHA256SUMS.cosign.bundle
```

### `release-image.yml`

PR 1 (Task 2) changes only the trigger, `:latest`, `IMAGE_TAG`, and `id: build` in today's single
job. The shape below is the end state after PR 2.

```yaml
on:
  push:
    tags: ['rel-*']           # D4; was '*' (PR 1)
  workflow_dispatch: { inputs: unchanged except dp_grpc_ref default '' (D6) }

concurrency:
  group: release-image-${{ github.ref }}
  cancel-in-progress: false

permissions:
  contents: read

jobs:
  test:
    permissions: { contents: read }
    outputs:
      commit: <git rev-parse HEAD after checkout>
      dp_grpc_commit: <resolved dp-grpc ref, peeled to a commit (D2, D6)>
    # checkout (same ref expression as today), setup-java, resolve dp-grpc ref (D6 rules),
    # checkout + install dp-grpc at that commit, compose up + wait, mvn test, compose down

  publish-image:
    needs: test
    permissions: { contents: read, packages: write, id-token: write }
    env:
      IS_RELEASE: <same expression as release.yml>
      DRY_RUN: <unchanged>
      IMAGE_TAG: <PR 1's form: unchanged priority order, ref_type replaced by IS_RELEASE>
    steps:
      # checkout at needs.test.outputs.commit (NOT the ref name; D2); no Docker layer cache step
      # login (if DRY_RUN != 'true')
      # build-push, `id: build`, DP_GRPC_REF=needs.test.outputs.dp_grpc_commit;
      #   :latest only when IS_RELEASE (D3)
      # cosign-installer (if DRY_RUN != 'true')
      # cosign sign --yes "ghcr.io/${{ github.repository_owner }}/dp-service@${{ steps.build.outputs.digest }}"
      #   (if DRY_RUN != 'true')
```

Sign **by digest**, never by tag: `:latest` and `:rel-X` are mutable, and one digest signature
covers every tag that points at it. Check that the digest step output is not empty before signing,
so the step fails loudly instead of signing `dp-service@`.

The diagnostic step (`:115-137`) moves to `test`, since that is where `docker-compose.yaml` is
needed. Everything else keeps its current behavior.

## Implementation tasks

Two PRs, in this order, both under #221. They are reviewed separately because the image half has
its own rehearsal constraints (Task 7). PR 1 also carries the `release-image.yml` hazard fixes
(Task 2). They don't depend on signing, they're a few lines, and landing them first is what makes
a release with PR 1 alone a coherent state (see Dependencies and sequencing).

### PR 1 — release-page signing, and the image workflow's hazard fixes

**Task 1 — `release.yml`.** Rewrite to the three-job shape above. Carry the existing steps across
unchanged in content, with four exceptions: version derivation (D6), the notes guard (D3),
checksum generation from inside `release/` (triage finding 3), and dp-grpc ref resolution (D6).
Add `set -euo pipefail` to multi-line `run` steps as dp-grpc did. This is a new convention, not an
existing one being matched. Copy dp-grpc's workflow comments where the reasoning carries over
(rehearsal trigger, `IS_RELEASE`, `working-directory`, `fail_on_unmatched_files`,
`overwrite_files`). Fix the rehearsal-trigger comment according to Task 4's finding on which
workflow copy a dispatch runs.

**Task 2 — `release-image.yml` hazard fixes.** These are edits to today's single job. The job split
and signing wait for PR 2:

- trigger `tags: ['*']` → `tags: ['rel-*']` (D4)
- add a job-level `IS_RELEASE` with the D3 expression
- `:latest` pushed only when `IS_RELEASE` (was `github.ref_type == 'tag'`, `:191`)
- `IMAGE_TAG`'s tag-name fallback keyed off `IS_RELEASE` (was `github.ref_type == 'tag'`, `:47`)
- `id: build` on the build-push step, so PR 2 can read the digest
- `concurrency: { group: release-image-${{ github.ref }}, cancel-in-progress: false }`

~~Leave the dp-grpc resolver's `github.ref_type == 'tag'` alone for now.~~ Changed in #298's
review: the resolver's tag-name candidate keys off `IS_RELEASE` too, and `inputs.dp_grpc_ref` (and
the diagnostic step's `github.ref_name`) reach their scripts through `env:` instead of being
spliced in. A dispatch against a tag now falls through to the POM's `rel-<dp-grpc.version>`, which
is the same ref for a `rel-*` tag. PR 2 may still rewrite the resolver (D6).

Rehearse with a **dry-run** dispatch against the PR branch. That is enough, because every change
here shows in the run log without a push. Confirm the build-push step's resolved tag list has no
`:latest`, and that `IMAGE_TAG` is the commit SHA, or `inputs.tag` when given. The case the old
check got wrong, a dispatch whose `--ref` *is* a tag, can't be rehearsed yet. Every existing tag
carries the old workflow file, and a dispatch runs the file at its ref. So that case rests on the
expression itself: `IS_RELEASE` requires `event_name == 'push'`, which a dispatch never has.

**Task 3 — `README.env`.** Replace the asset list and step 2. Port dp-grpc's text, adapted: a
single artifact, so `--ignore-missing` becomes a short note rather than a paragraph. Add what
`SHA256SUMS` does and does not cover, the cosign install pointer, the `release.yml` identity (D5),
and the "keep the regexp exactly as written" warning. Add a line saying releases through 1.16.0
shipped an unsigned `.sha256` with a `release/`-prefixed path. **Do not add the image section yet**;
that ships in PR 2 with the workflow that makes it true.

**Task 4 — Rehearse.** Run `gh workflow run release.yml --ref issue-221-...` against the PR branch.
If GitHub refuses (422) or the run executes `main`'s copy (compare the job list), record which one
happened. If so, rehearse against `main` after merge. GitHub's documentation also requires a
`workflow_dispatch` workflow to exist on the default branch. `release.yml` does, so this shouldn't
bite, but if the dispatch is refused, record which requirement caused it. Never push a tag to
rehearse (triage finding 4). Confirm:

- all three jobs run, and `publish` is **skipped**
- `sign` has no checkout step, and its log shows no `mvn`
- `VERSION` is the POM version, and the dp-grpc ref resolution is logged
- the downloaded `SHA256SUMS` has bare filenames and `sha256sum -c` passes in a flat directory
- `cosign verify-blob` **passes** with `…/release\.yml@refs/heads/<branch>$`
- `cosign verify-blob` **fails** with the published `…@refs/tags/rel-` pattern. That refusal is
  what shows the tag anchor matters.
- `cosign verify-blob` **fails** with the `release-image\.yml` pattern. This is the D5 copy-paste
  case.

**Task 4 result (2026-09-24).** Rehearsal run `36067531985` against `issue-221-release-signing`
at `e038072`: dispatch accepted (no 422), branch copy executed (see triage "Other facts").
`build` and `sign` succeeded and `publish` was skipped. `sign` ran only download, `SHA256SUMS`,
install cosign, sign, upload — no checkout, no `mvn`. `VERSION` was `1.16.0` from the POM and the
log shows `dp-grpc ref: rel-1.16.0 (rehearsal: POM dp-grpc.version)`. The downloaded
`SHA256SUMS` records the bare `dp-service-1.16.0.jar` and `sha256sum -c` passes in a flat
directory. With cosign v3.1.3 locally, `verify-blob` passed with
`…/release\.yml@refs/heads/issue-221-release-signing$`, and failed with both the published
`…/release\.yml@refs/tags/rel-` pattern and the `release-image\.yml` pattern, each reporting the
actual SAN. Task 2's dry-run dispatch (`36067534812`) logged `IS_RELEASE: false`, `IMAGE_TAG` =
the commit SHA, a single tag in the build-push tag list with no `:latest`, and the registry
login skipped.

**Re-rehearsal after #298's review (2026-09-24).** Run `36069255215` at `6dd6c17`. `build` passed,
and the new "Derive version" step logged `Release version: 1.16.0 (release run: false)`. `sign`
failed on attempt 1 with `connection reset by peer` from `timestamp.sigstore.dev`, a transient
network error at Sigstore's timestamp service. Re-running only the failed job signed the same
`build` artifact without rebuilding, which is the recovery path if this happens on a real
release. `publish` was skipped. `sha256sum -c` passes. `verify-blob` passes with the exact branch
identity. Adding `--certificate-github-workflow-trigger push` makes it fail with
`got "workflow_dispatch"`, and the published `rel-1.16.0` form fails on the SAN.

**Task 5 — Adopt `NEXT.md` (D7).** Create `doc/release-notes/NEXT.md` from dp-grpc's, adapted:
dp-service title, and a "Cutting the release" checklist that keeps dp-grpc's steps and adds the
`README.md` table row that CLAUDE.md already requires. Seed it with #221's release-page section:
why, the three-job split, the asset rename, the checksum-path fix, verification, and upgrade items.
Include Task 2's image changes: only `rel-*` tags publish an image, which is a behavior change for
anyone who relied on another tag producing one, and `:latest` moves only on a release. Add one line
saying the container image is not yet signed. PR 2 replaces that line, and if PR 2 misses the
release, the line ships as written. Write it without a version number, following dp-grpc's
`NEXT.md`.

**Task 6 — CLAUDE.md "Releases".** Add a signing paragraph (what is signed, the three-job rule and
*why* it is three here, the rehearsal gate, and that a rehearsal leaves a permanent public Rekor
entry naming its ref). Add the `NEXT.md` convention and its "never name a version before the tag
exists" rule, adapted from dp-grpc's CLAUDE.md. Record that since PR 1, only a `rel-*` tag push
publishes an image. Also record the exception that keeps "never push a tag to rehearse" a
permanent rule: a push runs the workflow file **at the tagged commit**, so a tag on any commit from
before PR 1 still runs the old `'*'` trigger and the old `:latest` logic.

### PR 2 — image signing

**Task 7 — `release-image.yml` and `Dockerfile`.** Split into `test` and `publish-image` (D2),
with `test` exporting the dp-service and dp-grpc commits and `publish-image` building exactly those.
Move `IS_RELEASE` and `concurrency` to where they now belong. Rewrite the dp-grpc resolver to D6's
rules, including the input default `''`. Drop the dead Docker layer cache step. Add the empty-digest
guard, cosign install with `cosign-release:` pinned, and `cosign sign` by digest. In the
`Dockerfile`, replace the `git clone --branch … || git clone …` pair with a fetch-by-ref with no
fallback (D2). The trigger, `:latest`, `IMAGE_TAG`, and `id: build` already landed in PR 1.
Rehearse with a dispatch against the PR branch, using
`dry_run: false` and `image_tag: rehearsal-221`. Signing needs a pushed digest, so a dry run cannot
exercise it. Confirm:

- `test` runs without `packages`/`id-token`, and `publish-image` runs only after `test` passes
- `publish-image`'s checkout commit and `DP_GRPC_REF` build arg equal `test`'s outputs. Push a new
  commit to the branch while `test` is running once, to show `publish-image` still builds the
  tested commit.
- a dispatch with no `dp_grpc_ref` logs the resolved `rel-<dp-grpc.version>` (or the logged `main`
  fallback), not an unconditional `main`
- a `DP_GRPC_REF` that doesn't exist fails the image build (local `docker build` is enough)
  instead of building dp-grpc `main`
- `:latest` is **not** moved (check its digest before and after)
- the throwaway `RUN env` check from D2, done once on a scratch branch and not committed
- `cosign verify ghcr.io/osprey-dcs/dp-service@<digest>` **passes** with
  `…/release-image\.yml@refs/heads/<branch>$` and **fails** with the published `refs/tags/rel-` and
  `release\.yml` patterns
- which signature storage the pinned cosign (the `cosign-release:` version, a v3.x) used against
  ghcr: a `sha256-<digest>.sig` tag or an OCI referrer. This decides what shows up in the ghcr
  package listing, and whether an older `cosign` can find the signature. State the minimum cosign
  version in `README.env` from what was observed.

Then delete the `rehearsal-221` package version and its signature from ghcr, and say so in the PR.

A dry-run dispatch should also pass unchanged, with no login, no push, and no signing.

**Task 7 as implemented, beyond the above** (2026-09-24):

- **A publishing dispatch against a tag ref is refused** in `test`'s first step, mirroring
  `release.yml` (D5 amendment); a dry run is allowed, since it signs nothing. So is a dispatch
  `image_tag` of `latest` or `rel-*`: `IMAGE_TAG` takes the input verbatim, so a dispatch could
  otherwise overwrite `:latest` or a release's tag with an unreleased build — the D3 hazard by
  another route, and one the signature cannot undo, since the tag moves whether or not it verifies.
- **The release resolver also checks the tag against the POM** (`project.version` and
  `dp-grpc.version`), the check `release.yml` gained in #298's review, so the image cannot publish
  where the jar would refuse.
- **On a release, `test` asserts the checked-out commit is `GITHUB_SHA`**, the commit the signing
  certificate records as its source.
- **A dispatch `dp_grpc_ref` may be a full commit SHA**, used as-is; the dp-grpc checkout fails if
  it is unreachable. Before, `ls-remote` only matched tags and heads, so a SHA was rejected.
- **Both checkouts in `publish-image`'s lineage use `persist-credentials: false`.** There is no
  `.dockerignore`, so `COPY . /build/app` copies `.git` — including the checkout's persisted token
  header — into the builder stage of a job whose token can write packages. The builder stage is
  not pushed, but the token has no reason to be there.
- **Both signing steps retry once** after 30 s (`release.yml`'s `sign-blob` too), per the PR 1
  rehearsal's TSA connection reset.
- The resolver reads `dp-grpc.version` with `mvn help:evaluate`, like `release.yml`, instead of
  `sed` over `pom.xml`.

Verified locally before the rehearsal: the resolver's peel against dp-grpc for a lightweight tag
(`rel-1.16.0`), an annotated one (`beta-1.3` → its `^{}` commit, not the tag object), `main`, and
two nonexistent refs, one a prefix of a real tag (`rel-1.1`); the `Dockerfile` fetch for a SHA and
a tag; and `docker build --target builder --build-arg DP_GRPC_REF=no-such-ref` failing with
`fatal: couldn't find remote ref no-such-ref` instead of building `main`.

**#299 review fixes** (2026-09-26), from Copilot's two findings plus our own pass:

- **The image-tag refusal checked `image_tag` only**, but `IMAGE_TAG` falls back to `inputs.tag`,
  so a publishing dispatch with `tag=rel-1.16.0` (or `tag=latest`) passed the check and overwrote
  that tag. The check now covers the effective tag.
- **A publishing dispatch could build a source other than the commit its certificate names**,
  through the `ref`/`tag` inputs. Such a run signed under a branch identity the release verify
  command rejects, but anyone checking a looser policy got a false source claim. A publishing
  dispatch now refuses both inputs, and `test`'s commit assertion applies to every signing run,
  not only releases. The inputs remain for dry runs.
- **`publish-image` re-checks both** (commit is `GITHUB_SHA`; a non-release tag is not
  `latest`/`rel-*`) before logging in. It runs no project code, so the invariant no longer rests on
  an output from the job that ran Maven; `test`'s copies are only the fast failure.
- README.env shows the verify-by-digest form; the test step is renamed "Run Maven unit tests" (the
  ITs run under Failsafe, which `mvn test` does not reach — pre-existing, left for #250); the stale
  `tag=v1.11` help text in the diagnostic step is replaced; `doc/running.md` says only post-1.16.0
  images are signed.

**Rehearsal results** (2026-09-24/25):

- **Dry runs** (`36072723061` on the PR branch): both jobs pass; login, push, cosign install and
  signing are skipped. With no `dp_grpc_ref` the resolver logged `rel-1.16.0 -> cc61ec6`, and the
  `Dockerfile` fetched exactly that commit.
- **BuildKit does not see the OIDC request token** (scratch branch, run `36072761267`, deleted
  since): the `publish-image` runner had `ACTIONS_ID_TOKEN_REQUEST_{URL,TOKEN}`, and a `RUN env` in
  the builder stage printed neither.
- **Publishing rehearsal** (`36156952591`, `dry_run=false`, `image_tag=rehearsal-221`): pushed and
  signed `sha256:0d335449…7d93`; `:latest` stayed `sha256:4af258ed…d951c` (= `rel-1.16.0`).
  `cosign verify` (v3.1.3) **passed** with the exact `release-image.yml@refs/heads/<branch>`
  identity, and with `--certificate-github-workflow-trigger workflow_dispatch`; it **failed** with
  the published `release-image.yml@refs/tags/rel-<version>` identity, with the `release.yml`
  identity (both on the SAN), and with `--certificate-github-workflow-trigger push`.
- **It found a defect.** `test` checked out `github.ref`, and the branch moved (the empty commit
  `6ca6c73`) after dispatch but before the checkout. Both jobs agreed on `6ca6c73`, but
  `--certificate-github-workflow-sha` showed the certificate naming `b169084`, the dispatch-time
  `github.sha`: the signature described source the image was not built from. On a release the
  `GITHUB_SHA` assertion would have failed the run; on a dispatch nothing did. Fixed by checking
  out `github.sha` when no `ref`/`tag` input is given (`4b66a4d`). Re-run as a dry run
  (`36157677311`) with the branch moved to `670b921` at 15:58:02, three seconds before `test`'s
  checkout: both jobs built `4b66a4d`.
- **Signature storage:** cosign v3.0.6 wrote an OCI referrer — a manifest with `artifactType`
  `application/vnd.dev.sigstore.bundle.v0.3+json` and `subject` = the image — under an OCI image
  index tagged `sha256-<digest>` (the referrers tag-schema fallback; ghcr's referrers API returns
  404). No `.sig` tag. cosign v2.4.3 and v2.5.3 report `no signatures found`, with or without
  `--new-bundle-format`, so `README.env` states cosign v3 as the minimum.
- **Cleanup:** the `rehearsal-221` image, its `sha256-…` index and the bundle manifest (three ghcr
  package versions) were deleted. The package is private, and making it public is disabled by the osprey-dcs org
  administrators (found 2026-09-26), so `README.env` and `NEXT.md` say pulling or verifying needs
  `docker login ghcr.io` with `read:packages` access. If the org allows public packages later,
  that note is the only doc change.

**Task 8 — Docs.** Add a "Container image" section to `README.env` covering: pull by digest or tag,
`cosign verify` with the `release-image.yml` identity (D5), and a note that `:latest` moves on every
release, so verifying `:latest` verifies whatever it points at *now*. Add a pointer from
`doc/running.md:54`, where the image is mentioned. Replace `NEXT.md`'s "the image is not yet
signed" line (Task 5) with the image half of #221's section, including that the image release now
fails, rather than falling back, when the matching dp-grpc tag is missing. Finish the CLAUDE.md
"Releases" update (Task 6) with the source-pinning rule from D2.

**Task 9 — Close out.** After the next release is cut, verify both signatures end to end as a
consumer would, from the release page and from `ghcr.io`, before announcing it.

## Out of scope

- **Maven signing / publishing to a Maven repository.** The ticket's scope note is right, and
  grpc-D1 applies. The distribution-model question it raises (GitHub Packages vs. Central vs.
  build-from-source) needs its own ticket if pursued.
- **CI `packages: write`, the duplicated MongoDB wait, CI concurrency, the dp-grpc tag existence
  check, and the vendored jhdf5 on the release and image build paths.** All owned by #250 (which
  absorbed #213). #250 also declines #213 item 2, so `build` keeps `mvn clean verify`, which its
  poll-timeout fix shortens to minutes. Nothing here blocks or conflicts with them. #250's edits to
  `release.yml`, `release-image.yml`, and the `Dockerfile` land after or rebase onto this ticket's.
- **Tag/POM version cross-check.** `release.yml` renames whatever `target/dp-service-*-shaded.jar`
  exists to the tag's version, so a `rel-1.17.0` tag on a 1.16.0 POM publishes a 1.16.0 build as
  1.17.0. A signature binds the jar to a commit, but it does not check the jar's name. This is the
  same follow-on the dp-grpc plan names. File it as a new ticket.
- **Making the image carry the release-page jar.** The two builds are separate (triage). Building
  the image from the signed jar would make the two signatures describe one artifact. This is a
  larger change to `Dockerfile` and job ordering.
- **Pinning `Dockerfile` base images by digest.** `maven:3.9.6-eclipse-temurin-21` and
  `eclipse-temurin:21-jre` are pinned by tag, which weakens what the image signature attests to.
  File it as a follow-on. The other provenance gap this plan first deferred, the dp-grpc clone's
  silent fallback, is now in scope (D2, Task 7). Source pinning across the job split cannot work
  while that fallback exists.
- **Back-porting the three-job split (D1) to dp-grpc and dp-desktop-app.** This is a follow-on for
  each of those repos.
- **Consumer-side verification in CI.** Only relevant if dp-service ever consumes a published
  dp-grpc jar rather than building from source.

## Dependencies and sequencing

- **Depends on:** nothing unmerged. dp-grpc#137 is merged and rehearsed (dp-grpc runs `35919676487`
  and `35921265085`), and its shape is the reference. dp-grpc's signed artifacts do not affect this
  work, because dp-service builds dp-grpc from source.
- **PR 1 before PR 2.** PR 2 reuses PR 1's `IS_RELEASE` wording, README.env structure, and
  `NEXT.md`. PR 1 does not depend on PR 2.
- **PR 1 is required before the next `rel-*` tag. PR 2 is intended to be, but does not block it.**
  A release with PR 1 alone is a coherent, documented state, not a half-finished one. The jar is
  signed. The image is unsigned, as every image so far has been, but is published only from `rel-*`
  tags and moves `:latest` only on a real release (Task 2). `NEXT.md` says the image is unsigned
  (Task 5), and `README.env` documents only the jar. This is why the hazard fixes moved into PR 1:
  without them, a PR-1-only release would ship signing alongside an image workflow that any tag
  push, or a dispatch against a tag, could use to repoint `:latest`.
- **Does not block on:** #250, dp-desktop-app#24, or the distribution-model question.
- **Never push a tag to rehearse,** before or after this ticket. PR 1 narrows the image trigger only
  for commits that contain it. A tag on an older commit runs that commit's `'*'` trigger (Task 6).
