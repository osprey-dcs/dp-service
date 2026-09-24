# Vendored dependency: `cisd:jhdf5:19.04.1`

**Do not delete this directory.** Removing it breaks CI on every pull request, and every
release and image build. Read this
first if you are tempted to tidy an 8 MB binary out of the repository.

## Why this is here

`cisd:jhdf5` is **not published to Maven Central**. Its only public host is
`maven.scijava.org`, and on 2026-08-27 that host began returning **HTTP 503 for JAR
downloads while continuing to serve POMs** — deterministically, for every version of this
artifact, and for other large artifacts on the same host. Small artifacts such as
`cisd:base` continued to resolve normally, which is why the failure looked selective
rather than like an outage.

With no Central fallback and no working mirror (`maven.imagej.net` and the Nexus service
paths all returned 503 as well), the dependency became unresolvable from a clean
environment.

## Why CI had been passing until then

CI never had a reliable path to this jar; it had been downloading it fresh on most runs
and occasionally restoring it from an Actions cache. GitHub Actions caches are **scoped
per ref** — a branch may read only its own caches and the default branch's. Every cache in
this repository was created on a `refs/pull/NNN/merge` ref and none on `main`, so **no PR
could ever reuse another PR's cache**. Each PR therefore depended on `maven.scijava.org`
being healthy at that moment. When it stopped serving JARs, the next PR failed, and every
subsequent PR would have failed the same way.

The failure surfaced as a dependency-resolution error that reads like a transient outage,
which makes it easy to misdiagnose as "just re-run the job." It is not transient.

## What is vendored

| File | Purpose |
|---|---|
| `jhdf5-19.04.1.jar` | The artifact itself, unmodified |
| `jhdf5-19.04.1.pom` | Its POM — carries the transitive deps on `cisd:base`, `commons-io`, `commons-lang3` |

Both files are byte-for-byte as published by `maven.scijava.org`. Verified checksums:

```
SHA-1    8eb7ba646cf064dc6e31fde236cb129e6b830d90   (matches upstream .sha1)
SHA-256  0c20636d3388ebaa3c941f6a2c867fc0420ebbc2a5768c14f008f87069e6cecc
```

## Licensing

JHDF5 is **Apache-2.0**, from ETH Zurich Scientific IT Services. Its POM declares
`<distribution>repo</distribution>`, which is the publisher's explicit statement that the
artifact may be redistributed through a repository. The jar is redistributed here
unmodified, so its embedded copyright and license notices are intact, as Apache-2.0
requires.

Note that the jar bundles native HDF5 binaries (`libjhdf5.so`, `jhdf5.dll`) from The HDF
Group, under a separate BSD-style license. That is permissive and redistribution-friendly,
but it is a distinct license from the Apache-2.0 covering the Java wrapper — worth a closer
look before redistributing this artifact outside the project.

## How it is used

Every build path installs it into its local Maven repository before building dp-service, by
running `third-party/install-vendored.sh` (which also installs `../cisd-base/`, see its
README): `.github/workflows/ci.yml`, `.github/workflows/release.yml` and
`.github/workflows/release-image.yml` on the runner, and the `Dockerfile` in its builder stage
(a BuildKit build cannot see the runner's `~/.m2`). A new build path needs the same step.

`ci.yml` also makes `maven.scijava.org` unreachable for the whole job, so a new dependency
that only SciJava hosts fails CI rather than the next outage. If that step fails a build,
vendor the dependency here and add it to `install-vendored.sh`; do not remove the block.

Local developer builds need no setup: Maven resolves from `~/.m2` first, and anyone who
already built this project has the jar cached. A developer starting from an empty `~/.m2`
can run `third-party/install-vendored.sh` from the repository root.

Vendoring was chosen over hosting the jar in GitHub Packages specifically to avoid a
per-developer credential requirement. A missing token would surface as an HTTP 401 that
looks nearly identical to the 503 outage documented above — a confusing failure landing on
whoever is least equipped to diagnose it, possibly years from now.

## When this can be removed

Remove it once **either** of the following is true, and not before:

1. `cisd:jhdf5` is published to Maven Central, or
2. the project no longer depends on jhdf5 (see `pom.xml` and the HDF5 export path in
   `ExportDataJobHdf5` / `DataExportHdf5File`).

`maven.scijava.org` recovering is **not** sufficient. The per-ref cache scoping means a
single-host dependency with no Central fallback will break CI again the next time that
host has trouble.

To remove: delete this directory and `../cisd-base/`, drop `install-vendored.sh` and the
step that runs it from all four build paths listed above, and drop the
corresponding note from `CLAUDE.md`.
