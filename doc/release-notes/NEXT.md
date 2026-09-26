# Release Notes — next release (unreleased)

**This is the working draft for the next release. It is not a release note yet.**

Sections accumulate here as tickets land, so the content is written while it is fresh and gets
reviewed in the PR that causes it. At release time this file is renamed to
`doc/release-notes/rel-<version>.md` and finished — see **Cutting the release** at the bottom.

**The version of the upcoming release is deliberately not named anywhere in this file**, in its
filename or in its prose. `release.yml` resolves the notes path strictly from the tag
(`doc/release-notes/${GITHUB_REF_NAME}.md`), so a file committed under a guessed version is both
stranded and a failed release-notes check on the tag that does ship. Past versions are named
freely where they are the point — "published through 1.16.0" is a durable fact about what shipped,
not a guess about what is about to.

Nothing here should assert what *else* the release contains, either: that is knowable only once
the release is cut, and a stale claim in a file that already looks finished is not something the
person cutting the release has any reason to re-read.

## Contents

- [Signed release artifacts (Issue #221)](#signed-release-artifacts-issue-221)
- [Cutting the release](#cutting-the-release)

---

## Signed release artifacts (Issue #221)

The checksum published with previous releases established integrity but not origin. It was
written by the same job, with the same token, to the same release page as the jar it described —
so anyone able to replace the jar could replace the checksum sitting next to it.

This release adds a keyless Sigstore signature over `SHA256SUMS`, and one over the container
image (below). The first binds the jar to the repository, workflow file, tag, and source commit
that produced it. There is no key to distribute,
rotate, or leak: the signing identity is a short-lived certificate issued to the GitHub Actions
run itself and recorded in the public Rekor transparency log.

The release workflow is now split into three jobs. `build` runs the full build and test suite with
read-only permissions. `sign` holds the OIDC signing token, but checks out no code and runs nothing
from the repository: it checksums and signs only what `build` handed over. `publish` can write to
the release but holds no signing token. Publishing is gated on a `rel-*` tag push, so the
`workflow_dispatch` rehearsal trigger added alongside it is structurally incapable of publishing.

**The asset names change.** A scripted download of `dp-service-<version>.jar.sha256` will get a
404 against this release. Published assets are now:

```
dp-service-<version>.jar
SHA256SUMS
SHA256SUMS.cosign.bundle
```

### Verifying the jar

Download the three assets into a single directory with no subdirectories, then:

```bash
sha256sum -c SHA256SUMS
```

`SHA256SUMS` does not list itself or `SHA256SUMS.cosign.bundle`, so `sha256sum -c` says nothing
about either. What protects them is the signature: `cosign verify-blob` below checks the bundle
against `SHA256SUMS`, and the checksum in turn covers the jar.

To verify the signature, install [cosign](https://docs.sigstore.dev/cosign/system_config/installation/)
and run, with `<version>` replaced by the release you downloaded:

```bash
cosign verify-blob \
  --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity 'https://github.com/osprey-dcs/dp-service/.github/workflows/release.yml@refs/tags/rel-<version>' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-trigger push \
  SHA256SUMS
```

Expect `Verified OK`.

Keep every identity flag exactly as written:

- `--certificate-identity` is an exact match on this repository, the `release.yml` workflow file,
  and the one tag being verified. A pattern accepting any `rel-` tag would also accept an older
  release's genuine `SHA256SUMS` and bundle substituted for this one's. A loosened or unanchored
  pattern would accept a valid signature made by any workflow in any repository — the usual way
  this check ends up passing while proving nothing.
- `--certificate-github-workflow-trigger push` requires the signature to come from the tag push
  that published the release. A manual run of the same workflow against the tag would carry the
  same identity; the release workflow refuses such runs, and this flag is what makes a verifier
  independent of that.

Full instructions, including what the signature proves that the checksum does not, are in
[`README.env`](https://github.com/osprey-dcs/dp-service/blob/main/README.env).

### Checksum path fixed

The `.sha256` files published through 1.16.0 recorded the jar's path as
`release/dp-service-<version>.jar`, because the workflow generated them from the repository root.
A consumer who downloaded the jar and its checksum into one directory and ran `sha256sum -c` got:

```
sha256sum: release/dp-service-<version>.jar: No such file or directory
```

unless they first recreated a `release/` subdirectory. `SHA256SUMS` is generated from inside the
artifact directory and records bare filenames, so it verifies where the files actually land.

### Container image: signed, and published only from releases

The container image `ghcr.io/osprey-dcs/dp-service` is now signed the same way, by digest, by the
`release-image.yml` workflow. Its identity names that workflow file, not `release.yml`, so the
jar's verify command does not apply to the image.

The image package is private to the osprey-dcs organization, so pulling or verifying it needs
`docker login ghcr.io` with a GitHub account that has read access to it (a token with the
`read:packages` scope); cosign uses the same credentials. Without it both fail with a 401, which
is an access error, not a signature failure. Then pull the release tag and verify it:

```bash
cosign verify \
  --certificate-identity 'https://github.com/osprey-dcs/dp-service/.github/workflows/release-image.yml@refs/tags/rel-<version>' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-trigger push \
  ghcr.io/osprey-dcs/dp-service:rel-<version>
```

Image verification needs **cosign v3 or later**: the signature is stored as an OCI referrer, and
cosign v2 reports `no signatures found` against a correctly signed image. The identity flags
carry the same meaning as the jar's above. `:latest` moves on every release, so
verifying `:latest` verifies whatever it points at *now*: verify and deploy by digest. The image is a
separate build of the same tagged source and does not contain the release-page jar. Full
instructions are in the "Container Image" section of
[`README.env`](https://github.com/osprey-dcs/dp-service/blob/main/README.env).

Changes to how the image is published:

- **BEHAVIOR CHANGE: only a `rel-*` tag publishes an image.** Previously a push of *any* tag built
  and pushed an image under that tag's name and moved `:latest` to it. Anyone who relied on another
  tag (a `v1.14`-style tag, say) producing an image no longer gets one.
- **`:latest` moves only on a release.** A manual `workflow_dispatch` of the image workflow never
  moves `:latest`, even when it targets a tag. Previously a non-dry-run dispatch against any tag
  did. A publishing dispatch against a tag ref, or one whose `image_tag` is `latest` or `rel-*`, is
  now refused outright.
- **The image release fails, rather than falling back, when dp-grpc is not tagged.** An image built
  for `rel-<version>` is built against dp-grpc `rel-<version>` or not at all, as the jar already
  was; the tag must also match the POM's version and `dp-grpc.version`. Previously the image
  workflow fell back to another dp-grpc tag or to `main` and published anyway, and the `Dockerfile`
  fell back to dp-grpc's default branch whenever its clone failed. Both fallbacks are gone.
- **The image is built from the exact source that passed the tests.** The test run and the
  push-and-sign run are now separate jobs, and the second builds the dp-service and dp-grpc
  commits the first tested rather than re-resolving a branch or tag name.
- **A manual image dispatch now defaults to dp-grpc `rel-<dp-grpc.version>`**, falling back to
  `main` with a warning, instead of always `main`. Set `dp_grpc_ref` explicitly to override.

### Upgrade items

1. **Update any scripted download of the `.sha256` file.** `dp-service-<version>.jar.sha256` no
   longer exists; `SHA256SUMS` replaces it.
2. **Drop any workaround for the checksum path.** If a script recreated a `release/` subdirectory
   to make `sha256sum -c` succeed, remove it.
3. **Stop relying on non-`rel-*` tags for images.** Only release tags publish to
   `ghcr.io/osprey-dcs/dp-service`.
4. **Optionally, start verifying the signatures**, on the jar and on the image. It is a new
   capability, not a new requirement.
5. **Rebuilding the image with `docker build` passes `DP_GRPC_REF` to a fetch with no fallback.**
   A branch, tag, or commit SHA works; a ref dp-grpc does not have now fails the build instead of
   building dp-grpc's default branch.

---

## Cutting the release

When the version is known and the release is being cut:

1. **`git mv doc/release-notes/NEXT.md doc/release-notes/rel-<version>.md`.** The filename must
   match the tag exactly; `release.yml` fails the run before the build if it does not.
2. **Retitle** the H1 to `# dp-service <version> Release Notes` and replace this file's preamble
   with a "Changes since rel-<previous>" summary, naming the dp-grpc release it builds against —
   written now, when the full contents of the release are actually known.
3. **Add the "Upgrading from &lt;previous&gt;" section** as the first section after Contents,
   folding in the per-ticket upgrade items above. Call out silent behavior changes separately from
   compile errors, per CLAUDE.md — a change that alters results without raising an error is the
   one a reader most needs up front.
4. **Repoint `blob/main/...` links to `blob/rel-<version>/...`.** This file is published as the
   release body via `body_path`, and relative links do not survive that lift — they resolve against
   the repo root, not `doc/release-notes/`, and 404. Links here are already absolute for that
   reason, but one pinned to `main` drifts as the repo moves on; pinned to the tag it keeps
   describing the content this release actually shipped.
5. **Delete this "Cutting the release" section** and update Contents.
6. **Add the row to `README.md`'s `## Release Notes` table**, newest first, with a one-line
   summary and **Breaking.** if it is.
7. **Decide whether the release is breaking** and say so in the opening if it is. Note that #221
   renames published release assets: that breaks scripted downloads even in a release with no API
   change at all.
8. **Merge the notes before pushing the tag.** `release.yml` reads them from the tagged commit.
   Push tags in dependency order — dp-grpc first, since a dp-service release builds strictly
   against dp-grpc's matching `rel-<version>` tag.
9. **Start a fresh `NEXT.md`** for the following cycle.
