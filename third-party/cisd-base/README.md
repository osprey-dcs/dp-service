# Vendored dependency: `cisd:base:18.09.0`

**Do not delete this directory.** It is vendored for the same reason as, and alongside,
`../cisd-jhdf5/` — read that README first; everything it says applies here.

`cisd:base` is JHDF5's support library. `pom.xml` declares it directly and the jhdf5 POM
depends on it, and like jhdf5 it is **not on Maven Central** (404) — `maven.scijava.org` is its
only public host. The original vendoring covered only jhdf5, because during the 2026-08-27
outage SciJava still served this jar; a build with that host unreachable failed on
`cisd:base` instead (#250 Task 4). `../install-vendored.sh` installs both.

| File | Purpose |
|---|---|
| `base-18.09.0.jar` | The artifact itself, unmodified |
| `base-18.09.0.pom` | Its POM — carries the transitive deps on `commons-io` and `commons-lang3`, both on Central |

Both files are byte-for-byte as published by `maven.scijava.org`. Verified checksums:

```
jar SHA-1    6c54c88f7a51e26d066a5c8935b03bc432d516fb   (matches upstream .sha1)
jar SHA-256  c73d01ffb427e5b7008003b4eaf9303c1febd883100bf81752ba71f41c701148
pom SHA-1    25c5fb2428a944fcb07e0d9cc959798e9749ab08   (matches upstream .sha1)
pom SHA-256  ccfe023a478b818416600dabb3f54dd9be5a100ca8abd72386056bc3a9f170b5
```

**Licensing:** Apache-2.0, from ETH Zurich Scientific IT Services; the POM declares
`<distribution>repo</distribution>`, as jhdf5's does. Redistributed unmodified.

**Removal:** under the same conditions as jhdf5, and together with it — it is only here
because jhdf5 needs it.
