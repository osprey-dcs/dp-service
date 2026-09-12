# Schema migration rehearsal

How to rehearse the schema migrations against a copy of a production database **before** upgrading
that deployment, without moving its time-series data. Companion to
[schema-migration.md](schema-migration.md), which explains the mechanism itself and what each
startup failure means.

The upgrade's first startup runs every pending migration inside `MongoClientBase.init()` and fails
closed on any problem. As of 1.16.0 there are five:

| Version | What it does | Collections |
|---|---|---|
| 1 | Rename `comment` → `description`; replace the annotations text index | `annotations` |
| 2 | Normalize `tags` to lowercase/deduplicated/sorted | `annotations` |
| 3 | Canonicalize `dataSetIds`/`annotationIds` reference ids to lowercase hex | `annotations` |
| 4 | Stamp the `_t` discriminator on legacy columns | `buckets`, `calculations` |
| 5 | Seed per-PV `pvStats.maxBucketSpanSeconds`; drop `bucketSpanVerification` | `buckets`, `pvStats` |

## Scope: what this rehearsal does and does not cover

This procedure restores everything **except** the large time-series collections (`buckets`,
`sampleStatusBuckets`), which is what keeps it cheap enough to run on a workstation. That
exclusion was free through 1.15, when every migration touched `annotations` only. **It is no
longer free**: v4 and v5 both operate on `buckets`, so with the buckets excluded they run
against an empty collection and complete trivially.

So be precise about what a pass means:

- **Fully rehearsed — v1, v2, v3.** They run against genuinely restored production data and
  indexes.
- **Partly rehearsed — v4.** Its *calculations* half runs against restored data. Its *buckets*
  half — the one whose failure is silent — has nothing to act on unless you do the optional
  [Part 1b](#part-1b-optional--a-bucket-sample-to-rehearse-v4s-bucket-half-for-real), which
  restores a few thousand sampled buckets for exactly this purpose.
- **Mechanism only — v5.** Executed, marker recorded, version reaches 5, so ordering, claim
  handling, and idempotency are exercised end to end; its seed has no archive to read (or, with
  Part 1b, only the sample).

Either way the **mechanism** is fully exercised: all five migrations run in order, the claim is
taken and released, the marker records five applied entries, and the second run proves idempotency.

What a pass does **not** tell you about a 1.16.0 upgrade, with or without Part 1b: how long v4 and
v5 take on the real archive. Both scan `buckets` end to end, and on a large archive that is the
dominant cost of the upgrade window — the thing most worth predicting. A sample cannot predict it.
Estimate it separately with the read-only queries in
[upgrade-1.16-slac.md](upgrade-1.16-slac.md), which run the same pipeline shapes against a
secondary without writing.

Rehearsing v4/v5's data effects at *full scale* would mean restoring the whole `buckets`
collection, at which point this stops being a workstation procedure. Part 1b is the middle ground:
a sample large enough to prove the stamping works, small enough to keep the rehearsal cheap. Run
this for migration-mechanism confidence, and predict the bucket scans by measurement rather than by
rehearsal.

Two properties still make the partial restore faithful for what it does cover:

- **The legacy-vs-fresh probe classifies the same way production will.** A database with no
  `schemaVersion` marker is treated as legacy (migrate from 0) if *any* managed collection holds a
  document. Any restored metadata collection being non-empty produces that classification; the
  missing buckets don't change the outcome.
- **The v1 index replacement is exercised for real.** `mongodump` captures index definitions and
  `mongorestore` rebuilds them, so the annotations collection comes back with the deployment's
  actual old text index, and v1's drop-then-create runs against the genuine shape — the step most
  worth rehearsing, since a mismatched index is what can fail with `IndexOptionsConflict`.

## Part 0 — pre-checks against the live database (read-only)

Optional but cheap: predict the migrations' write volume before rehearsing, and catch v1's one halt
condition in advance. Run against the **live** database with a read-only connection:

```js
// v1 volume: documents still carrying the old field name
db.annotations.countDocuments({comment: {$exists: true}})

// v1 halt condition: documents carrying BOTH fields make v1 stop rather than
// overwrite a description. Must be 0; resolve any hits by hand before upgrading.
db.annotations.countDocuments({comment: {$exists: true}, description: {$exists: true}})

// v2 volume (approximate — catches case, not dedupe/order): tags with uppercase
db.annotations.countDocuments({tags: /[A-Z]/})

// v3 volume: reference ids with uppercase hex
db.annotations.countDocuments({$or: [{dataSetIds: /[A-F]/}, {annotationIds: /[A-F]/}]})
```

## Part 1 — dump the metadata collections

Run wherever the production cluster is reachable (`mongodump` is bundled in the `mongo` Docker
image if it isn't installed locally). The database name is fixed at `dp`. This skips the two large
time-series collections; what remains is typically thousands of documents, not millions.
(To additionally rehearse v4's bucket half, see the optional Part 1b below, which adds a small
sampled subset of `buckets` to this dump.)

```bash
mongodump --uri="<production-uri>" --db=dp \
  --excludeCollection=buckets \
  --excludeCollection=sampleStatusBuckets \
  --out=rehearsal-dump/
```

If the URI does not name an auth database, add `?authSource=admin` (or pass
`--authenticationDatabase admin`): mongodump defaults its auth database to the `--db` value,
unlike the service's driver, which defaults to `admin` — the same URI that works in
`application.yml` can fail authentication here.

Copy `rehearsal-dump/` to the workstation running Part 2.

## Part 1b (optional) — a bucket sample, to rehearse v4's bucket half for real

Skip this unless you want coverage of v4's most consequential effect. Part 1's exclusion of
`buckets` leaves v4's bucket half untested, and that is the half whose failure is **silent**: an
unstamped legacy column throws `CodecConfigurationException` mid-decode, which escapes the query
dispatchers' `DpException`-only catch, so the client receives zero buckets with no error. A few
thousand sampled buckets restore that coverage at negligible cost.

This does **not** predict how long v4 and v5 take on the full archive — a sample cannot. Keep using
the read-only estimates in [upgrade-1.16-slac.md](upgrade-1.16-slac.md) for the window.

**Step 1 — build the sample on the source side.** `mongodump --query` cannot express "N random
documents", so materialize the sample into a scratch collection first. Run against a **secondary**;
this reads the full collection once.

```js
// On the production cluster. Adjust the sample size to taste; 5000 is ample.
db.buckets.aggregate([
  {$match: {dataColumn: {$exists: true}, "dataColumn._t": {$exists: false}}},
  {$sample: {size: 5000}},
  {$out: "bucketsRehearsalSample"}
], {allowDiskUse: true})

db.bucketsRehearsalSample.countDocuments()   // 0 means nothing needs stamping — see below
```

The `$match` is v4's own bucket filter, so the sample contains exactly the documents it would
stamp. **A count of 0 is a legitimate and useful result**: it means this archive holds no
pre-1.13 unstamped bucket columns, so v4's bucket half is a no-op in production and there is
nothing to rehearse. Drop the scratch collection and skip the rest of this part.

Sampling only unstamped documents is deliberate. A uniform sample of a mostly-stamped archive
would likely contain no unstamped columns at all and would rehearse nothing, passing vacuously.

**Step 2 — dump the sample alongside the metadata.** Add it to the Part 1 dump:

```bash
mongodump --uri="<production-uri>" --db=dp \
  --collection=bucketsRehearsalSample \
  --out=rehearsal-dump/
```

Then drop the scratch collection on the source: `db.bucketsRehearsalSample.drop()`.

**Step 3 — restore it as `buckets`.** In the Part 2 script, add this immediately after the
`mongorestore` line, before the restored-document check:

```bash
echo "== restoring bucket sample as 'buckets'"
docker exec "$CONTAINER" mongosh -u admin -p admin --quiet --eval '
  const d = db.getSiblingDB("dp");
  if (d.getCollectionNames().includes("bucketsRehearsalSample")) {
    d.bucketsRehearsalSample.aggregate([{$out: "buckets"}]);
    d.bucketsRehearsalSample.drop();
    print("   seeded buckets with " + d.buckets.countDocuments() + " sampled document(s)");
  } else {
    print("   no bucket sample in the dump — skipping (v4 bucket half not rehearsed)");
  }'
```

Restoring under the real collection name is what matters: the migrations address `buckets` by name,
and `buckets` is also a managed collection for the legacy-vs-fresh probe.

**Step 4 — assert the sample was stamped.** Add one entry to the verify block's `bad` object:

```js
    v4_unstampedBucketColumns: d.buckets.countDocuments(
        {dataColumn: {$exists: true}, "dataColumn._t": {$exists: false}}),
```

With the sample present, `run1.log` should report a non-zero stamp count:

```
V4StampColumnDiscriminators: stamped _t on 5000 bucket document(s)
```

**What this adds, and what it still doesn't.** v4's bucket half now runs against genuine pre-1.13
documents and is verified. v5 still seeds from the sample rather than the archive, so its
`pvStats now holds N document(s)` line reflects the sampled PVs only — a real exercise of the
pipeline, but not a prediction of the production count. Neither migration's runtime here says
anything about the full-archive scans.

## Part 2 — rehearsal script

Prerequisites: Docker, Java 21, and the **release-candidate** shaded jar (build the release
tag/branch with `mvn clean package -DskipTests`). The script starts a throwaway MongoDB 8.0
container on port 27018, restores the dump, starts the Annotation Service twice (first run
migrates, second run must be a no-op), verifies the results, and cleans up. On any failure it
leaves the container running for inspection.

`EXPECTED_VERSION` must match `SchemaMigrationRunner.SCHEMA_VERSION` in the release candidate — it
is 5 as of 1.16.0. Bump it here whenever a release adds a migration, or run 1 fails against its own
success message. Note that v4 and v5 complete in milliseconds in this rehearsal because `buckets`
was not restored; see Scope above.

```bash
#!/usr/bin/env bash
set -euo pipefail

# ---- adjust these ----------------------------------------------------------
JAR="target/dp-service-1.16.0-shaded.jar"     # the release-candidate shaded jar
DUMP_DIR="$PWD/rehearsal-dump"                # output of Part 1
CONTAINER="dp-migration-rehearsal"
PORT=27018
EXPECTED_VERSION=5                            # SchemaMigrationRunner.SCHEMA_VERSION in the RC
# ----------------------------------------------------------------------------

URI="mongodb://admin:admin@localhost:${PORT}/"
MAIN=com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer

mongosh_eval() {
  docker exec "$CONTAINER" mongosh -u admin -p admin --quiet --eval "$1"
}

wait_for_line() {  # logfile pattern [timeout-seconds]
  local logfile=$1 pattern=$2 timeout=${3:-180} i
  for ((i = 0; i < timeout; i++)); do
    grep -q "$pattern" "$logfile" && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || {
      echo "FAIL: server exited before logging: $pattern"; tail -40 "$logfile"; return 1; }
    sleep 1
  done
  echo "FAIL: timed out waiting for: $pattern"; tail -40 "$logfile"; return 1
}

run_service() {  # logfile completion-pattern
  local logfile=$1 pattern=$2
  DP_MONGO_DB_URI="$URI" java -cp "$JAR" "$MAIN" > "$logfile" 2>&1 &
  SERVER_PID=$!
  wait_for_line "$logfile" "$pattern"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}

echo "== starting throwaway mongo:8.0 on port ${PORT}"
docker run -d --name "$CONTAINER" -p "${PORT}:27017" \
  -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=admin \
  -v "${DUMP_DIR}:/dump:ro" mongo:8.0 > /dev/null
until mongosh_eval 'db.runCommand({ping: 1}).ok' 2>/dev/null | grep -q 1; do sleep 1; done

echo "== restoring dump (indexes included)"
docker exec "$CONTAINER" mongorestore -u admin -p admin --authenticationDatabase admin \
  --nsInclude 'dp.*' /dump > /dev/null

# A dump that restored nothing would classify as a FRESH install and skip every
# migration — a false pass. Require at least one restored document.
RESTORED=$(mongosh_eval '
  const d = db.getSiblingDB("dp");
  d.getCollectionNames().reduce((n, c) => n + d.getCollection(c).estimatedDocumentCount(), 0)')
[ "$RESTORED" -gt 0 ] || { echo "FAIL: restore produced 0 documents — check DUMP_DIR"; exit 1; }
echo "   restored ~${RESTORED} documents"

echo "== run 1: expecting migrations to apply"
run_service run1.log "schema migration complete; database is at version ${EXPECTED_VERSION}"
if grep -q "recording fresh database" run1.log; then
  echo "FAIL: database classified as a fresh install — the rehearsal did not exercise the migrations"
  exit 1
fi
grep -E "treating database as|claimed schema migration|applying schema migration|migration complete" run1.log

echo "== verifying migrated state"
# v1-v3 assert on real restored data. v4 asserts its calculations half; add the bucket-half
# assertion from Part 1b if you restored a bucket sample. v5 asserts the legacy collection was
# dropped; its pvStats seed has nothing (or only the sample) to read. See Scope above.
mongosh_eval '
  const d = db.getSiblingDB("dp");
  const bad = {
    v1_commentLeft: d.annotations.countDocuments({comment: {$exists: true}}),
    v1_oldTextIndex: d.annotations.getIndexes().filter(i => i.weights && i.weights.comment).length,
    v2_upperTags: d.annotations.countDocuments({tags: /[A-Z]/}),
    v3_upperRefIds: d.annotations.countDocuments(
        {$or: [{dataSetIds: /[A-F]/}, {annotationIds: /[A-F]/}]}),
    v4_unstampedCalcColumns: d.calculations.countDocuments(
        {dataFrames: {$elemMatch: {dataColumns: {$elemMatch: {_t: {$exists: false}}}}}}),
    v5_legacyCollectionLeft: d.getCollectionNames().includes("bucketSpanVerification") ? 1 : 0,
  };
  const marker = d.serviceMetadata.findOne({_id: "schemaVersion"});
  print(JSON.stringify({bad, markerVersion: marker ? marker.version : null,
      applied: marker ? marker.appliedMigrations.length : 0}, null, 2));
  if (Object.values(bad).some(n => n > 0) || !marker || marker.version !== '"$EXPECTED_VERSION"') quit(1);
' || { echo "FAIL: post-migration verification"; exit 1; }

echo "== run 2: expecting a no-op (idempotency / marker check)"
run_service run2.log "schema version ${EXPECTED_VERSION} is current; no migration needed"

echo "== cleanup"
docker rm -f "$CONTAINER" > /dev/null

echo "REHEARSAL PASSED — see run1.log for the exact lines the production upgrade will log."
```

## What success looks like

`run1.log` contains, in order (host/count details vary):

```
no schema version marker but existing data found; treating database as schema version 0 and migrating to 5
claimed schema migration from version 0 to 5 as <pid>@<host>
applying schema migration version 1: rename annotation 'comment' field to 'description' and replace its text index
applying schema migration version 2: normalize annotation tags to lowercase/deduplicated/sorted
applying schema migration version 3: canonicalize annotation reference ids to lowercase hex
applying schema migration version 4: stamp _t discriminator on legacy bucket and calculations columns
applying schema migration version 5: seed pvStats maxBucketSpanSeconds from buckets; drop bucketSpanVerification
V5SeedPvStatsMaxBucketSpan: seeded pvStats from buckets; pvStats now holds 0 document(s)
V5SeedPvStatsMaxBucketSpan: dropped legacy collection bucketSpanVerification
schema migration complete; database is at version 5
```

and `run2.log` contains `schema version 5 is current; no migration needed`. The verify step prints
all-zero `bad` counts and `markerVersion: 5`.

`pvStats now holds 0 document(s)` is the **expected** result here, not a failure: `buckets` was not
restored, so v5 had nothing to seed from. On the production upgrade that number is the count of
distinct PVs in the archive, and it is the line to watch for — see
[upgrade-1.16-slac.md](upgrade-1.16-slac.md) for what to compare it against.

If a step fails, the container is left running on port 27018 for inspection; the failure messages
map onto the "Startup failures and what to do" section of [schema-migration.md](schema-migration.md).
A rehearsal failure is the mechanism working — fix the cause against the rehearsal container
(re-runs are cheap: `docker rm -f`, start over) before touching production.
