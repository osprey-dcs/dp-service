# Schema migration rehearsal

How to rehearse the schema migrations (v1–v3) against a copy of a production database **before**
upgrading that deployment, without moving its time-series data. Companion to
[schema-migration.md](schema-migration.md), which explains the mechanism itself and what each
startup failure means.

The upgrade's first startup runs every pending migration inside `MongoClientBase.init()` and fails
closed on any problem. All three current migrations operate on the **annotations collection only**:

| Version | What it does |
|---|---|
| 1 | Rename `comment` → `description`; replace the annotations text index |
| 2 | Normalize `tags` to lowercase/deduplicated/sorted |
| 3 | Canonicalize `dataSetIds`/`annotationIds` reference ids to lowercase hex |

The buckets are irrelevant to all of them, so the rehearsal restores everything **except** the large
time-series collections (`buckets`, `sampleStatusBuckets`). Two properties make this partial restore
a faithful rehearsal rather than an approximation:

- **The legacy-vs-fresh probe classifies the same way production will.** A database with no
  `schemaVersion` marker is treated as legacy (migrate from 0) if *any* managed collection holds a
  document. Any restored metadata collection being non-empty produces that classification; the
  missing buckets don't change the outcome.
- **The v1 index replacement is exercised for real.** `mongodump` captures index definitions and
  `mongorestore` rebuilds them, so the annotations collection comes back with the deployment's
  actual old text index, and v1's drop-then-create runs against the genuine shape — the step most
  worth rehearsing, since a mismatched index is what can fail with `IndexOptionsConflict`.

What the rehearsal does **not** cover: bucket-side startup work (max-bucket-span verification over
the real archive). That is independent of the migration mechanism and unchanged by migrations.

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

## Part 2 — rehearsal script

Prerequisites: Docker, Java 21, and the **release-candidate** shaded jar (build the release
tag/branch with `mvn clean package -DskipTests`). The script starts a throwaway MongoDB 8.0
container on port 27018, restores the dump, starts the Annotation Service twice (first run
migrates, second run must be a no-op), verifies the results, and cleans up. On any failure it
leaves the container running for inspection.

```bash
#!/usr/bin/env bash
set -euo pipefail

# ---- adjust these ----------------------------------------------------------
JAR="target/dp-service-1.16.0-shaded.jar"     # the release-candidate shaded jar
DUMP_DIR="$PWD/rehearsal-dump"                # output of Part 1
CONTAINER="dp-migration-rehearsal"
PORT=27018
EXPECTED_VERSION=3                            # SchemaMigrationRunner.SCHEMA_VERSION in the RC
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
mongosh_eval '
  const d = db.getSiblingDB("dp");
  const bad = {
    v1_commentLeft: d.annotations.countDocuments({comment: {$exists: true}}),
    v1_oldTextIndex: d.annotations.getIndexes().filter(i => i.weights && i.weights.comment).length,
    v2_upperTags: d.annotations.countDocuments({tags: /[A-Z]/}),
    v3_upperRefIds: d.annotations.countDocuments(
        {$or: [{dataSetIds: /[A-F]/}, {annotationIds: /[A-F]/}]}),
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
no schema version marker but existing data found; treating database as schema version 0 and migrating to 3
claimed schema migration from version 0 to 3 as <pid>@<host>
applying schema migration version 1: rename annotation 'comment' field to 'description' and replace its text index
applying schema migration version 2: normalize annotation tags to lowercase/deduplicated/sorted
applying schema migration version 3: canonicalize annotation reference ids to lowercase hex
schema migration complete; database is at version 3
```

and `run2.log` contains `schema version 3 is current; no migration needed`. The verify step prints
all-zero `bad` counts and `markerVersion: 3`.

If a step fails, the container is left running on port 27018 for inspection; the failure messages
map onto the "Startup failures and what to do" section of [schema-migration.md](schema-migration.md).
A rehearsal failure is the mechanism working — fix the cause against the rehearsal container
(re-runs are cheap: `docker rm -f`, start over) before touching production.
