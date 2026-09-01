# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run specific service:
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.14.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

## Architecture Overview
This is a Data Platform service implementation with three main services:
- **Ingestion Service**: Handles data ingestion with high-performance streaming APIs and comprehensive validation
- **Query Service**: Provides time-series data retrieval and metadata queries
- **Annotation Service**: Manages data annotations, datasets, data exports, PV metadata, and machine configurations

### Service Framework Pattern
Each service follows a consistent architecture:
1. **gRPC Server**: Entry point extending `GrpcServerBase`
2. **Service Implementation**: Implements gRPC service methods, extends protobuf-generated stubs
3. **Handler**: Manages request queue and worker threads, extends `QueueHandlerBase`
4. **Jobs**: Process individual requests asynchronously, extend `HandlerJob`
5. **Database Client**: MongoDB interface for persistence operations
6. **Dispatchers**: Send responses back to clients, extend `Dispatcher`

### Key Components by Service
- **Ingestion**: `ingest.server.IngestionGrpcServer` → `ingest.service.IngestionServiceImpl` → `ingest.handler.mongo.MongoIngestionHandler`
- **Query**: `query.server.QueryGrpcServer` → `query.service.QueryServiceImpl` → `query.handler.mongo.MongoQueryHandler`
- **Annotation**: `annotation.server.AnnotationGrpcServer` → `annotation.service.AnnotationServiceImpl` → `annotation.handler.mongo.MongoAnnotationHandler`

## Multi-Project Structure
The Data Platform consists of two related projects:
- **dp-grpc** (`~/dp.fork/dp-java/dp-grpc`): Contains protobuf definitions for all service APIs
- **dp-service** (this project): Java implementations of the services defined in dp-grpc

### gRPC API Evolution
When modifying gRPC APIs:
1. Update protobuf files in `dp-grpc/src/main/proto/`
2. Regenerate Java classes: `mvn clean compile` in dp-grpc
3. Update service implementations in dp-service to match new protobuf signatures
4. Update validation logic in `IngestionValidationUtility` for new column types
5. Follow systematic renaming pattern: Service → Handler → Jobs → Dispatchers → Tests

## Ticket Planning Workflow

Every non-trivial ticket gets a **version-controlled plan** at `plan/tickets/<issue>/plan.md`,
committed alongside the implementation. This is a deliberate change from the older convention of
keeping plans in the gitignored `.dev/plan/issue-<n>/` directory — plans there were invisible to
reviewers, to CI, and to anyone working from a fresh clone. `.dev/` remains in `.gitignore` and still
holds scratch material; do not add new ticket plans there.

Scratch and draft material stays outside the repo, under `~/dp/dev/tickets/dp-service/<issue>/`.
The distinction is intent, not format: a draft being iterated on is scratch; the plan the
implementation will be reviewed against belongs under `plan/tickets/`.

**Triage before planning.** Verify the ticket's stated premises against the code before writing the
plan — several tickets in this repo have been filed on a claim that turned out not to hold (see
#243's triage of #245, where "these two methods already behave this way" was wrong for all three
methods, changing the scope of both tickets). Where triage contradicts the ticket, update the issue
description and say so explicitly in the plan's Background section rather than silently planning
around it.

**Plan structure** (see `plan/tickets/243/plan.md` for a worked example):

- **Overview** — what the ticket delivers, and for whom.
- **Background / triage findings** — verified facts with `file:line` references, especially anything
  that contradicts the issue as filed.
- **Design decisions** — the choices a reviewer would otherwise have to reverse-engineer, each with
  its rationale and the alternative that was rejected.
- **Implementation tasks** — per file, concrete enough to execute without re-deriving the design.
- **Out of scope** — with a pointer to the ticket that owns each excluded item.
- **Dependencies and sequencing** — what blocks on what, and explicitly what does *not*.

Record findings that outlive the ticket in CLAUDE.md rather than leaving them only in the plan: a
plan documents one change, CLAUDE.md documents the invariant it established.

## MongoDB Collections
- **buckets**: Time-series data storage (main data collection with embedded protobuf serialization)
- **providers**: Registered data providers
- **requestStatus**: Ingestion request tracking
- **dataSets**: Annotation dataset definitions (contains DataBlockDocuments for time ranges and PV names)
- **annotations**: Data annotations (references dataSets and optionally calculations)
- **calculations**: Associated calculation results (embedded CalculationsDataFrameDocuments)
- **pvMetadata**: PV metadata records (pvName unique index, aliases index; tags, attributes, description, modifiedBy, createdAt, updatedAt)
- **configurations**: Machine configuration records (configurationName unique index, category index; tags, attributes, description, modifiedBy, createdAt, updatedAt)
- **configurationActivations**: Time-bounded activations of configurations (clientActivationId unique sparse index; configurationName, internalCategory, startTime, endTime indexes; tags, attributes, description, modifiedBy, createdAt, updatedAt)
- **sampleStatusBuckets**: Sample status storage (SampleStatusBucketDocument: pvName/domain/layer identity, embedded DataTimestampsDocument, firstTimeNanos/lastTimeNanos epoch-nanos scalars, statusCodes/confidence/reasons arrays, source/modifiedBy/updatedTime; indexes on (pvName, domain, layer, firstTimeNanos) and (domain, layer, firstTimeNanos))

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

### Column Document Class Hierarchy
The ingestion service uses a class hierarchy for MongoDB column document storage:

**Base Classes:**
- **`ColumnDocumentBase`**: Abstract base with `name` and `columnMetadata` fields
- **`ScalarColumnDocumentBase<T>`**: Generic base for scalar columns; holds `List<T> values`
- **`ArrayColumnDocumentBase`**: Base for array columns; binary little-endian serialization
- **`BinaryColumnDocumentBase`**: Base for binary columns (struct, image, serialized data)

**Column Types (all implemented ✅):**
- Scalar: `DoubleColumnDocument`, `FloatColumnDocument`, `Int64ColumnDocument`, `Int32ColumnDocument`, `BoolColumnDocument`, `StringColumnDocument`, `EnumColumnDocument`
- Array: `DoubleArrayColumnDocument`, `FloatArrayColumnDocument`, `Int32ArrayColumnDocument`, `Int64ArrayColumnDocument`, `BoolArrayColumnDocument`
- Binary: `StructColumnDocument` (schemaId), `ImageColumnDocument` (ImageDescriptor), `SerializedDataColumnDocument` (encoding)
- Legacy: `DataColumnDocument` (maintain for backward compatibility)

Each class uses `@BsonDiscriminator(key = "_t", value = "columnType")` and must be registered in `MongoClientBase.getPojoCodecRegistry()`.

**MongoDB POJO Codec Warning:** The codec silently skips any field missing a getter or setter — `insertMany` succeeds but the field is not written. Every instance variable on every registered BSON class must have both getter and setter.

### Column-Level Metadata
All 16 column proto types carry an optional `metadata` field (`ColumnMetadata` with `ColumnProvenance`, tags, and attributes). The ingestion service stores this as `columnMetadata` on `ColumnDocumentBase`. `ColumnDocumentBase.applyMetadataToProto()` restores it on round-trip via reflection. Validation limits: provenance fields ≤ 256 chars; ≤ 20 tags/attributes each ≤ 256 chars.

## Systematic Process for Adding New Protobuf Column Types

Seven steps for adding a new column type end-to-end:
1. **Create Document Class** — choose base class (Scalar/Array/Binary), add `@BsonDiscriminator`, implement abstract methods, add static factory method, check `hasMetadata()` and call `setColumnMetadata()` in factory
2. **Update BucketDocument** — add handling in `BucketDocument.generateBucketsFromRequest()`
3. **Register POJO Class** — add to `MongoClientBase.getPojoCodecRegistry()`
4. **Data Subscription** — add case in `SourceMonitorManager.publishDataSubscriptions()`
5. **Event Subscription** — update `ColumnTriggerUtility` and `DataBuffer` (scalar only; array/binary are targets only)
6. **Test Framework** — add field to `IngestionTestBase.IngestionRequestParams`, update `buildIngestionRequest()` and `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`
7. **Integration Test** — create `<ColumnType>IT`; scalar: single-PV pattern; array/binary: dual-PV pattern (scalar trigger + array/binary target)

**Known Technical Debt:** `createColumnBuilder()` and `addAllValuesToBuilder()` are defined at `ColumnDocumentBase` level but only apply to scalars. Future refactoring should move them to `ScalarColumnDocumentBase`.

## Export Framework Architecture
The Annotation Service includes a format-specific export framework:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **Scalar Columns**: Support all formats (CSV, Excel, HDF5) via `toDataColumn()` conversion
- **Array/Binary Columns**: HDF5 only — cannot convert to legacy DataColumn for tabular formats
- **Excel**: `DataExportXlsxFile` uses `XSSFWorkbook` (non-streaming); suitable for ~50K–100K rows
- **Import**: `DataImportUtility.importXlsxData()` in `com.ospreydcs.dp.client.utility`

## Annotation Service CRUD API Pattern

This section documents the standard pattern for implementing new CRUD APIs on the Annotation Service. `PvMetadataIT` and `ConfigurationIT` are the reference implementations.

### Full Implementation Pipeline

```
AnnotationServiceImpl (gRPC stub override)
  → validates request fields
  → calls AnnotationHandlerInterface method
  → MongoAnnotationHandler (enqueues job)
  → XxxJob.execute() (validates, queries/mutates MongoDB, dispatches result)
  → MongoAnnotationClientInterface / MongoSyncAnnotationClient (MongoDB operations)
  → XxxDispatcher (sends gRPC response to StreamObserver)
```

**Stub methods** skip the queue: `AnnotationServiceImpl` responds immediately with `RESULT_STATUS_ERROR` "not yet implemented".

### Step-by-Step: Adding a New API Method

**Step 1 — BSON Document Class** (`common/bson/<entity>/XxxDocument.java`)
- Extend `DpBsonDocumentBase` for entities with tags, attributes, createdAt, updatedAt
- Every field must have both getter and setter (POJO codec silent-drop pitfall)
- Add static factory `fromSaveXxxRequest()` and conversion method `toXxx()`
- Register in `MongoClientBase.getPojoCodecRegistry()`; embedded helper classes before parent

**Step 2 — MongoDB Collection** (`MongoClientBase.java` and `BsonConstants.java`)
- Add `COLLECTION_NAME_XXX` constant to `MongoClientBase`
- Add `BSON_KEY_XXX_*` constants to `BsonConstants`
- In `MongoSyncAnnotationClient.init()`, call `createIndex()` for key fields (idempotent)

**Step 3 — MongoClient interface and implementation**
- Add signatures to `MongoAnnotationClientInterface`
- Implement in `MongoSyncAnnotationClient`
- Add no-op stubs to `MongoAsyncAnnotationClient`

**Step 4 — Dispatcher** (`annotation/handler/mongo/dispatch/XxxDispatcher.java`)
- Extend `Dispatcher`; implement `handleValidationError()`, `handleError()`, `handleResult()`
- Not-found on get/delete → `RESULT_STATUS_REJECT` (not error)

**Step 5 — Job** (`annotation/handler/mongo/job/XxxJob.java`)
- Extend `HandlerJob`; validation in `execute()` (fail-fast), then mongoClient call, then dispatch

**Step 6 — Handler** (`annotation/handler/mongo/MongoAnnotationHandler.java`)
- Add method to `AnnotationHandlerInterface`; implement in `MongoAnnotationHandler` using `executeJob(job)`

**Step 7 — Service Implementation** (`annotation/service/AnnotationServiceImpl.java`)
- Add static `sendXxxResponseReject/Error/Success()` helpers
- Override gRPC stub method; validate, then delegate to handler

### BSON Document Base Class: `DpBsonDocumentBase`

Documents that need tags, attributes, or managed timestamps extend `DpBsonDocumentBase`:
- Inherited: `List<String> tags`, `Map<String,String> attributes`, `Instant createdAt`, `Instant updatedAt`
- **Attributes**: use `AttributesUtility.attributeMapFromList()` / `attributeListFromMap()` to convert to/from proto `List<Attribute>`
- **Timestamps**: use `TimestampUtility.getTimestampFromInstant()` when building proto responses

### Standard Conventions

**Tag normalization:** Lowercase, deduplicated, sorted on save:
```java
List<String> normalizedTags = new ArrayList<>(
    new TreeSet<>(request.getTagsList().stream().map(String::toLowerCase).toList()));
```

**Upsert with `createdAt` preservation:** On first save, set `createdAt = Instant.now()`. On update, preserve `createdAt` and set `updatedAt = Instant.now()`.

**Not-found → RESULT_STATUS_REJECT:** Get/delete that finds no record returns `RESULT_STATUS_REJECT`, not error.

**Stub methods → immediate RESULT_STATUS_ERROR:** Respond in `AnnotationServiceImpl` with "not yet implemented"; no job enqueued.

**Validation in Job.execute():** Call `dispatcher.handleValidationError(new ResultStatus(true, "message"))` and return early for each violation.

**Result wrapper classes:**
- `MongoSaveResult` — document identifier, plus `isError`/`isReject` state
- `MongoDeleteResult` — deleted document identifier, plus `isError`/`isReject` state
- `MongoCountResult` — affected-item count, plus `isError`/`isReject` state
- `PvMetadataQueryResult` — `List<PvMetadataDocument>` and `String nextPageToken`
- `ConfigurationQueryResult` — `List<ConfigurationDocument>` and `String nextPageToken`
- `ConfigurationActivationQueryResult` — `List<ConfigurationActivationDocument>` and `String nextPageToken`

### Reject vs. Error in the Mongo Client (issue #235)

A failure detected *inside* the Mongo client must be classified, not just flagged. The three result
wrappers above carry both `isError` and `isReject`, and the `Save*`/`Delete*` dispatchers route
`isReject` to `sendXxxResponseReject` ahead of the `isError` branch.

- **Reject** — the request violated a business rule: a referenced entity does not exist, or a
  constraint would be broken. Retrying the identical request is pointless, and the condition is a
  correctable mistake the caller may want to surface to a user. Build with
  `MongoSaveResult.reject(...)` / `MongoDeleteResult.reject(...)` / `MongoCountResult.reject(...)`,
  and log at `debug` — a client mistake is not a service error.
- **Error** — the service failed to handle an otherwise valid request: a `MongoException`, an
  unacknowledged write, an unexpected null id. A retry may succeed. Build with the matching
  `error(...)` factory and log at `error` with the exception object.

`isReject` implies `isError`, so callers reading only `isError` still see every failure. That
invariant is enforced by construction, not by convention: the constructor that sets `isReject` is
private on all three wrappers, so `isReject=true, isError=false` cannot be built. The public
constructor is the legacy non-reject form, retained for the ~50 untouched call sites.

Adding a business rule on these paths without the `reject(...)` factory silently reproduces the
original bug — the failure reads like a rejection but arrives as `RESULT_STATUS_ERROR`.

`MongoDeleteResult` carries two different not-found outcomes and they are not interchangeable: a
delete that simply matched nothing returns `isError=false` with a null `deletedIdentifier` (the
dispatcher converts that to a rejection), while a delete blocked by a business rule uses
`reject(...)`. `deleteConfiguration` uses both. The field is named `deletedIdentifier` rather than
`deletedPvName` because the same wrapper serves configuration and activation deletes.

**Do not classify "not found" by a helper that swallows exceptions.** `findDataSet()`/`findAnnotation()`
return null for both "absent" and "query failed", so a Mongo outage is indistinguishable from a
genuine not-found. `saveDataSet`/`saveAnnotation` therefore use the private `lookupDataSet()`/
`lookupAnnotation()` variants, which throw `DpException` on query failure — otherwise a database
outage would be reported to the caller as "your id does not exist", inverting the retry decision.

**A lookup helper must throw a *checked* exception, not an unchecked one.** `findConfigurationByName()`
and `findPvMetadataByNameOrAlias()` originally wrapped query failures in a bare `RuntimeException`,
which escaped both of their in-client callers: it is not a `MongoException`, so it slipped past
`saveConfigurationActivation`'s `catch (MongoException)`, and `deletePvMetadata` called its helper
with no catch at all. In both cases `QueueHandlerBase`'s worker caught the escapee, logged it, and
moved on — so the job never reached `dispatcher.handleResult()` and the caller's response stream
stayed open until it timed out, with no error ever sent. That is strictly worse than a
misclassified failure: the caller gets nothing to act on.

Both helpers now throw `DpException`, like the two `lookup*` helpers above, so the compiler forces
every caller to decide what a query failure means. The regression guard is
`MongoSyncAnnotationClientLookupFailureTest`, which pins each failing lookup to an error result and
each genuine absence to the not-found/reject path. Prefer a checked exception for any
Mongo-client helper whose failure must reach the client, and catch it at the call site into the
matching `error(...)` result — never let it fall through to a rejection branch, which would invert the
retry decision.

The guard against regression is on the test side: the `sendAndVerify*` wrappers for the eight
affected Save/Delete methods assert `RESULT_STATUS_REJECT` in their `expectReject` branch, and the
observers in `AnnotationTestBase` capture `getExceptionalResultStatus()` to make that possible. Before
this, `expectReject` asserted only `isError()` and a message substring, so the naming and the wire
status could — and did — diverge silently.

**Never `upsert(true)` on an `_id` filter.** An upsert filtered by natural key (pvName,
configurationName, clientActivationId) re-creates the same logical record and is fine — that is what
`savePvMetadata`, `saveConfiguration`, and `saveConfigurationActivation` do. An upsert filtered by
`_id` cannot: if the document was deleted between the lookup and the write, Mongo inserts a
*different* document under a newly generated id, having silently written data the caller never sees.
`saveDataSet`/`saveAnnotation` therefore replace without upsert and test `getMatchedCount() == 0`,
reporting that race as a rejection.

Test `matchedCount`, not `modifiedCount`, when checking whether a `replaceOne` found its target.
`modifiedCount` is also 0 when the replacement leaves the stored document unchanged, which is a
successful save. (These documents carry an always-refreshed `updatedAt`, so that case does not arise
today — but the check should not depend on that.)

### Pagination Pattern

```java
int skipOffset = (pageToken == null || pageToken.isEmpty()) ? 0
    : Integer.parseInt(new String(Base64.getDecoder().decode(pageToken)));
// collection.find(filter).sort(...).skip(skipOffset).limit(limit)
String nextPageToken = (skipOffset + results.size() < totalCount)
    ? Base64.getEncoder().encodeToString(String.valueOf(skipOffset + results.size()).getBytes())
    : null;
```

### Query Criteria → MongoDB Filter Pattern

Build a compound `Filters.and()` from criteria list:

| Criterion type | MongoDB filter |
|---|---|
| Exact match | `Filters.eq(field, value)` |
| Prefix match | `Filters.regex(field, "^prefix")` |
| Contains match | `Filters.regex(field, ".*substring.*")` |
| Tags `$in` | `Filters.in(BSON_KEY_TAGS, values)` |
| Attribute key-only | `Filters.exists("attributes." + key)` |
| Attribute key+values | `Filters.in("attributes." + key, values)` |
| Timestamp overlap | `lte(startTime, ts)` AND (`gt(endTime, ts)` OR `exists(endTime, false)`) |

Multiple match types within one criterion are combined with `Filters.or()`.

### Empty Criteria Is Match-All, and Every Query Is Bounded (issue #245)

An empty criteria list on `queryPvMetadata`, `queryConfigurations`, and
`queryConfigurationActivations` means **match-all**, not an error. The three `Query*Job` classes
deliberately have no list-level emptiness check; each carries a comment saying so, because the
absence of a validation block reads like an omission. Per-criterion validation is untouched: a
criterion that *is* supplied must still be well-formed, so "no filters requested" and "a filter was
requested but is malformed" stay distinguishable.

`MongoSyncAnnotationClient.DEFAULT_QUERY_LIMIT` (100) is applied by all three when `limit` is unset,
and **the default is unconditional** — it does not depend on whether criteria were supplied. Making
it conditional would couple page size to an unrelated request field: a client removing its last
filter would silently switch from "everything" to "first 100 with a token". There is deliberately no
unbounded path left in `executeQueryPvMetadata`; before #245 an unset limit there returned every
match with an **always-blank `nextPageToken`**, so the caller could not detect the unbounded read.
Reintroducing a `limit > 0 ? ... : 0` branch restores exactly that hazard.

Keep the constant shared across all three call sites. It replaced two hardcoded `100` literals plus
`queryPvMetadata`'s `0`, so a future change to the default cannot land on two of the three.

**This interacts with the #243 blank-criterion guard, and the interaction is subtle.** Before #245, a
blank-only criterion was observably a *rejection*: `nonBlank()` dropped the blank entry, the
criterion was omitted, and the server rejected the resulting empty criteria list. That rejection is
gone, so a blank-only query now legitimately succeeds as match-all. The #243 invariant still holds,
but it is narrower than "the caller does not receive the whole collection" — under #245 a blank-only
query and an explicit empty-criteria query both return everything. What #243 guarantees is that no
`"^" + Pattern.quote("")` regex is ever built, i.e. the server is never *asked* to filter on a blank
value. Assert that on the built request (`getCriteriaCount() == 0`), never by expecting an error:
`PvMetadataClientIT.testQueryPvMetadataBlankCriteriaEmitsNoCriterion` was rewritten for exactly this
reason when it failed during #245's implementation.

### Blank Criterion Values Must Never Reach the Server (issue #243)

A blank string in a `prefix` or `contains` criterion is a **silent match-all**, not a no-op. The
filter builder produces `"^" + Pattern.quote("")` and `".*" + Pattern.quote("") + ".*"`, and both
match every value — so a client that forwards an unfilled optional UI field retrieves the entire
collection while appearing to have applied a filter. The empty string survives protobuf
serialization as a zero-length repeated entry, so it also satisfies the server's "at least one of
exact/prefix/contains" check and slips past the empty-criteria rejection by making the criteria list
non-empty.

Like the #197 and #207 invariants, the failure mode is a **wrong answer rather than an error**, which
is why the guard belongs at the point where a criterion is built rather than in a validator.

`AnnotationClient.nonBlank()` is the single source for this: it drops blank *and* null entries (the
latter because protobuf's `addAll` throws `NullPointerException` on a null element), and every
criterion list is both guarded and populated through it. A criterion whose entries are all blank is
therefore omitted entirely rather than emitted empty. `TextMatch.isEmpty()` is defined in the same
terms — were it to use a plain `isEmpty()` on the lists, a blank-only `TextMatch` would pass the
guard and emit a criterion with all three lists empty, which the server rejects.

Checking a criterion list with a plain null/empty test reintroduces the bug. There is deliberately no
weaker helper in `AnnotationClient` to reach for.

Attribute keys have a milder version of the same problem: the three `Query*Job` classes validate
`AttributesCriterion.key` with `isBlank()`, so a whitespace key is an avoidable `REJECT` rather than
an omitted filter. `isBlankKey()` guards those three sites.

### Overlap Constraint Pattern (ConfigurationActivation)

`saveConfigurationActivation` enforces that no two activations for the same `configurationName` or `internalCategory` have overlapping time intervals. The `overlapExists()` method in `MongoSyncAnnotationClient` runs two `countDocuments()` queries (one per dimension). The overlap condition for an existing record [S, E] against a new interval [newS, newE] is:
- `existing.startTime < newEndTime` (or newEndTime absent) AND
- `existing.endTime > newStartTime` OR `existing.endTime` absent

The record being updated is excluded from the check via `Filters.ne(clientActivationId, excludeId)`.

`internalCategory` is denormalized from the referenced `Configuration.category` at save time. Category changes on a `Configuration` are blocked if any activations exist for it.

### Adding a New MongoDB Collection

1. Add `COLLECTION_NAME_XXX` to `MongoClientBase`
2. Add `BSON_KEY_XXX_*` constants to `BsonConstants`
3. In `MongoSyncAnnotationClient.init()`, get collection and call `createIndex()` for key fields
4. Store collection reference as instance field on `MongoSyncAnnotationClient`
5. Register `XxxDocument.class` in `MongoClientBase.getPojoCodecRegistry()`

## Code Style Guidelines
- Java 21 is used for this project
- MongoDB is used for persistence with embedded protobuf serialization
- Package structure: `com.ospreydcs.dp.service.<component>`
- Follow existing naming conventions (CamelCase for classes, lowerCamelCase for methods)
- API method implementations follow: Handler → Job → Database Client → Dispatcher pattern
- Jobs named as `<APIMethod>Job`, Dispatchers as `<APIMethod>Dispatcher`
- Error handling uses DpException and structured logging
- **Exception logging convention (new as of #191):** when logging a caught exception, pass the exception object as the final `logger` argument so the stack trace is captured — e.g. `logger.error("methodName database error: {}", ex.getMessage(), ex)`. Older code logs `ex.getMessage()` only (no trace); migrate those to include `ex` as you touch them.
- Integration tests located in `integration.<service>` packages
- Follow existing patterns for protobuf ↔ MongoDB document conversion
- Result objects use `ResultStatus` class with `isError` (Boolean) and `msg` (String) fields

## API Method Naming Conventions
- `saveXxx` — upsert (create or update) by natural key
- `queryXxx` — search with filter criteria; returns a list, paginated
- `getXxx` — single-record lookup by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `deleteXxx` — remove by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `patchXxx` / `bulkSaveXxx` — reserved for partial update and bulk operations; implement as stubs ("not yet implemented") until ready
- Legacy "create" references should be updated to "save" when encountered

## Ingestion Validation Framework
`IngestionValidationUtility` performs layered validation:
1. Basic request (provider ID, request ID, frame presence)
2. Timestamps (SamplingClock and TimestampList, ordering checks)
3. Legacy columns (DataColumn, SerializedDataColumn)
4. New columns (all column-oriented types)
5. Cross-cutting (unique PV names across all column types in a frame)

**Constraints:** string values ≤ 256 chars; array dimensions 1–3 (all > 0); ≤ 10M array elements; image ≤ 50MB; struct ≤ 1MB; timestamps non-decreasing, nanos 0–999,999,999; sample count must match timestamp count; bucket time span ≤ `Buckets.maxBucketSpanSeconds` (default 86400) — this invariant lets the query-side bucket overlap filter add a `firstTime` lower bound (`BucketSpanLimits`, issue #197), so never relax it query-side without ingestion-side enforcement.

### Max Bucket Span Invariant (issue #197)
`Buckets.maxBucketSpanSeconds` is a shared invariant between ingestion and query, and both of its failure modes are *silent wrong answers* rather than errors — treat it accordingly:
- **`BucketSpanLimits`** — single source for the limit. Value is validated once (rejects non-positive, and anything above `MAX_CONFIGURABLE_SPAN_SECONDS` where the nanos conversion would overflow) and cached; invalid config throws `DpRuntimeException`.
- **Ingestion** enforces the limit for *new* data only, via `IngestionValidationUtility`.
- **Query** adds the `firstTime` lower bound only when the stored archive is known to comply. `BucketSpanVerifier` checks this and records the outcome in the `bucketSpanVerification` collection, so the scan runs once per limit value rather than every restart. On violation or error the bound is disabled process-wide (`BucketSpanLimits.disableQueryLowerBound()`) and queries degrade to the slower unbounded scan — correct but slow, never fast but wrong.
- `verifyBucketSpans()` lives on `MongoSyncClient` because the flag it controls is **process-wide** and more than one service issues bucket time-range queries: the query service directly, and the annotation service through dataset export (`executeDataBlockQuery`). Any new service that queries buckets must call it from its handler's `init_()`, or that process will apply the bound unverified.
- Disable the check with `Buckets.verifyBucketSpansOnStartup: false` only when compliance has been confirmed independently. Off by default under test.
- **Never sample** as a shortcut for this check: over-long buckets are typically rare, so a sample that misses them reports a false all-clear.

### Bucket Deserialization Must Fail as `DpException`
The query dispatchers (`QueryDataDispatcher`, `QueryDataStreamDispatcher`, `QueryDataBidiStreamDispatcher`, `QueryBuckets*Dispatcher`) catch **only `DpException`** around bucket deserialization. Any other exception escapes the dispatch loop and terminates the response stream, so the client receives **zero buckets instead of an error** — indistinguishable from "no data in range."

`BucketDocument.dataBucketFromDocument()` / `dataBucketFromDocumentV2()` therefore validate required fields up front and wrap any `RuntimeException` as `DpException`. Preserve that contract when adding deserialization logic: a malformed stored document must produce a reportable error, never an unchecked throw. In tests, insert fully-populated `BucketDocument`s (see `MongoTestClient.insertBucketDocument()`) rather than hand-rolled partial BSON.

Because a malformed bucket blocks every query covering it, `BucketSpanVerifier` also scans for buckets missing `dataColumn`/`dataTimestamps` — in the same pass as the span check, since both must visit stored buckets (measured ~20% over the span check alone). A corrupt bucket is reported with its id, PV, and missing field, but does **not** disable the query lower bound: corruption and the span invariant are independent. The verification marker is not recorded while corruption exists, so an unrepaired bucket keeps being reported on each startup rather than going quiet after the first.

### querySamples Fragment Clamp Invariant (issue #207)
A `querySamples` request with a `ConfigurationSelector` resolves to a set of **disjoint** retrieval fragments. Two filters must agree on that fragment set, and they run at different granularities:

- **Database (bucket granularity)** — `MongoSyncQueryClient.executeQuerySamplesV2()` builds a per-fragment `$or` of bucket-overlap predicates.
- **Assembly (sample granularity)** — `TabularDataUtility.addBucketsToTable()` retains a sample only when it falls inside *some* fragment.

**Both must derive their fragments from `TimeInterval.clampToWindowBegin()`** — the single source for clamping each fragment to the page window begin. Do not reimplement the clamp at either call site. When the two disagree, the database returns buckets the assembly then keeps samples from incorrectly, which is exactly issue #207: a bucket spanning the gap between two fragments passes the bucket-level filter, and its in-gap samples were never trimmed.

Like the max-bucket-span invariant, the failure mode is a **silent wrong answer** — out-of-interval data in an otherwise normal-looking result, not an error. Two related traps:

- **Never collapse the fragments** into a single `[min begin, max end)` window for sample filtering; that window spans the gaps. `computeWindowBegin()` deliberately returns only a begin — there is no correct single upper bound. Do not reintroduce a window end.
- **`TimestampDataMap.getColumnIndex()` is a mutator.** It appends unseen names to the list that determines the emitted/exported column set, so it must be called for every column regardless of whether any sample survives trimming — otherwise a PV with no in-range samples is silently dropped instead of emitted as an all-empty column. `addColumnsToTable()` registers columns up front for this reason; `AnnotationCalculationsIT` (16 columns expected) is the regression guard.

## Sample Status API (issue #238)

The Annotation Service implements the Sample Status API (`saveSampleStatuses`, `querySampleStatuses`,
`querySampleStatusesStream`, `deleteSampleStatuses`; the two domain-registry methods are deferred
stubs). An individual status is keyed by **(pvName, timestamp, domain, layer)** at nanosecond
precision; storage is the `sampleStatusBuckets` collection.

### Storage invariant: no duplicate identity keys
No two documents may ever assert a status for the same identity key. The save path maintains this
with a **carve-and-insert upsert** (`MongoSyncAnnotationClient.saveSampleStatuses()`): exactly-colliding
timestamps are carved out of existing overlapping documents (via `SampleStatusDocumentUtility.removeTimestamps()`),
then the incoming column is inserted whole, preserving its axis representation. Documents whose spans
overlap but whose timestamps don't collide are left untouched, provenance intact. Carve rewrites happen
**before** the insert so a mid-write failure can never leave duplicate keys (partial persistence on
error is documented API behavior). Rewritten documents take the incoming save's source/modifiedBy and
a fresh server-set updatedTime; delete-path trims keep the original provenance (deletion is not a save).

### Key semantics
- **Delete is exact at the sample axis** `[beginTime, endTime)`: boundary documents are trimmed/split
  via `removeRange()` (an evenly spaced surviving run re-emits as a SamplingClock, so an interior
  delete splits a clock document into two clocks); counts are individual statuses, not documents.
- **Query returns boundary buckets whole** (span-overlap test `firstTimeNanos < end AND lastTimeNanos >= begin`),
  ordered by (pvName, domain, layer, firstTimeNanos) — a total order under the storage invariant.
- **Keyset paging** (`SampleStatusPageToken`): the token encodes the last-returned sort position, not a
  skip offset (documents are rewritten in place, so offsets drift). Unparseable tokens are **rejected**
  per the contract — unlike pvMetadata/configuration, which silently reset to page 0.
- **No maximum document span**: sparse labeling over an arbitrarily wide range is first-class, so a
  status frame has no `maxBucketSpanSeconds`-style cap and **no #197-style firstTime lower bound may
  ever be added** to sampleStatusBuckets overlap queries.
- Validation lives in `SampleStatusValidationUtility` (whole-request reject; strictly increasing
  TimestampLists — equal timestamps would collapse identity keys).
- Config keys (`AnnotationHandler` section, in **both** application.yml files):
  `sampleStatusQueryDefaultPageSize` (10000), `sampleStatusQueryMaxPageSize` (100000, silent clamp),
  `sampleStatusSaveMaxStatuses` (1000000 per-request cap).

### QuerySpec.sampleStatusSelector (Query V2)
Supported by `querySamples`/`querySamplesStream` only; `QueryV2Resolver` **rejects** it on
bucket-oriented methods (whole storage buckets cannot represent per-sample filtering). The validated
selector is carried as `ResolvedStatusFilter` on `ResolvedQuery`;
`MongoSyncQueryClient.resolveSampleStatusTimestamps()` fetches per-PV matching-timestamp sets over the
same clamped page window as bucket retrieval, and `TabularDataUtility.SampleStatusFilter` applies the
per-sample test during assembly (INCLUDE keeps iff labeled at the **exact** timestamp; EXCLUDE drops
iff labeled). Composition with `configurationSelector` is by intersection — both the fragment retention
test and the status test are applied in the same per-sample retention decision. A DB error or corrupt
status document during the join surfaces as `DpException`/error, never as "no statuses" (in EXCLUDE
mode that would silently return filtered-out samples). Filtered samples are simply never inserted into
the `TimestampDataMap`, so missing values and all-PVs-filtered row omission fall out of the existing
representation; the `getColumnIndex()` registration invariant still guarantees all-filtered PVs emit
all-empty columns.

## Performance Benchmarking Framework
Benchmarks in `com.ospreydcs.dp.service.ingest.benchmark`:
- **`BenchmarkIngestDataStream`** / **`BenchmarkIngestDataBidiStream`**: compare `DATA_COLUMN` (legacy), `DOUBLE_COLUMN`, and `SERIALIZED_DATA_COLUMN` strategies
- Use `--double-column` or `--serialized-column` flags; `--help` for usage
- Key parameters: `numThreads=7`, `numStreams=20`, `numRows=1000`, `numColumns=200` (4000 PVs total), `numSeconds=60`

## Testing Strategy
- **Framework**: JUnit 4 (`@Test`, `@Before`, `@After`)
- **Integration Tests**: `src/test/java/com/ospreydcs/dp/service/integration/`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase`
- **Test Database**: "dp-test" (cleaned between tests via `MongoTestClient.init()`)
- **Temporary Files**: `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();`

### Annotation Service Test Framework

Integration tests follow a layered structure:
- **`AnnotationTestBase`** — request builders, `*Params` records, and `*ResponseObserver` inner classes for each API method
- **`AnnotationIntegrationTestIntermediate`** — starts the service and wires up the wrapper
- **`GrpcIntegrationAnnotationServiceWrapper`** — `sendAndVerifyXxx()` helpers that send a request, await response, and assert success/failure

**Response observer pattern:** Each `XxxResponseObserver` holds a `CountDownLatch`, `AtomicBoolean isError`, and result list. `onNext()` spawns a thread to process the response and count down the latch; `onError()` sets the error flag and counts down. `await()` uses a 1-minute timeout.

**`sendAndVerifyXxx()` pattern:** Starts a thread to call the async stub, awaits the observer, asserts `isError()`/`getErrorMessage()` for failure cases or extracts and returns the key identifier for success.

**`MongoTestClient` pattern:** Add `findXxx(String key)` following the retry-loop pattern (300 retries × 100ms = 30s max) to handle asynchronous worker-thread insertion.

**Integration test structure** (`PvMetadataIT` and `ConfigurationIT` are reference implementations):
- Extend `AnnotationIntegrationTestIntermediate`
- Group tests by operation: save, query (all criterion types), get, delete, stubs
- Use `sendAndVerify*` wrappers for happy path and error cases
- Use `MongoTestClient.findXxx()` to verify DB state after saves
- For pagination, use `DpAnnotationServiceGrpc.newStub(channel)` directly with an inline `StreamObserver` and `CountDownLatch`

### Ingestion Test Framework
- **`IngestionTestBase.IngestionRequestParams`**: holds a dedicated `List<XxxColumn>` field for each column type
- **`buildIngestionRequest()`**: populates `IngestDataRequest` from params fields
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`**: verifies all column types via `toProtobufColumn()` round-trip
- **Scalar tests**: single-PV pattern (`DoubleColumnIT`, etc.)
- **Array/Binary tests**: dual-PV pattern — scalar trigger + array/binary target (`DoubleArrayColumnIT`, `StructColumnIT`, etc.)

**`IngestionRequestParams` has one all-positional constructor with ~12 same-typed arguments, called
from 62 sites across 26 files.** Adding or removing a parameter is therefore never a local change,
and because most arguments are `null` literals of similar types, a wrong-position argument compiles
silently. Two consequences when touching it (learned in #252, which removed `valuesStatus`):

- Change every call site in the same commit; there is no overload to absorb the difference, unlike
  `IngestionClient.IngestionRequestParams`, which carries a shorter delegating constructor.
- The class hand-writes `equals`, `hashCode`, and `toString` over its full field list. A field
  removed from the declarations but left in those three still compiles — it is the *field*, not a
  type — so grepping for the proto type name misses them. Grep the lowercase field name too.

Prefer adding new state as a chained setter (as `setColumnMetadata()` does) over extending the
positional list.

### Ingestion Validation Test Coverage
- `IngestionValidationUtilityTest` (22 test cases): legacy validation, new column types, duplicate PV names, timestamp integrity

## Continuous Integration
- **GitHub Actions**: `.github/workflows/ci.yml`
- **Multi-Repository Setup**: builds dp-grpc before dp-service
- **Triggers**: pushes/PRs to main/master; manual workflow dispatch
- **Services**: MongoDB 8.0 service container
- **Artifacts**: Surefire and Failsafe test reports

### Vendored dependency: `cisd:jhdf5` (do not remove)

`cisd:jhdf5` is **not on Maven Central**; its only public host is `maven.scijava.org`. The jar and
its POM are committed under `third-party/cisd-jhdf5/` and installed into the runner's local Maven
repository by a CI step that runs before any build. Deleting either the directory or that step
breaks CI on every pull request.

This exists because on 2026-08-27 SciJava began returning **503 for JAR downloads while still
serving POMs**, making the dependency unresolvable with no Central fallback. The host has since
recovered, which is *not* a reason to remove the vendored copy: GitHub Actions caches are **scoped
per ref**, so a PR branch can read only its own caches and the default branch's. Every cache in this
repo was created on a `refs/pull/NNN/merge` ref and none on `main`, meaning no PR can reuse another
PR's cache — every PR was resolving this jar from the network, and a single-host dependency with no
mirror will break CI again the next time that host has trouble.

The failure reads like a transient outage ("could not transfer... 503"), so the tempting response is
to re-run the job. That does not help; it only passes if the run happens to restore a cache.

`third-party/cisd-jhdf5/README.md` records the checksums, the Apache-2.0 licensing (the POM declares
`<distribution>repo</distribution>`, permitting redistribution), and the two conditions under which
this may be removed — jhdf5 reaching Central, or the project dropping the HDF5 export path.
