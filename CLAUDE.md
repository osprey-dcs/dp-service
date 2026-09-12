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
- **pvStats**: Per-PV ingestion statistics (issue #232), one document per PV keyed by name as `_id`; today `maxBucketSpanSeconds`, the largest `lastTime.seconds - firstTime.seconds` ever ingested for the PV, written by the ingestion service and read per query to set the bucket overlap filter's `firstTime` lower bound (see Per-PV Bucket Span Bound). Default `_id` index only; further per-PV statistics (#201) belong on the same document
- **serviceMetadata**: Service-level markers, currently the `schemaVersion` document recording the applied schema version, its audit list, and the in-progress migration claim (see Schema Migration below)
- **sampleStatusBuckets**: Sample status storage (SampleStatusBucketDocument: pvName/domain/layer identity, embedded DataTimestampsDocument, firstTimeNanos/lastTimeNanos epoch-nanos scalars, statusCodes/confidence/reasons arrays, source/modifiedBy/updatedTime; indexes on (pvName, domain, layer, firstTimeNanos) and (domain, layer, firstTimeNanos))

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

### Column Document Class Hierarchy
Ingestion buckets and calculations frames (the latter as of #248 Phase 4) share a class hierarchy for MongoDB column document storage:

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
All 16 column proto types carry an optional `metadata` field (`ColumnMetadata` with `ColumnProvenance`, tags, and attributes). The ingestion service stores this as `columnMetadata` on `ColumnDocumentBase`. `ColumnDocumentBase.applyMetadataToProto()` restores it on round-trip via reflection. Validation limits: provenance fields ≤ 256 chars; ≤ 20 tags/attributes each ≤ 256 chars. The limits live in the shared `ColumnMetadataValidationUtility` (`common/handler`), parameterized on the request field path; `IngestionValidationUtility` delegates to it, and any new save path accepting `ColumnMetadata` must too.

Provenance `derivedFrom` links (#248 Phase 4, D29) are **stored as supplied**: `ColumnSourceDocument` (pvName oneof arm, or embedded `CalculationsColumnDocument`; optional timeRange as begin/end `TimestampDocument`s) round-trips the proto exactly, including a pvName arm set to the empty string (dispatch is on the oneof case, not field emptiness). No existence checks and no ObjectId parse of `calculationsId` — links may point at records not yet created and may dangle; only the shared length limits apply. Storing them on the document is also what keeps the legacy `DataColumnDocument` read path lossless: `applyMetadataToProto()` overwrites the complete in-bytes metadata with the document version, so any provenance field missing from the document silently vanishes on every `toProtobufColumn()` read.

### Calculations Typed Columns (issue #248 Phase 4)

`CalculationsDataFrameDocument.dataColumns` is the polymorphic `List<ColumnDocumentBase>` (D25),
under the original BSON field name and matching the `BucketDocument.dataColumn` pattern: every
post-1.13 stored document reads unchanged, and the `_t` discriminator round-trips the concrete
type. Columns enter through the shared `ColumnDocumentUtility.fromDataFrame()` dispatch and are
restored to their typed `DataFrame` repeated fields by `addColumnToDataFrame()`. A consumer that
needs the legacy `DataColumn` view must narrow — `ScalarColumnDocumentBase`/`DataColumnDocument`
via `toDataColumn()`, anything else `NonScalarColumnException` (see Export Framework Architecture
for how that is classified).

**Adding `@BsonDiscriminator` to a class with instances already stored embedded requires a
migration stamping the stored copies.** The POJO codec writes `_t` even when the declared field
type is concrete, but a stored entry *without* `_t` decodes only under a concrete declared type —
under an abstract one it throws `CodecConfigurationException`, which on the query path reads as
silently empty results, not an error. Schema migration v4 (`V4StampColumnDiscriminators`) stamps
`_t: "dataColumn"` on discriminator-less legacy columns in `buckets` and `calculations` alike —
the migration #173 should have shipped. Idempotent by construction (both `updateMany` filters
test for the absent key), and a one-time full scan of `buckets`; the startup choreography while
it runs (other services time out the five-minute claim wait and rely on supervisor restart) is
documented in `doc/schema-migration.md`.

`validateSaveAnnotationRequest` validates the full frame shape (D28): every column of every type
needs a non-blank name, non-empty values, and a value count equal to the frame's timestamp count
(array columns: timestamp count × dims product; `SerializedDataColumn` entries carry no countable
values and get name/metadata checks only). Column names are unique across all column types within
a frame, frame names unique within the Calculations object — both are addressing keys for
`CalculationsSpec`, tabular "frameName/columnName" column naming, and provenance links. The count
check is what closes the finding-5 export hang (an out-of-range read escaping unchecked in the
worker thread left the export stream hanging with no response); do not exempt a new column type
from it. Column metadata is validated through the shared `ColumnMetadataValidationUtility` like
every other save path, and column *values* against the shared `ColumnValueLimits` caps (string
values ≤ 256 chars, array dims product ≤ 10M elements, image ≤ 50MB, struct ≤ 1MB) — one contract
with ingestion, so a payload ingestion would reject cannot be stored through saveAnnotation.
Ingestion's identity-field requirements (enumId, schemaId, imageDescriptor, serialized encoding)
deliberately remain ingestion-only: calculations columns are not PV channels.

## Systematic Process for Adding New Protobuf Column Types

Seven steps for adding a new column type end-to-end:
1. **Create Document Class** — choose base class (Scalar/Array/Binary), add `@BsonDiscriminator`, implement abstract methods, add static factory method, check `hasMetadata()` and call `setColumnMetadata()` in factory
2. **Update Column Dispatch** — add a branch in `ColumnDocumentUtility.fromDataFrame()`, the shared dispatch every path that stores DataFrame columns flows through (`BucketDocument.generateBucketsFromRequest()` and, as of #248 Phase 4, calculations frames); do not add a per-path dispatch
3. **Register POJO Class** — add to `MongoClientBase.getPojoCodecRegistry()`
4. **Data Subscription** — add case in `SourceMonitorManager.publishDataSubscriptions()`
5. **Event Subscription** — update `ColumnTriggerUtility` and `DataBuffer` (scalar only; array/binary are targets only)
6. **Test Framework** — add field to `IngestionTestBase.IngestionRequestParams`, update `buildIngestionRequest()` and `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`, and add the type's case to `AnnotationTestBase.parseColumnByEncoding()` (HDF5 export verification)
7. **Integration Test** — create `<ColumnType>IT`; scalar: single-PV pattern; array/binary: dual-PV pattern (scalar trigger + array/binary target)

**Known Technical Debt:** `createColumnBuilder()` and `addAllValuesToBuilder()` are defined at `ColumnDocumentBase` level but only apply to scalars. Future refactoring should move them to `ScalarColumnDocumentBase`.

## Export Framework Architecture
The Annotation Service includes a format-specific export framework:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **Scalar Columns**: Support all formats (CSV, Excel, HDF5) via `toDataColumn()` conversion
- **Array/Binary Columns**: HDF5 only — cannot convert to legacy DataColumn for tabular formats (dataset PVs and calculations columns alike; tabular requests containing them are rejected, see below)
- **Excel**: `DataExportXlsxFile` uses `XSSFWorkbook` (non-streaming); suitable for ~50K–100K rows
- **Import**: `DataImportUtility.importXlsxData()` in `com.ospreydcs.dp.client.utility`

### Export Classification and Typed Columns (issue #248 Phase 4)

- **Client mistakes are rejections (D30), carried on `ExportDataStatus`.** The nested record
  follows the #235 wrapper convention — `isReject` implies `isError`, enforced by its compact
  constructor; `reject(...)` is the only way to build a rejection, and the two-arg constructor
  remains the legacy non-reject form for existing call sites. `execute()` routes `isReject` to
  `ExportDataDispatcher.handleReject()` → `RESULT_STATUS_REJECT`, logged at debug. Rejected: a
  malformed or not-found `dataSetId`/`calculationsId`, a `dataFrameColumns` filter naming a frame
  or column the calculations object lacks, and non-scalar content in a tabular export. Errors
  stay errors: lookup `DpException` (outage), file I/O, the export size limit — and "data block
  query returned no data" stays an error (pre-existing, deliberately not reclassified).
- **Tabular non-scalar content must surface as the reject, never a hang (D33).**
  `ExportDataJobAbstractTabular` catches `NonScalarColumnException` ahead of `DpException` on
  both the dataset-bucket and calculations paths, phrasing "…export to HDF5 instead"; for a
  calculations column the exception's pvName slot carries "frameName/columnName". Independently,
  `ExportDataJobBase.execute()` wraps `exportData_()` in a `RuntimeException` catch dispatched as
  an error — a column stored before D28's count validation can still throw unchecked during
  assembly, and an escaped exception is swallowed by `QueueHandlerBase`, hanging the caller's
  stream.
- **HDF5 calculations columns are self-describing (D32).** `writeCalculations` writes each
  column's protobuf bytes plus a `dataColumnEncoding` tag (`"proto:" + <proto message simple
  name>`), the scheme bucket data has always used. Files from earlier builds carry no tag on
  calculations columns and are implicitly DataColumn-encoded — a point-in-time artifact, not a
  compatibility mechanism. `AnnotationTestBase.parseColumnByEncoding()` is the single tag→parser
  dispatch for both the bucket and calculations HDF5 verifiers; a new column type needs a case
  added there (Systematic Process step 6).
- **Inline `dataBlocks` are an export source (D31)**: validated like `saveDataSet` blocks,
  appended after any stored dataset's blocks into one effective, never-persisted
  `DataSetDocument`. At least one of `dataSetId`/`dataBlocks`/`calculationsSpec` is required; an
  inline-only export's output file is keyed by a generated ObjectId.

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

**Tag normalization:** Lowercase, deduplicated, sorted on save — the shared helper is
`DpBsonDocumentBase.normalizedTags()`, and as of the #248 Phase 2 review fixes every tagged
entity's `fromSaveRequest` factory calls it (pvMetadata, configuration, configurationActivation,
dataSet, annotation — the first three previously carried inline copies, which is exactly the drift
a shared helper exists to prevent). Lowercasing is `Locale.ROOT`: the default-locale overload folds
case differently under some locales (Turkish dotless i), making the stored form — and what a
`TagsCriterion` value can match — depend on the server JVM's locale. Annotations previously stored
tags as-given, so schema migration v2 (`V2NormalizeAnnotationTags`) normalizes stored annotation
tags, lowercasing the same way — without it, a stored mixed-case tag is unreachable by any
normalized `TagsCriterion` value, a #197-class silent wrong answer. Any new save path for a tagged
entity must normalize through the shared helper, and any diff/verify helper must compare against
the normalized form.

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
- `DataSetQueryResult` — `List<DataSetDocument>` and `String nextPageToken`
- `AnnotationQueryResult` — `List<AnnotationDocument>` and `String nextPageToken`

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

### DataSet / Annotation / Calculations CRUD invariants (issue #248 Phase 2)

Phase 2 implemented `getDataSet`, `getAnnotation`, `getCalculations`, `deleteDataSet`,
`deleteAnnotation`, and the `patchDataSet`/`patchAnnotation` deferred stubs, plus audit/entity
fields (`modifiedBy`, `createdTime`/`updatedTime` emission, `DataSet` tags/attributes). The
invariants that outlive the ticket:

- **A malformed ObjectId in a get/delete request is a REJECT, validated in the job** before any
  client call, via the shared `AnnotationValidationUtility.validateRequiredObjectId()` (blank check
  + `ObjectId.isValid()`) — new id-keyed jobs (Phase 4 patch included) must call it rather than
  hand-rolling the block. Unvalidated, the `ObjectId` constructor throws `IllegalArgumentException`
  inside the worker thread, where `QueueHandlerBase` swallows it and the caller's stream hangs.
  `validateSaveAnnotationRequest` applies the same check to `dataSetIds`/`annotationIds` entries.
  (The save methods' *internal* lookups still classify a malformed id as error — documented
  divergence, predating Phase 2.)
- **`deleteDataSet` is rejected while any annotation references the dataset**; the rejection names
  one referencing annotation id plus the total count (one id is enough to act on, the count says
  whether to expect more, the message stays bounded).
- **`deleteAnnotation` cascades to the annotation's calculations document** (lifecycle belongs to
  the owner) and is NOT blocked by incoming `annotationIds`/provenance references — soft links may
  dangle. The annotation is deleted **before** its calculations: a failure between the two leaves a
  harmless orphan rather than a live annotation whose dangling `calculationsId` would break
  `getAnnotation`. Do not reverse that order.
- **`getAnnotation` is the only method that populates `Annotation.calculations`**, and a
  `calculationsId` that resolves to no document is an ERROR, never silently-empty content — the
  annotation asserts calculations exist, so absence is corruption.
- **`SaveAnnotationResult.calculationsId` is returned whenever the request carried calculations**
  — it is the addressing key for `getCalculations`, `CalculationsSpec`, and provenance links.
- **`saveAnnotation()` (client) deletes a replaced or cleared annotation's previous calculations
  document** (D8/D14) after a successful replace — the cleanup lives in the client beside the
  lookup that captures the previous document, matching where `deleteAnnotation`'s cascade lives.
  Cleanup failure logs the orphaned id but does not fail the response — the save succeeded, and a
  retry cannot remove the orphan.
- **A rejected save compensates for the calculations document it just inserted** (`SaveAnnotationJob`):
  both reject paths in `saveAnnotation()` fire before any annotation write, so the job deletes the
  freshly inserted document rather than orphaning it. On an *error* the write state is ambiguous —
  deleting could dangle a live annotation's `calculationsId` (the D16 corruption) — so the job logs
  the possibly-orphaned id instead. Do not "clean up" on the error branch.
- **`validateSaveAnnotationRequest` throws `DpException` on a lookup failure**, which the job
  dispatches as `RESULT_STATUS_ERROR`; the `ResultStatus` return carries only genuine validation
  rejections. Folding a lookup failure into `ResultStatus` routes it through
  `handleValidationError()` → REJECT, inverting the retry decision (#235) at the wire-status level
  even when the message says "error".
- **Annotation reference ids are stored canonical** — `dataSetIds`/`annotationIds` are lowercased
  to `ObjectId.toHexString()` form on save, and `deleteDataSet`'s reference check plus the
  queryAnnotations dataSets/annotations criteria canonicalize their inputs to match. These checks
  match *strings* while validation parses *binary* ObjectIds (hex-case-insensitive), so a
  case-variant id would otherwise pass validation yet bypass every reference check — deleting a
  dataset a stored annotation still references. Schema migration v3
  (`V3CanonicalizeAnnotationReferenceIds`) canonicalizes previously stored references.
- **`deleteDataSet`'s reference check-then-delete is not atomic** with a concurrent
  `saveAnnotation` (multiple workers, no transactions): a validated save can commit a reference
  after the count. Accepted v1 limitation, documented at the check like `overlapExists()`.
- **`updatedTime` stays unset on create** for all entity types; it is set on the first
  full-replace update, with `createdAt` preserved. An absent `updatedTime` means "never updated".
- Lookup helpers: `lookupDataSet`/`lookupAnnotation`/`lookupCalculations` are the interface-level
  throwing variants (`DpException` on query failure, null only for genuine absence); the `find*`
  variants collapse both to null and remain only for callers that cannot act on the distinction.
  `MongoSyncAnnotationClientLookupFailureTest` pins the classification for the new paths too.
- Test-side: asserting a document was **deleted** must use the `findXxxNoRetry` variants on
  `MongoTestClient` — the retry finders wait ~30s before reporting absence.

### Pagination Pattern

Two token families exist, split by API generation (#248 Phase 3, plan D6/D18):

- **Skip tokens** — `queryPvMetadata`, `queryConfigurations`, `queryConfigurationActivations`:
  Base64 of a decimal skip offset, decoded by `decodePageTokenSkip()` and applied by
  `applySkipPaging()` on `MongoSyncAnnotationClient` (limit+1 probe, trim, re-encode). An
  unparseable token silently resets to page 0. Converting these to opaque reject-on-malformed is
  a follow-on.
- **Keyset tokens** — `queryDataSets`, `queryAnnotations`: `AnnotationQueryPageToken`, Base64 JSON
  of `{query, lastId}`; resume filters `_id > lastId` and `applyKeysetPaging()` emits the next
  token from the last returned document's id. The **jobs** decode and REJECT malformed tokens
  before the client call (sample-status pattern); the client method takes the decoded
  `ObjectId resumeAfterId`. The `query` discriminator makes a cross-query token a rejection —
  without it, a queryDataSets token pasted into queryAnnotations decodes cleanly and silently
  skips results. `querySampleStatuses` uses the same keyset scheme with its own token type
  (`SampleStatusPageToken`).

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

Criteria list entries combine with **AND**; values within one criterion OR (#248 Phase 3, plan
D4 — the legacy two-bucket AND/OR scheme is gone). At most one `TextCriterion` is accepted per
request, rejected in `AnnotationServiceImpl` validation: two `$text` clauses cannot be ANDed
(Mongo fails the query with "Too many text expressions", so the client mistake would otherwise
surface as `RESULT_STATUS_ERROR` — the #235 inversion). `queryConfigurationActivations` sorts
(`startTime`, `configurationName`, `_id`) — startTime alone is not unique, and under skip paging
a non-total order drops or duplicates rows at page boundaries.

### Empty Criteria Is Match-All, and Every Query Is Bounded (issue #245)

An empty criteria list on `queryPvMetadata`, `queryConfigurations`, `queryConfigurationActivations`,
and — since #248 Phase 1 — `queryDataSets` and `queryAnnotations` means **match-all**, not an error.
The `Query*Job` classes (and the two annotation-query validation switches in `AnnotationServiceImpl`)
deliberately have no list-level emptiness check; each carries a comment saying so, because the
absence of a validation block reads like an omission. Per-criterion validation is untouched: a
criterion that *is* supplied must still be well-formed, so "no filters requested" and "a filter was
requested but is malformed" stay distinguishable.

`MongoSyncAnnotationClient.DEFAULT_QUERY_LIMIT` (100) is applied by all five when `limit` is unset,
and **the default is unconditional** — it does not depend on whether criteria were supplied. Making
it conditional would couple page size to an unrelated request field: a client removing its last
filter would silently switch from "everything" to "first 100 with a token". There is deliberately no
unbounded path left in `executeQueryPvMetadata`; before #245 an unset limit there returned every
match with an **always-blank `nextPageToken`**, so the caller could not detect the unbounded read.
Reintroducing a `limit > 0 ? ... : 0` branch restores exactly that hazard.

Keep the constant shared across all call sites — it replaced hardcoded literals so a future change to
the default cannot land on a subset — and keep the probe/token mechanics in the shared
`applySkipPaging()`/`applyKeysetPaging()` helpers on `MongoSyncAnnotationClient` (see Pagination
Pattern above for which queries use which). Both helpers guard `limit + 1` against int overflow
(proto `uint32` limit of `Integer.MAX_VALUE`); a hand-rolled paging block reintroduces both the
drift and the overflow.

**queryDataSets/queryAnnotations reject blank criterion entries** (`RESULT_STATUS_REJECT`) rather
than dropping them — a dropped blank entry turns the criterion into a silent match-all (#243 class),
and a malformed `IdCriterion` ObjectId would otherwise throw `IllegalArgumentException` inside the
worker thread, where `QueueHandlerBase` swallows it and the caller's stream hangs with no response.
Validation in `AnnotationServiceImpl` checks `ObjectId.isValid()` on ids and rejects blank entries in
every criterion value list; `MongoSyncAnnotationClient` then builds filters without re-filtering,
via the shared `MongoQueryFilterBuilder` helpers. (The pvMetadata/configuration queries instead rely
on the #243 client-side guard plus blank-exact-matches-nothing semantics — a known asymmetry.)

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

Like the #232 and #207 invariants, the failure mode is a **wrong answer rather than an error**, which
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

As of #244 the guard lives in `com.ospreydcs.dp.client.criteria.ClientCriteria`, alongside the
`TextMatch` and `AttributeCriterion` value types, because the invariant is not specific to the
annotation service: the Query API V2 `PvSelector.MetadataQuery` selector resolves through the same
`MongoQueryFilterBuilder.nameMatchFilter()` and needs the identical guard. Every criterion builder
in every client goes through that one copy. Copying the four lines into a second client is the
drift the shared helper exists to prevent.

### Query API V2 Client Wrappers (issue #244)

`QueryClient` wraps all four V2 methods (`queryBuckets`, `queryBucketsStream`, `querySamples`,
`querySamplesStream`). Three invariants outlive the ticket:

- **A mutually exclusive proto oneof is modeled as a sealed interface, not nullable fields.**
  `PvSelectorParams` permits one record per `PvSelector` arm, so populating two arms does not
  compile. The alternative is live in the same class: `buildQueryTableRequest` silently prefers
  `pvNameList` over `pvNamePattern` when both are set, handing the caller a query they did not ask
  for with no diagnostic. For the same reason `QueryBucketsParams` simply omits
  `sampleStatusSelector` — the server rejects that combination, and a field that can only ever
  produce a rejection should not be offered.
- **An empty `ConfigurationSelector` is a rejection, so the builder drops the whole selector.**
  `AnnotationClient`'s idiom — "a null or empty field contributes no criterion" — applied naively
  here builds a *rejected* request rather than an omitted filter. This is the one place where
  criteria emptiness is not benign; `buildQuerySpec` guards it and `QueryClientIT` pins it on the
  built request. Note the deliberate asymmetry with its sibling `pvSelector.metadataQuery`, whose
  empty form is **match-all** (documented in `query.proto` as of dp-grpc#149): two selectors on the
  same `QuerySpec`, opposite empty-criteria semantics.
- **The streaming builders drop `pageToken`, they do not forward it.** The params types are shared
  with the unary methods, where a token is legitimate, and the server rejects a non-empty token on a
  streaming call. `buildQuerySamplesStreamRequest`/`buildQueryBucketsStreamRequest` therefore
  suppress it; a params instance reused from a unary call would otherwise fail every time.

`QuerySamplesStreamResponseObserver` accumulates streamed pages by merging columns **by name, never
by position**, and hard-fails a page whose column set differs from the first page's. The server
seeds a column per resolved PV on every page so the sets match in practice — but an index-based
merge would silently mis-align a whole PV's values against the timestamp axis if that stopped
holding, which is a wrong answer rather than an error.
`QuerySamplesStreamAccumulationTest` drives the observer directly to cover the page shapes a real
server will not produce.

Two behaviors the wrapper javadoc carries because the proto does not: `excludeColumnMetadata` is
**inert on the samples path** (no sample dispatcher reads it; `AbstractQueryBucketsDispatcher.java:30`
is the only call site repo-wide), so `QuerySamplesParams` does not expose it; and the non-scalar PV
rejection is **data-driven, not pre-flight** (#194) — a non-scalar PV with no buckets in the
requested window passes silently, and the same PV set can succeed on one page and reject on the
next.

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

**Constraints:** string values ≤ 256 chars; array dimensions 1–3 (all > 0); ≤ 10M array elements; image ≤ 50MB; struct ≤ 1MB (the four value caps live in the shared `ColumnValueLimits` and bind the saveAnnotation calculations path too); timestamps non-decreasing, nanos 0–999,999,999; sample count must match timestamp count; bucket time span ≤ `Buckets.maxBucketSpanSeconds` (default 86400, validated once by `BucketSpanLimits`) — ingestion-only since #232; the query-side `firstTime` lower bound is derived per PV from `pvStats`, not from this value (see below).

### Per-PV Bucket Span Bound (issue #232)
The query-side bucket overlap filter adds a `firstTime` lower bound,
`firstTime.seconds >= beginSeconds - maxBucketSpanSeconds`, built in the shared
`MongoQueryFilterBuilder.bucketOverlapsRangeFilter()`. The span is **not configuration**: it is the
largest `lastTime.seconds - firstTime.seconds` ever ingested for each PV, recorded by the ingestion
service in the `pvStats` collection (`PvStatsDocument`, `_id` = pvName) and resolved on every query
by `MongoSyncQueryClient.resolveMaxBucketSpanSeconds()` as the maximum over the request's PVs
(pattern queries run the same regex against `pvStats._id`). This replaced the #197 startup scan
that verified the whole archive against the configured limit and disabled the bound process-wide
on violation. Every failure mode is a **silent wrong answer** — a bucket the bound excludes is
missing from the result, not an error — so the invariants below are load-bearing:

- **Stats are written before the buckets, and a failed stats write fails the request.**
  `MongoSyncIngestionClient.insertBatch()` calls `PvStatsMaxSpanUpdater.recordSpan()` ahead of
  `insertMany`; a `DpException` there returns an error `IngestionTaskResult` with no buckets
  inserted. At every instant the stored maximum therefore covers every stored bucket. Do not
  reorder the two writes, and do not make the stats write fire-and-forget.
- **The stored value only grows.** The updater issues `$max` upserts in one unordered `bulkWrite`,
  gated by a per-process high-watermark cache that advances only after the bulk succeeds, so a PV
  costs one write per process lifetime unless a longer span arrives. Skipping is safe across
  processes because `$max` is monotone. Any **out-of-band writer** of `buckets` (a direct import
  that bypasses ingestion) must `$max` the affected PV's `pvStats` document — that one `updateOne`
  is the whole recourse (`doc/schema-migration.md`, note on version 5): no rescan, no restart, and
  deliberately no kill switch for the bound. Lowering a stored value by hand needs an ingestion
  restart, or the cache skips the re-raise. In-repo out-of-band writers must do this in code:
  `QueryBenchmarkBase.BenchmarkDbClient` and the test helpers
  (`MongoTestClient.upsertPvStatsMaxSpan()`, `MongoQueryHandlerTestBase.recordPvStatsForBuckets()`)
  all record the span through `PvStatsMaxSpanUpdater` before inserting. A new direct `insertMany`
  into `buckets` anywhere must do the same — a fixture whose spans are all 0 can hide the omission
  until someone widens a bucket.
- **Never cache on the read side.** A cached span can only be too small once a longer bucket is
  ingested, and a too-small bound silently drops that bucket. The read is one `$in` on `_id`
  against a collection with one document per PV.
- **A missing `pvStats` document means the PV has no buckets** and contributes nothing to the
  bound; a request naming only such PVs gets a bound of `beginSeconds`. Do not treat absence as
  "unbounded" (one mistyped PV name would turn the query into the multi-minute full scan), and do
  not floor the bound at `Buckets.maxBucketSpanSeconds`. The corollary is a deployment constraint:
  a bucket written by a pre-#232 ingestion process after migration v5 has seeded `pvStats` is
  uncovered, so upgrade ingestion first or stop every service for the upgrade. Test code that
  inserts buckets directly has the same gap; see Testing Strategy.
- **A failed `pvStats` read is a query error, but a corrupt stored value is not.** The resolver
  throws `DpException` on a read failure, the client returns a null cursor, and every dispatcher
  reports that as an error. A stored *negative* span is instead logged at `warn` and clamped to 0 —
  no writing path can produce one (ingestion `$max`es a non-negative span; the v5 seed filters
  `$gte 0`), so it means hand-editing, and rejecting it would fail every query naming that PV and,
  since the bound is a maximum over the request's PVs, every multi-PV query including it. Clamping
  narrows that one PV to the no-document bound (D6), which is what D10 already chose at write time.
  Never degrade to the unbounded scan — on the customer archive that is a four-minute query hiding a database problem
  behind slow but "successful" responses.
- **`Buckets.maxBucketSpanSeconds` is ingestion-only.** `BucketSpanLimits` validates it once
  (rejects non-positive, and anything above `MAX_CONFIGURABLE_SPAN_SECONDS` where the nanos
  conversion would overflow) and `IngestionValidationUtility` rejects frames over it. The query side
  does not read it, so changing it changes only what ingestion accepts from then on.
- **The bound must reach the planner as an index bound, not merely a filter.** The seconds/nanos
  `$or` halves of the overlap predicate cannot become index bounds and run as a residual filter on
  the fetched documents, so the scan size is set entirely by this bound on the compound
  `(pvName, firstTime, ...)` index. A predicate moved inside an `$or`, a changed index declaration,
  or a sort the index cannot serve still returns the right buckets — only after scanning each PV's
  whole history — which is why `MongoBucketQueryPlanTest` checks the `explain` plan shape.

Schema migration v5 (`V5SeedPvStatsMaxBucketSpan`) seeds `pvStats` from the existing archive in one
`$group`/`$merge` pipeline (`$max` on match, so re-runs and concurrent upgraded ingestion are safe)
and drops the legacy `bucketSpanVerification` collection. A PV whose every bucket lacks
`dataTimestamps` or has an inverted span is left without a document rather than seeded null or
negative, either of which breaks the query side. Further per-PV ingestion statistics belong on the
same document (#201) and the same write path.

### Bucket Deserialization Must Fail as `DpException`
The query dispatchers (`QueryDataDispatcher`, `QueryDataStreamDispatcher`, `QueryDataBidiStreamDispatcher`, `QueryBuckets*Dispatcher`) catch **only `DpException`** around bucket deserialization. Any other exception escapes the dispatch loop and terminates the response stream, so the client receives **zero buckets instead of an error** — indistinguishable from "no data in range."

`BucketDocument.dataBucketFromDocument()` / `dataBucketFromDocumentV2()` therefore validate required fields up front and wrap any `RuntimeException` as `DpException`. Preserve that contract when adding deserialization logic: a malformed stored document must produce a reportable error, never an unchecked throw. In tests, insert fully-populated `BucketDocument`s (see `MongoTestClient.insertBucketDocument()`) rather than hand-rolled partial BSON.

The #197 startup scan also reported buckets missing `dataColumn`/`dataTimestamps`; that diagnostic went with the scan when #232 deleted it. A malformed bucket still surfaces as a `DpException` at query time under the contract above, and locating offenders offline belongs with #258's utility.

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
  status frame has no `maxBucketSpanSeconds`-style cap and **no #232-style firstTime lower bound may
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

### Bucket Span Bound Tests (issue #232)
- **Direct bucket inserts need a recorded span.** `BucketUtility` and
  `MongoTestClient.insertBucketDocument()` bypass ingestion, so nothing writes `pvStats`, and a
  bucket inserted that way is found only by windows beginning in or before its first second. Record
  the span first through `MongoTestClient.upsertPvStatsMaxSpan()` (the production `$max` upsert) or
  `MongoQueryHandlerTestBase.recordPvStatsForBuckets()`; `insertPvStatsDocument()` is the raw insert
  for a stored value the updater would not write. A query test that loses buckets after a fixture
  change usually has this cause.
- **`MongoBucketQueryPlanTest`** is the repo's only `explain`-based plan-shape test: it pins the
  `[begin − span, ∞)` index bound on the compound bucket index, resolved through the production
  resolver from `pvStats` documents seeded through the production updater. Extend it, not a
  result-level test, for any change to the overlap filter, the bucket index, or the sort.
- **`PvStatsMaxSpanUpdaterTest`** pins the `$max` upsert and watermark semantics through a Mockito
  mock delegating to the real `dp-test` collection (call counting, write-model capture, fault
  injection). A closed-client handle is not a substitute: it throws a driver state exception, not
  the `MongoException` the updater classifies, and cannot produce the unacknowledged case.
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`** asserts every
  ingested PV has a `pvStats` document with a span at least the request's;
  **`ExportDataBucketSpanIT`** covers the annotation export path (seeded span finds the over-long
  bucket, no entry excludes it); **`V5SeedPvStatsMaxBucketSpanTest`** covers the seed pipeline.

## Schema Migration (issue #254)

Schema changes are delivered by a versioned migration runner that executes during
`MongoClientBase.init()`, and the mechanism **fails closed**: a database whose schema version the
binary cannot establish stops the service rather than being served from. Operator documentation is
`doc/schema-migration.md`.

- **Adding a migration**: implement `Migration` in `common/mongo/migration/migrations/`, add it to
  `SchemaMigrationRunner.MIGRATIONS`, and bump `SCHEMA_VERSION`. The version is a plain integer owned
  by the code, deliberately *not* derived from the Maven project version — most releases change no
  schema, so the two move at different rates.
- **Migrations operate on `MongoDatabase`/`Document`, never the POJO document classes.** The codec
  registry is bound to the *current* class shape, while a migration by definition reads documents
  written under a previous one. This also keeps an old migration working after the classes move on.
- **Every migration must be idempotent**, and must say why in its Javadoc — the runner cannot enforce
  it. A crash between applying and recording the version re-runs it, as can the stuck-claim recovery.
- **Index changes are migration steps**, so `createMongoIndex*` stays purely additive. Reconciling
  live indexes against the declared set instead would drop an index an operator added deliberately.
  Migrations therefore run **after** collection init but **before** every `createMongoIndexes*()`
  call; that ordering is load-bearing, see below.

### Absence of a marker is resolved by emptiness, not assumption

No marker plus no documents in any managed collection is a **fresh install** (stamped at the current
version, nothing run); no marker plus any document is a **legacy database** (migrated from 0). Neither
can be assumed: always-version-0 replays an accumulating list against every fresh install, while
always-current silently stamps a real unmigrated deployment as done — the failure the mechanism
exists to prevent.

`SchemaMigrationRunner.MANAGED_COLLECTION_NAMES` must list every `COLLECTION_NAME_*` constant on
`MongoClientBase` (except `serviceMetadata`, which holds the marker itself). That includes
`COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY`, the marker collection of the startup bucket-span
check #232 removed: no client initializes it any more and migration v5 drops it, but the constant
stays so a pre-v5 database holding only that marker still reads as legacy. A collection omitted
makes a populated database look fresh and skips every migration — verified against MongoDB 8.0:
with `bucketSpanVerification` missing from the list, a database holding only that marker reported
zero migrations applied and was stamped as current.

The probe asks "has any build ever used this database?", which is broader than "does it hold service
data" — a prior deployment's marker still counts when the data collections have been emptied.
Including a collection can only push a database toward "legacy", never "fresh", and that is the safe
direction, since every migration is idempotent.

`SchemaMigrationRunnerTest.testManagedCollectionListCoversEveryDeclaredCollection` pins this by
reflection over `MongoClientBase`, which is why **every collection constant — the legacy one
included — must be declared there**: a constant declared on any other class falls outside a test
whose whole promise is that a collection cannot be forgotten (the legacy constant originally lived
on the deleted verifier, and the test had to reflect over two classes to see it).

### Every marker write checks `matchedCount`

`recordApplied` and `releaseClaim` filter on `_id: "schemaVersion"`, and a filtered update that
matches nothing writes nothing while reporting no error. Both therefore test
`getMatchedCount() == 0` and throw — the same convention the annotation client's `replaceOne` calls
follow, and for a sharper reason here: `recordApplied` runs *after* the migration has already changed
the data, so an unchecked write leaves migrated data with no record of it, and the run continues to
report success. Any new marker write must check the same way.

### The claim wait is bounded by one deadline, not one per attempt

`SchemaMigrationRunner.migrateOrWait()` is a single loop carrying one absolute deadline across every
claim attempt, including takeovers after another process releases without completing. It was
originally a pair of mutually recursive methods, each allocating a fresh timeout — so the bound the
class documents was not the bound in force, and repeated takeovers grew the stack. Keep the deadline
computed once at entry; do not reintroduce a per-attempt timeout.

### The async client skips the version check, and that is a deployment constraint

`MongoAsyncClient.runSchemaMigrations()` returns true without doing anything: the reactive driver has
no synchronous `MongoDatabase`, and the async client is not a separate database — it connects to the
one the sync clients migrate. It does **not** fail startup, because refusing there would stop a
process that has no migration to perform and no way to perform one.

What it cannot do is verify that the database matches the schema its binary expects. That is only
acceptable while the client stays off every production path (today its sole construction is in
`MongoAsyncIngestionHandlerTest`). **Putting it into service requires implementing the version check
first** — read the marker via a short-lived sync client — or that process serves requests against a
schema it never verified.

### A text index is identified by its `weights`, never its key or name

MongoDB stores a text index as `key: {_fts: "text", _ftsx: 1, <ascending fields>}` with the indexed
text fields moved into a separate `weights` document. Two text indexes over different fields therefore
have **identical key documents**. Matching a text index by key finds both or neither; matching by the
default derived name misses one created explicitly under another name. Match on `weights`.

**Mongo permits only one text index per collection.** Replacing one is therefore a drop-then-create,
and the drop must precede any `createIndex` for the replacement or it fails with
`IndexOptionsConflict` — a startup failure, not a stale-index performance cost. This is why the
migration runner is ordered ahead of index creation in `init()`.

### A stored field rename must move its index declaration in the same change

Renaming a BSON field without moving the `BsonConstants` key and the index declaration that uses it
has index creation rebuild, moments after the migration drops it, an index over a field no document
has any more — while the marker records the migration as applied. Caught end-to-end during #254; the
constant and index line ship with the migration, not with the document-class rename.

### `Instant` round-trips through BSON as `java.util.Date`

BSON has one date type, and the driver's default decoding target is `Date`. So
`Document.get(key, Instant.class)` on a value written as an `Instant` throws `ClassCastException`
rather than returning null. Convert explicitly (`SchemaVersionMarker.readInstant()`) when reading
timestamps out of a raw `Document`.

### Startup failure stops the server

`GrpcServerBase.initService_()` returns `boolean` and `start()` throws `DpRuntimeException` before
binding the port when it is false. Before #254 it returned `void` and all four servers logged and
returned — which exited `initService_()`, not `start()`, so a service whose Mongo connection or
handler init failed went on to serve requests against an uninitialized handler.

`start()` must **throw**, not return: `main()` calls `start()` then `blockUntilShutdown()`, and a
silent return leaves `server` null, so `blockUntilShutdown()` falls through to `finiService_()` and
the process exits 0 — a supervisor reads that as a clean shutdown and never alerts. Any new server
implementation must return its `serviceImpl.init(...)` result rather than swallowing it.

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
