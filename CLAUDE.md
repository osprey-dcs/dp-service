# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run specific service:
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.16.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.16.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.16.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

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
documented in `doc/runbooks/schema-migration.md`.

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

- **A mutually exclusive proto oneof never resolves a multi-arm value by preference order.** Both
  V2 oneofs enforce that, at different points. `PvSelectorParams` is a sealed interface permitting
  one record per `PvSelector` arm, so populating two does not compile. `ConfigurationCriterion`
  stays a five-nullable-arm record — it is a *repeated* field whose criteria are ANDed, so a sealed
  hierarchy would cost five records plus a wrapper and force every caller to build a heterogeneous
  list — and `buildConfigurationCriterion` therefore evaluates every arm and returns null unless
  exactly one is populated, rather than short-circuiting on the first. The anti-pattern both avoid
  is live in the same class: `buildQueryTableRequest` silently prefers `pvNameList` over
  `pvNamePattern` when both are set, handing the caller a query they did not ask for with no
  diagnostic. For the same reason `QueryBucketsParams` simply omits `sampleStatusSelector` — the
  server rejects that combination, and a field that can only ever produce a rejection should not be
  offered.
- **`configurationSelector` distinguishes "not requested" from "requested but unusable", and the
  two resolve oppositely.** Null or empty `configurationCriteria` means no restriction was asked
  for, so `buildQuerySpec` omits the selector. A *non-empty* list from which no criterion survives
  (every arm blank, or a multi-arm criterion) instead emits the **empty** selector, which the server
  rejects. **This is the #243 rule inverted and the distinction is load-bearing:** everywhere else
  dropping a blank value narrows toward correctness, because a blank prefix would have matched
  everything. Here omitting the selector *widens* the query from "only while configuration X was
  active" to the whole time range, so silently dropping a criterion the caller filled in returns
  strictly more data than they asked for with no diagnostic. Rejection is the loud outcome, and it
  matches `PvNameListSelector`, whose all-blank name list is likewise forwarded empty for the server
  to reject. Collapsing the two cases back into one unconditional drop reintroduces the silent
  widening. Note also the deliberate asymmetry with its sibling `pvSelector.metadataQuery`, whose
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

**Serialized columns are the one thing that stream cannot accumulate, and the result says so.**
Under `useSerializedColumns` the server puts every column in `serializedDataColumns` and leaves
`dataColumns` empty (`AbstractQuerySamplesDispatcher.buildColumnTable()`), so the by-name merge is
inert and merging would require deserializing each payload. The accumulated table then holds
(pages × columns) entries — each a per-page *fragment*, the same column name repeated once per page
— against a fully concatenated timestamp axis: structurally valid, but its columns do not line up
with it. Because that looks assembled, the condition is **reported** rather than left to javadoc:
`QuerySamplesStreamResponseObserver.isSerializedColumnsFragmented()` and the
`QuerySamplesApiResult.serializedColumnsFragmented` field are true whenever more than one page
carried serialized columns, and false for the unary method, the non-serialized representation, and
a single-page stream. A new streaming accumulator for a representation that cannot be merged owes
the caller the same flag.

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
by `MongoSyncQueryClient.resolveSpanClasses()`, which partitions the request's PVs into
power-of-two span classes, each bounded by its own actual maximum (#274, below; the V1 `queryTable`
pattern arm has no PV list and keeps `resolveMaxBucketSpanSeconds()` over the `pvStats._id`
regex matches). This replaced the #197 startup scan
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
  is the whole recourse (`doc/runbooks/schema-migration.md`, note on version 5): no rescan, no restart, and
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
  `$gte 0`), so it means hand-editing, and rejecting it would fail every query naming that PV and
  every multi-PV query including it. Clamping narrows that one PV to the no-document bound (D6),
  which is what D10 already chose at write time.
- **The overlap residual runs after the fetch, so the span bound is a document-fetch bound, and
  the request is partitioned by span class (#274).** Measured with `explain` on the exact
  production filter, sort, and hint: the `lastTime >= begin` half and the nanos half of
  `firstTime < end` are evaluated on the `FETCH` stage, and `totalDocsExamined` equals
  `totalKeysExamined`. Every index key inside `[begin − span, end]` is a fetched document. A single
  bound taken as the maximum over a request's PVs therefore made one long-span PV (the customer
  archive has 42-day spans) fetch that span's worth of history for *every* PV in the request.
  `SpanClass` assigns each PV to a class by its own span (class 0 for spans ≤ 1 s, which includes
  PVs with no `pvStats` document; class *k* for `2^(k−1) < span ≤ 2^k`), each class's find carries
  the class's actual maximum, and `MergedBucketCursor` merges the class cursors in
  `(pvName, firstTime)` order. A request whose PVs share one class returns that class's cursor
  directly with a filter identical to the pre-partition query, so the plan-shape test's existing
  assertions keep pinning the common case. All class cursors are opened eagerly inside the
  retrieval method's `try`/`catch`, so a class that fails to open (missing hinted index, outage)
  still reports the null cursor; `MongoSyncQueryClientMissingIndexTest` pins a two-class request.
  The merged cursor is wrapped in `TimedMongoCursor` with plain inner cursors, so the `db` stage
  covers the merge. If a future server version evaluates the residual on the index scan, the
  `totalDocsExamined == totalKeysExamined` assertion in the plan-shape test (#275) is the signal
  that the partition's cost model has changed.
  Never degrade to the unbounded scan — on the customer archive that is a four-minute query hiding a database problem
  behind slow but "successful" responses.
- **`Buckets.maxBucketSpanSeconds` is ingestion-only.** `BucketSpanLimits` validates it once
  (rejects non-positive, and anything above `MAX_CONFIGURABLE_SPAN_SECONDS` where the nanos
  conversion would overflow) and `IngestionValidationUtility` rejects frames over it. The query side
  does not read it, so changing it changes only what ingestion accepts from then on.
- **The bounds must reach the planner as index bounds, not merely filters.** The seconds/nanos
  `$or` halves of the overlap predicate cannot become index bounds and run as a residual filter on
  the fetched documents, so the scan size is set entirely by the two plain range predicates on
  `firstTime.seconds` from `MongoQueryFilterBuilder.bucketFirstTimeSecondsIndexBounds()`: the span
  lower bound and, since #271, the upper bound `<= endSeconds` implied by `firstTime < end`.
  Without the upper bound the planner's single-scan plan ran from the lower bound to the end of each
  PV's history and filtered the rest out. A predicate moved inside an `$or`, a changed index
  declaration, or a sort the index cannot serve still returns the right buckets — only after
  scanning each PV's whole history — which is why `MongoBucketQueryPlanTest` checks the `explain`
  plan shape.
- **A bound inside an `$or` branch is not a bound.** The V2 fragment `$or` puts each fragment's
  bounds inside its own branch, and the planner's single-scan plan — which it chose on the plan
  test's fixture, hint or no hint — then had `firstTime.seconds: [MinKey, MaxKey]`: every named PV's
  whole history. `MongoSyncQueryClient.fragmentsOverlapFilter()` therefore hoists the pair of bounds
  above the `$or`, over the fragments' earliest begin and latest end (#271). Any new predicate that
  wraps the overlap filter in an `$or` owes the planner the same hoisted copy.
- **Every bucket query hints the compound index, and the hint is the index declaration.**
  `MongoSyncQueryClient.bucketFind()` is the single `find()` for bucket retrieval (V1 and all three
  V2 paths): filter, `(pvName, firstTime)` sort, and `hint(MongoClientBase.BUCKET_QUERY_INDEX_KEYS)`
  — the same `Bson` object `createMongoIndexesBuckets()` passes to `createIndex`, so the two cannot
  drift. The hint exists because startup never drops an index: a long-lived archive still carries
  the `pvName`-led shapes retired in beta-1.6.0 and rel-1.15.0 plus whatever operators added, each a
  planner candidate whose scan the multi-plan trial executes before choosing, and on recent-window
  queries the planner was measured picking a `lastTime`-led index with a blocking `SORT`. The trade
  is deliberate: a missing index now fails the query with a driver error instead of degrading to a
  collection scan. Do not add a second `find()` on `mongoCollectionBuckets` in the query client.
- **That driver error only reaches the caller because every retrieval method catches it.** The find
  is issued by `cursor()`, so a `MongoException` — a missing hinted index on every call, or an
  outage on any call — is thrown there, not at first iteration. Uncaught it escapes the job into
  `QueueHandlerBase`'s worker, which logs it and takes the next job: `dispatcher.handleResult()`
  never runs and the caller's stream stays open until it times out, with no error ever sent. So
  every bucket retrieval method wraps `cursor()` and returns the null cursor that each dispatcher
  turns into an error response. The V1 `executeBucketDocumentQuery` lacked that catch when the hint
  landed, which made #271's "fails loudly" trade a hang on `queryData`/`queryTable`/data-block
  export; `MongoSyncQueryClientMissingIndexTest` pins all four paths. A new bucket query method
  owes the same catch — a throw here is strictly worse than a misclassified failure, because the
  caller gets nothing at all.

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

### querySamples Retrieval Is Time-Sliced (issue #274)

Every bucket query is sorted `(pvName, firstTime)`, so its cursor is **PV-major**: all of one PV's
buckets in the window, then the next PV's. Before #274 the unary `querySamples` page was assembled
by draining that cursor until the outgoing byte budget tripped, on the assumption that only the
last assembled timestamp could be incomplete. Under PV-major order that is false: a trip partway
through the first PV left every later PV with nothing, the page went out with those columns
all-unset (indistinguishable from "no sample here"), and the timestamp token resumed at the same
position, so the later PVs were **never returned** — a silent wrong answer reproduced with two PVs
and a 40-byte budget (30 values for the first PV, 0 for the second, no error). With the production
budget the trip point is ~455k values from the first PV in name order: a window over ~7.5 minutes at
1 kHz, ~12 hours at 10 Hz, ~5 days at 1 Hz.

Both samples paths therefore retrieve through `AbstractQuerySamplesDispatcher.SliceDrain`:

- **A slice is one query over every resolved PV, and is accepted whole or discarded whole.** A
  timestamp inside an accepted slice is complete across every PV by construction. Never emit rows
  from a partially drained retrieval on these paths.
- **The slice end is a retrieval bound applied per fragment, not a collapsed retention window.**
  `TimeInterval.clampToWindow()` intersects each resolved fragment with the slice and is the single
  source for both the client's per-fragment `$or` (`executeQuerySamplesV2`) and the dispatcher's
  retention intervals, so the #207 gap trim still holds inside a slice. `computeWindowBegin()`
  stays begin-only for that reason. A slice that intersects no fragment costs no database call; the
  position jumps to the next fragment begin.
- **Slice length adapts proportionally, because each retrieval pays the span scan.** After an
  accepted slice of *r* rows toward a target of `pageSize`, the length is multiplied by
  `clamp(pageSize / max(r, 1), 1, 16)`, reaching a 10k-row page in two or three retrievals at any
  steady rate. Doubling would need eight retrievals at 1 Hz, each fetching `[begin − span, end]`.
  The start is `QueryHandler.queryV2SamplesInitialSliceSeconds` (60), in both `application.yml`s.
- **A budget trip discards the slice and ends the page at the slice begin** (`TimestampMap.removeFrom`).
  Only when the page is still empty is the slice halved and retried; a trip at one nanosecond is
  the indivisible-oversized error. Every non-error page still makes progress, and the token is the
  first undrained timestamp — including on an **exactly filled** page, where nothing is truncated
  and the token is the drain's position (a missing token there silently ended the traversal early;
  `testGapWiderThanTheSliceIsJumpedWithoutRetrieval` pins it).
- **The stream path is bounded by the budget plus one slice.** It emits after each slice that
  reaches the row limit, on every budget trip (then retries the slice), and at exhaustion. It no
  longer materializes the window; do not reintroduce a `null` size limit on a samples path.
- **The status join is resolved per slice** (`resolveSampleStatusTimestamps` takes the slice end),
  and each slice's retrieval and cursor are timed into the request's `db` stage.
- **The `pvStats` span-class partition is resolved once per page, not once per slice.** It depends
  only on the request's PV list and the stored spans, so it is identical for every slice;
  `SliceDrain` carries a `MongoQueryClientInterface.SpanClassHolder` that `executeQuerySamplesV2`'s
  six-argument overload fills on the first slice and reuses. The five-arg form delegates with a null
  holder, and the overload is a `default` method, so no other client or test double knows about it.
  **This is a per-request hoist and must stay one:** spans must never be held across requests (#232,
  plan D7) — a stored span only grows, so a stale one is too small, and a too-small `firstTime`
  bound silently omits buckets instead of failing. A holder created per page cannot go stale within
  that page; a field, a static, or anything keyed by PV name can.
  `testSpanClassesResolvedOncePerPageNotPerSlice` pins the count.

The regression guards are the two-PV byte-budget tests in `MongoSyncQuerySamplesV2Test` (unary and
stream), which count set values per column across every page; the single-PV seam test cannot see
this class of defect. `TimestampDataMap.getColumnIndex()` is hash-backed and
`TabularDataUtility.addColumnsToTable()` resolves each column's index once per call, not once per
retained sample (it was an `indexOf` per value, O(columns) string compares per sample on wide
queries).

### Outbound Flow Control on Streaming Responders (issue #274)

gRPC never blocks `onNext`; every message a slow client has not consumed is buffered in the
transport. `queryDataStream`, `queryBucketsStream`, and `querySamplesStream` therefore call
`OutboundReadinessGate.awaitReady()` (`common/grpc`) before every streamed send. The gate wraps
`ServerCallStreamObserver.isReady()`/`setOnReadyHandler()`; a plain `StreamObserver` (the unit
tests) yields a no-op gate. It is built in the dispatcher **constructor** because the handler
constructs stream dispatchers synchronously inside the service method, on the gRPC thread, which is
where gRPC requires the ready handler to be registered — a dispatcher constructed later (on a worker)
would register too late. `awaitReady()` returns false on client cancellation or after
`QueryHandler.streamReadyTimeoutSeconds` (300), and the dispatcher then closes its cursor and
returns without completing: a cancelled call has no reader, and a stalled one is better left
incomplete than given another buffered message. The trade is deliberate — a slow reader now
occupies one worker instead of the heap. `queryDataBidiStream` is client-paced and ungated.
`FakeServerCallStreamObserver` (test) drives the gate without a server.

**The gate registers a cancel handler as well as a ready handler, and both signal the same
condition.** gRPC dispatches cancellation and readiness through separate callbacks — a cancel never
fires the ready handler, and `isReady()` stays false once the call is closed — so a gate that
registered only the ready handler could not observe a cancellation that arrived while a worker was
already parked in `awaitNanos`. It slept out the entire timeout (measured at the full bound) holding
a worker for a call that already had no reader, then logged it as a drain timeout, which is the
wrong diagnosis. The bounded-worker property this design trades for rests on a cancel freeing its
worker promptly, so a future `ServerCallStreamObserver` wrapper owes the same pair of handlers.
`OutboundReadinessGateTest.testCancelWhileBlockedWakesTheWaiter` pins it; the pre-existing
cancellation tests only set the flag *before* the wait, so they exercise the pre-check, not this.

**A gate refusal must not be followed by `onCompleted()`, on any path including the empty ones.**
The empty-result branches originally discarded `emitChunk()`'s return and completed regardless, so a
stream abandoned because the client was gone was still reported to gRPC as a finished RPC. Every
send site owes the same check; `testEmptyResultDoesNotCompleteWhenTheGateRefuses` pins the two empty
branches, which are the easy ones to miss because they look like they have nothing to send.

**A mid-slice cursor failure on the samples path must be wrapped as `DpException`.** The find is
issued when the cursor is opened, but the driver fetches later batches *during iteration* — which
happens inside `TabularDataUtility.addBucketsToTable`, not in the retrieval method. So a `getMore`
against a failed-over server, or a connection dropped mid-cursor, surfaces as an unchecked
`MongoException` there rather than as the null cursor the open path returns. The drain loops catch
only `DpException`, so uncaught it escapes into `QueueHandlerBase`'s worker and the caller's stream
hangs with no response — the same class of defect as the missing `cursor()` catch, and the reason
`SliceDrain.retrieveSlice` wraps its assembly block. `testMidSliceCursorFailureIsReportedAsAnError`
pins it.

**An abandoned response is `dp.outcome=abandoned`, never `success`.** The refusal paths return
without sending, and `QueryTelemetry.outcome` defaults to `success`, so before `markAbandoned()`
every stream cut short by a cancellation or a 300 s stall was counted as a successful request with a
full set of stage histograms — leaving the one condition outbound flow control exists to manage
invisible in metrics, the same defect `markFailedWithException()` exists to prevent for escaping
exceptions. Like that method it does not overwrite an outcome already set. A new gate call site owes
the same mark.

**Classify the outcome after the send succeeds, not before it is attempted.** `markAbandoned()`
preserves an outcome already set, so a path that marked its outcome *first* and emitted second
silently disabled it: the buckets stream dispatcher called `markEmpty()` ahead of `emitChunk()`, and
a refused empty send therefore recorded `empty` — a cancelled client that received nothing, counted
as a successful empty query. Both empty branches now emit first and mark only on success, which is
the order `queryDataStream` already had. The distinction is invisible on the wire (a refused send
and a genuine empty result both send nothing further), so
`testEmptyResultDoesNotCompleteWhenTheGateRefuses` asserts `QueryTelemetry.getOutcome()` as well as
the observer — an observer-only assertion passes against the wrong metric, which is how this
survived the fix to the `onCompleted()` half of the same two branches.

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

### A query benchmark must fail when it measures nothing (issue #275)

An empty query result is a normal, non-exceptional response, so a benchmark client that reports
success regardless turns "the fixture does not hold what I asked for" into a plausible-looking rate
computed over zero work. Every query client therefore returns its success result through
`QueryBenchmarkBase.resultRequiringData()`, which fails the task when the value count is zero, and
`queryScenario`'s executor-timeout and exception branches set `success = false` (leaving it true
reported a hung scenario as a pass at 0.0 values/sec). A new client owes the same.

**What the loader writes and what the clients wait for are one arithmetic contract.**
`LoadParams.bucketsPerPv()` and `QueryTaskParams.expectedBucketCount()` must agree, and they were a
floor against a ceiling: any history not dividing evenly by `secondsPerBucket` loaded one bucket
fewer than every V1 client waited for, so each task hung to its latch timeout and reported 0.0 —
the failure this ticket exists to remove, one level up. The loader now writes the trailing partial
period as a short bucket (`secondsInBucket()`) rather than the expectation dropping to a floor: a
truncated fixture leaves the last seconds of every query window holding no data, which measures a
narrower window than the one requested. `QueryBenchmarkFixtureShapeTest` pins the agreement across
bucket lengths and histories; the default one-second bucket divides evenly, so nothing else sees it.

The V1 clients terminate on a **bucket count**, not on `onCompleted()` — the bidi client also paces
its cursor requests from it — so that count must be derived from the loaded fixture, via
`QueryTaskParams.expectedBucketCount()`. It previously assumed one-second buckets and treated every
named PV as a regular fixture PV, which made all three V1 clients hang for their latch timeout and
report 0.0 under `--seconds-per-bucket != 1` or `--include-long-span`, including the documented
example command. `LoadMarker` records `secondsPerBucket` and `numPvs` for this reason, and
`--skip-load` rejects a fixture smaller than the scenarios about to run rather than querying PVs
that hold no data.

Direct bucket writers here are subject to the #232 rule like any other: `BenchmarkDbClient.insertBucketDocuments()`
records the batch's span through `PvStatsMaxSpanUpdater` **before** `insertMany`, and it is the only
`insertMany` on `buckets` in the benchmark code — the long-span fixture goes through it too.

## Testing Strategy
- **Framework**: JUnit 4 (`@Test`, `@Before`, `@After`)
- **Integration Tests**: `src/test/integration/java/com/ospreydcs/dp/service/integration/` — a
  separate source root added by `build-helper-maven-plugin` (`pom.xml`), run by Failsafe, not
  Surefire. Selecting them with `-Dit.test` needs slash-style package paths
  (`com/ospreydcs/.../**`) or `**/ClassName.java`; dotted wildcards match nothing.
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase`
- **Test Database**: "dp-test" (cleaned between tests via `MongoTestClient.init()`)
- **Merge gate**: CI runs `mvn verify`, so every integration test runs on every PR, and gates the
  merge once `build-and-test` is a required check (#250 Task 7; see Continuous Integration). A
  flaky IT then blocks every PR — fix it, in the PR that surfaced it when the fix is small; ticket
  it only when the fix is large or out of scope, and never exclude it from the gate silently.
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
  `[begin − span, end]` index interval on the compound bucket index, resolved through the production
  resolver from `pvStats` documents seeded through the production updater, for the V1 named and
  pattern paths and the V2 fragment-`$or`, keyset-page, and samples queries — the V2 and mixed-span
  cases explaining **one find per span class**, the shape production issues since #274. Since #275
  it also carries a deep-history PV (20,000 one-second buckets ending where the other PVs' history
  ends) and asserts that the same window costs the same keys and documents 1,000 s and 19,000 s
  into that history: scan cost depends on window plus span, not on history depth, which is the
  customer-archive risk a 300-bucket fixture cannot distinguish from "bounded by the history". It
  asserts `totalDocsExamined == totalKeysExamined` on the wide-span case, pinning that the overlap
  residual runs after the fetch (the cost model behind the span-class partition); a server that
  starts evaluating it on the index would fail that assertion, which is a change to understand,
  not to silence. The mixed-span counterfactual explains the pre-#274 single-bound shape so the
  widening the partition removes stays visible in numbers. It runs against an
  adversarial index set (#271: the retired `pvName_1` and `(pvName, firstTime.seconds,
  firstTime.nanos)` plus a `(pvName, lastTime, firstTime)`), asserts every candidate plan is on the
  shipped index exactly and that the winner has no `SORT` stage, and keeps a counterfactual
  (unhinted → rejected plans on other indexes) so the fixture cannot silently stop being
  adversarial. Extend it, not a result-level test, for any change to the overlap filter, the bucket
  index, the hint, or the sort. The explain walker reads the unsharded shape only.
- **`MongoSyncQueryClientMissingIndexTest`** pins the failure *classification* the plan test cannot
  see: with the hinted index dropped, each of the four retrieval methods must return a null cursor
  rather than throw (see the catch invariant above). It asserts the healthy cursor first so a query
  broken for an unrelated reason cannot pass as a correctly reported failure, and restores the index
  in `tearDown` for the rest of the shared `dp-test` run.
- **`PvStatsMaxSpanUpdaterTest`** pins the `$max` upsert and watermark semantics through a Mockito
  mock delegating to the real `dp-test` collection (call counting, write-model capture, fault
  injection). A closed-client handle is not a substitute: it throws a driver state exception, not
  the `MongoException` the updater classifies, and cannot produce the unacknowledged case.
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`** asserts every
  ingested PV has a `pvStats` document with a span at least the request's;
  **`ExportDataBucketSpanIT`** covers the annotation export path (seeded span finds the over-long
  bucket, no entry excludes it); **`V5SeedPvStatsMaxBucketSpanTest`** covers the seed pipeline.

## Metrics and Telemetry (issue #212)

Every service exports OpenTelemetry metrics over a Prometheus endpoint (9464–9467, one per service),
on by default. `doc/metrics.md` is the operator reference — what is measured, the query stage
breakdown, the slow-query log, and the PromQL. The invariants that outlive the ticket:

### Initialization order is load-bearing, and never `GlobalOpenTelemetry`

`DpTelemetry.init()` must run **before** `initService_()`. `DpMetrics` creates each instrument lazily
against whatever meter `DpTelemetry` holds at first use, and the Mongo client and the request
handlers build their instruments during their own init — so an instrument created before `init()` is
bound to the no-op meter and silently records nothing for the life of the process. `GrpcServerBase.start()`
has the order right; a new server implementation must keep it.

`DpTelemetry` never touches `GlobalOpenTelemetry`. That global is settable once per JVM and warns on
every later attempt, while the integration tests build many servers in one JVM. Every lookup goes
through `DpTelemetry.meter()`, which is also what lets a test install its own SDK with an in-memory
reader (`initForTest`/`resetForTest`) and tear it down again.

**An unbindable metrics port fails startup** (the #254 rule: a service that cannot complete
initialization must not serve). `start()` therefore also shuts telemetry down if `initService_()`
fails — the shutdown hook that would release the port is registered only after the failure throw,
so without that an in-process retry would hit a port its own previous attempt still held and report
a telemetry bind error in place of the real failure. The `serverBuilder.build().start()` call is
wrapped for the identical reason: a gRPC port conflict throws with the metrics port already bound.

**`shutdown()` discards the cached instruments, and that is a production requirement, not tidiness.**
Every `DpMetrics` instrument is bound to the meter of the provider being closed. Left cached, the
retry path above builds a fresh SDK that no instrument points at, and the process records nothing
for the rest of its life while reporting a healthy startup. `DpMetrics.discardInstruments()` is
named for production rather than tests for this reason; `initForTest` calls it too, so a test class
is self-correcting rather than dependent on a prior `tearDown`. `DpTelemetryTest`'s
`testShutdownDiscardsCachedInstruments` pins it, and asserts on the recorded *value* after shutdown
rather than installing another SDK first — routing through `initForTest` would mask the very
behavior under test.

**Telemetry shuts down after the handler drains, not after the gRPC server terminates.**
`stopServer()` calls `finiService_()` before `DpTelemetry.shutdown()`. The server terminating is not
when in-flight work finishes: `QueueHandlerBase.fini()` runs `executorService.awaitTermination()`,
and the jobs still draining there record `dp.handler.*`, `dp.query.*` and `dp.ingest.*` as they
complete. Shutting the SDK down first sent every one of those into a closed provider. `finiService_()`
is idempotent (`handler = null`, plus `QueueHandlerBase`'s own `shutdownRequested` guard), so the
JVM shutdown hook and `blockUntilShutdown()` cannot double-drain.

### Instrumentation must never disturb the request it measures

Recordings happen in a `finally` on the response path, after the client's response has gone out. An
exception escaping there would be thrown from a finally block, replacing whatever the try block was
doing — so a metrics failure would present as a service failure. The composite recording sites are
therefore guarded by a `catch` that logs and continues: `IngestionTelemetry.record()`,
`QueryTelemetry.logSlowQuery()`, and both `DpMongoCommandListener` callbacks (which also must not
throw into the driver).

The bare single-instrument calls in `QueueHandlerBase.executeJob` are deliberately unguarded — an
SDK `record()`/`add()` does not throw on the recording path, and wrapping each in its own try would
obscure the `finally` that keeps `workers.active` balanced. Add the guard when a site does something
that *can* fail: string formatting, a log call, or several recordings that must not be left
half-applied.

For the same reason `QueryTelemetry.complete()` is **idempotent**: every job calls it from a
`finally`, but the streaming dispatchers also complete early on error paths and the bidi dispatcher
outlives its job. A second recording would double-count the request in every histogram and counter —
an inflated request rate that still looks plausible, so no test would catch it.

**An unchecked throw out of a job must be classified before `complete()` runs.** `outcome` defaults
to `success`, so the escape documented throughout this file — the worker swallows the throwable, the
dispatcher never answers, the caller's stream hangs until it times out — was recorded as a
*successful* request with a full set of stage histograms, leaving the error rate flat for precisely
the requests that failed hardest. All three query jobs therefore `catch (RuntimeException)`, call
`telemetry.markFailedWithException()`, and rethrow. That method does not overwrite an outcome
already set, since a dispatcher that rejected and then threw has classified the request more
precisely. A dropped job gets the same treatment through `HandlerJob.discarded()`, which
`enqueueJob` calls when an interrupt discards a job that will never run.

**`QueryTelemetry`'s mutators are `synchronized`, and that is load-bearing.** On
`queryDataBidiStream` the worker thread calls `complete()` while gRPC threads still call
`recordResponse()` through `QueryResultCursor.next()`. An earlier version left the counters
unsynchronized on the reasoning that the dispatcher held its `cursorLock` — but `complete()` does
not acquire `cursorLock`, so the two monitors established no happens-before edge, and `long` fields
are not atomic under the JMM. Sharing this object's monitor across the mutators and `complete()` is
what makes the published values correct.

**`dp.query.response.bytes` measures the message actually sent.** The `send*` helpers on
`QueryServiceImpl` return the response they put on the wire so a dispatcher can size that rather
than the nested payload it passed in — the outer message adds a `responseTime` and its framing. Six
of the eight dispatchers were measuring the nested result while two measured the outer, which made
the metric incomparable across query methods. Using the return value also drops a redundant
`build()` of the repeated bucket list on the streaming hot path. The counters deliberately exclude
exceptional and pre-retrieval empty responses (see `recordResponse`'s javadoc): those requests are
identified by `dp.outcome` instead, so the ratio to `dp.query.requests` is bytes per *request*, not
per message.

### The attribute vocabulary is closed (D8)

The complete set of attributes dp instrumentation may attach is `dp.service`, `dp.job`, `dp.stage`,
`dp.outcome`, `rpc.method`, `db.operation.name`, `db.collection.name`, `db.namespace`, `error.type`
— all declared on `DpMetrics` and each bounded by something small and fixed. `dp.outcome`'s values
are `success`, `reject`, `error`, `empty`, and `abandoned` (a streaming response cut short by a
client cancellation or a readiness timeout, #274).

**A PV name, provider id, client request id, page token, or user identity must never become an
attribute.** A facility with 10^5 PVs would turn one histogram into 10^5 time series; that is how a
metrics backend is taken down by the service it monitors, while the service exports happily.
Per-request detail of that kind goes in the slow-query log line, which carries the PV names precisely
because the metrics do not. Integration tests assert the attribute key set of every dp metric and
that no PV name appears as any attribute value; keep them passing rather than widening them.

gRPC's own metrics are **not** governed by this vocabulary — they come from grpc-java's
instrumentation and are labeled `grpc_method` (fully qualified, e.g.
`dp.service.query.DpQueryService/queryData`) and `grpc_status`. Cardinality is bounded by the method
set, so the intent holds.

### Every duration is seconds, on one explicit bucket ladder (D9)

`DpMetrics.DURATION_BUCKET_BOUNDARIES_SECONDS` — 1 ms to 120 s — with unit `s`, per OTel semantic
convention. The SDK's default boundaries were chosen for milliseconds: with unit `s` every
observation a healthy service produces lands in the first bucket and every percentile above p50
reads as the bucket edge — plausible-looking, meaningless numbers. Every instrument is declared on
`DpMetrics` and duration histograms are built through its private `durationHistogram()` helper, so a
new one cannot miss the ladder; add instruments there rather than building one at a call site, and
convert with `DpMetrics.nanosToSeconds()`.

### Things that are measured where they are for a reason

- **Every handler job is timed by `QueueHandlerBase`**, not by the job. `enqueueJob` is the single
  enqueue path (`requestQueue.put` appears nowhere else — worth grepping for), and the worker records
  `queue.wait`, `job.duration`, and the `workers.active` delta around `execute()`. A new job type is
  covered with no work; a job that enqueues itself some other way is not covered at all.
- **Every cursor-returning query-client method wraps its cursor in `TimedMongoCursor`.** The driver
  issues the initial `find` on the first `hasNext()` and `getMore`s as iteration proceeds, and
  decoding happens in `next()` — so timing the `executeQuery*()` call alone attributes nearly all
  database time to `process`. Measured on a 50-bucket query, `db` was half of `total`. The wrapper
  cannot move inside `bucketFind()`, which must keep returning the `FindIterable` that
  `MongoBucketQueryPlanTest` explains.
- **There is no queue-depth gauge**, and its absence is not an oversight: the request queue has
  capacity 1, so depth reads 0 or 1 forever. Saturation is `workers.active` against `workers.max`,
  and the wait it causes is the `queue.wait` distribution.
- **Ingestion RPC duration is not ingestion latency.** Ingestion acks on enqueue and persists later
  on a worker, so `grpc.server.call.duration` measures validation and enqueue only. `dp.ingest.duration`
  measures arrival to end of handling, and is recorded in a `finally` with the outcome defaulting to
  `error`, so a request that fails hard enough to escape its own job is counted as the failure it is
  rather than vanishing from every counter.
- **The shared server-builder helper is what the ITs exercise.** `GrpcIntegrationServiceWrapperBase`
  builds an `InProcessServerBuilder` directly, so anything added only in `GrpcServerBase.start()` is
  untested. Both call `DpTelemetry.configureServerBuilder()`. (The in-process server still produces
  no `grpc.server.*` series, so an IT must assert on the dp instruments, not on those.)
- **`ServicesResourceTransformer` must stay in the shade configuration.** The OTel autoconfigure
  module discovers exporters through `ServiceLoader`, and two exporter jars each ship a
  `META-INF/services/...ConfigurableMetricExporterProvider`. Without the transformer the last one
  copied wins silently and the shaded jar finds `prometheus` or `otlp` but not both, depending on jar
  order. The commented-out per-service shade executions in `pom.xml` predate this and carry only a
  `ManifestResourceTransformer`; re-enabling one means adding the transformer to it as well.

### `db.client.operation.duration` does not mean what it appears to

All four verified against a real MongoDB, and each produces a plausible wrong answer rather than an
error:

- **`error.type` covers only commands the server refused, never a write error.** A duplicate key,
  failed validation, or any per-document write error is carried in the response body of a command the
  server *answered successfully* — the driver emits `commandSucceeded` while throwing to the caller.
  An alert assuming every database exception raises this rate would never fire.
- **A database outage makes the metric go silent rather than raising an error rate.** A
  server-selection failure emits no command events at all. Alert on absence of data.
- **`error.type` is the driver event's exception class, not the caller's.** A bad index hint arrives
  as `MongoCommandException` while the caller catches `MongoQueryException`.
- **`countDocuments()` issues an `aggregate`, not a `count`.** Only `estimatedDocumentCount()` issues
  `count`.

Correlation of a command's start (which carries the collection) to its end (which carries the
duration) is a `ThreadLocal`, not a map: the driver delivers both on the same thread, verified across
3364 commands on 25 threads. A map would need an eviction policy for an end event that never arrives.

### RPC-layer validation rejects are invisible to `dp.query.requests`

A request malformed enough to fail `QueryServiceImpl`'s field validation is rejected before the
handler is entered, so no `QueryTelemetry` exists and nothing is counted. Resolution-stage rejects
*are* counted (`completeRejectedResolution`) — the asymmetry is easy to miss.

Nor are they visible as gRPC errors: the service reports a rejection as an `OK` response carrying
an `ExceptionalResult` payload, so `grpc_status` stays `OK`. Verified against a running service —
an empty `queryData` request produced two `grpc_server_call_started_total` and one
`dp_query_requests_total` point, all `OK`. Counting them needs a telemetry context created at the
service layer; that is a follow-on. Until then the only signal is the difference between the two
counters, which `doc/metrics.md` documents as a prompt to read the log rather than a number to
alert on.

### The three timing layers nest, and the two gaps are not interchangeable

`dp_query_stage_duration_seconds{dp_stage="total"}` starts at **handler entry**, not at the wire.
Around it sit `grpc_server_call_duration_seconds` (the gRPC call, including decode) and, outside
that, whatever the client measures. The three nest strictly, and the gaps were measured under load
(#212 Task 11, 100 concurrent `queryDataStream` requests): ~9 ms/request between the gRPC span and
the handler span, ~16 ms/request between the client and the gRPC span.

So a client complaint of a slow query is not refuted by a healthy `dp_query_stage_duration` — that
histogram cannot see request decode, response wire time, or client-side deserialization. Compare
against the gRPC family for the first, and note that the two families' method labels do not join:
`grpc_method` is fully qualified (`dp.service.query.DpQueryService/queryDataStream`) where
`rpc_method` is bare.

Within the handler span, `process` is computed as `total` minus the three measured stages, so the
four stage fractions sum to 1 unconditionally. That identity is arithmetic, not a check that the
stages are attributed correctly; do not treat it as one.

### Testing telemetry

`DpTelemetry.initForTest(sdk)` + an `InMemoryMetricReader` is the seam; `GrpcIntegrationTestBase`
installs one before any wrapper's `init()` and resets after every `fini()`, so a handler closing its
observable-gauge registration still finds a live meter provider.

**Both services record their measurements after the response the client is waiting on has been
sent** — `QueryTelemetry.complete()` once `executeAndDispatch` returns, ingestion's on the worker
long after the enqueue ack. A test asserting as soon as the stub returns races the recording and
fails intermittently against correct code. Poll the relevant `*.requests` counter to a deadline; at
the unit level, enqueue a barrier job **of a distinct class** (so it gets its own `dp.job` point and
cannot inflate the counts under test) and wait for it to start.

`QueryHandler.slowQueryLogThresholdMillis` is resolved **once per JVM** and `ConfigurationManager`
folds `-D` overrides in once at singleton init, so there is no per-test override: it is set to `0` in
`src/test/resources/application.yml` for the whole suite. Production stays at 1000.

## Schema Migration (issue #254)

Schema changes are delivered by a versioned migration runner that executes during
`MongoClientBase.init()`, and the mechanism **fails closed**: a database whose schema version the
binary cannot establish stops the service rather than being served from. Operator documentation is
`doc/runbooks/schema-migration.md`.

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

## Releases

Tagged as `rel-<version>`; `release.yml` builds dp-grpc at the matching tag, runs the full
`mvn verify` against a MongoDB container, and attaches the shaded JAR, a `SHA256SUMS` listing it,
and `SHA256SUMS.cosign.bundle` — a keyless Sigstore signature (`cosign sign-blob`) binding the JAR
to the repo, workflow, tag, and source commit. `README.env` has the `cosign verify-blob`
invocation. Signing landed under #221; releases through 1.16.0 shipped an unsigned `.sha256` whose
recorded `release/`-prefixed path broke `sha256sum -c`. The container image is not signed yet
(#221 PR 2).

**`release.yml` is three jobs, and the OIDC token never shares a job with project code.**
`build` (`contents: read`) runs everything from the repository, the test suite included; `sign`
(`id-token: write`) has no checkout and runs only `sha256sum` and cosign over `build`'s uploaded
artifact; `publish` (`contents: write`) holds no signing token. dp-grpc uses two jobs because its
signing job runs only `mvn package`; here the build job runs a half-hour of tests, every Maven
plugin, and a dp-grpc build from another repo, and `id-token: write` exposes the token-request
credentials to every step of its job. Do not merge `sign` back into `build`. The split does not
stop `build` from tampering with the jar before it is checksummed — it confines the signing
identity to what `build` handed over, for seconds rather than the whole run. `VERSION` crosses
into `sign` as a job output read through step `env:`, never `${{ }}`-spliced into a script, since
on a rehearsal it is POM content.

**`IS_RELEASE` is keyed off the event, not the ref type**, in both release workflows:
`github.event_name == 'push' && startsWith(github.ref, 'refs/tags/rel-')`. A `workflow_dispatch`
can target a tag, so `github.ref_type == 'tag'` does not mean "release" — `release-image.yml` used
exactly that check to move `:latest` (and its dp-grpc ref resolution used it too, until #298's
review). In `release.yml` the expression appears twice, as `build`'s
env and literally in `publish.if` (a job `if:` cannot read `env`); passing it as a `build` output
instead would let the job running project code decide whether publishing happens.

**Rehearse with `gh workflow run release.yml --ref <branch>`, never by pushing a tag.** A
rehearsal builds, tests and signs, takes its version from the POM, and builds against dp-grpc
`rel-<dp-grpc.version>` (falling back to `main`, logged); `publish` is skipped. It signs for real,
leaving a permanent public Rekor entry naming its ref. A dispatch against a **tag** ref is refused
in `build`'s "Derive version" step: its certificate identity would be
`release.yml@refs/tags/rel-<version>`, the release's own, differing only in the workflow trigger.
The published verify command therefore pins both — an exact `--certificate-identity` naming the one
tag being verified (a pattern over any `rel-` tag accepts an older release's genuine signature
substituted for a newer one's) and `--certificate-github-workflow-trigger push` — so the refusal is
defense in depth rather than the only barrier. On a release the same step fails unless the tag
version equals both `project.version` and `dp-grpc.version`: the jar is renamed to the tag's version
regardless of the POM, so an unbumped POM would otherwise ship a signed jar whose name, reported
version, and dp-grpc dependency disagree. Since #221 only a `rel-*` tag push publishes an image — but a push runs the
workflow file **at the tagged commit**, so a tag of any name on a commit from before #221 still
runs the old `'*'` trigger, pushes an image, and moves `:latest`. That is why "never push a tag to
rehearse" is permanent rather than something #221 retired.

Release notes are version-controlled under `doc/release-notes/`, one document per release
(`rel-<version>.md`). A release note is organized by issue ticket rather than by PR, since a ticket
often spans several PRs, and a breaking release leads with an "Upgrading from <previous>" checklist
that calls out silent behavior changes separately from compile errors. Add each new document to the
table in the `## Release Notes` section of `README.md`.

**Notes accumulate in `doc/release-notes/NEXT.md` during a cycle** (adopted from dp-grpc under
#221). A PR that lands a user-visible change adds its own ticket-organized section there, so the
content is written while it is fresh and reviewed in the PR that causes it. At release time
`NEXT.md` is renamed to `rel-<version>.md` and finished — summary, "Upgrading from <previous>",
breaking-release framing — because only then is it known what the release contains. `NEXT.md`
carries its own "Cutting the release" checklist.

**Never name a version before the tag exists.** `release.yml` resolves the notes path strictly from
`GITHUB_REF_NAME`, so a file committed as `rel-<guess>.md` is both stranded and a failed
release-notes check on whatever tag does ship. For the same reason a section in `NEXT.md` must not
assert what *else* the release contains — a sibling ticket merging later falsifies it silently.

`release.yml` publishes `doc/release-notes/rel-<version>.md` as the GitHub release body via
`body_path`, and **the notes must be on the tagged commit**: write and merge them *before* pushing
the `rel-*` tag. The workflow checks for the file immediately after deriving the version rather than
leaving it to `action-gh-release` — here the build runs a MongoDB container and a full `mvn verify`,
so an unchecked missing-notes failure would surface only after several minutes of work, with the
release already half-published. Retagging is the only fix once the tag is pushed.

## Continuous Integration
- **GitHub Actions**: `.github/workflows/ci.yml`, one job, `build-and-test`
- **Multi-Repository Setup**: builds dp-grpc `main` before dp-service (the release builds against
  the matching dp-grpc tag instead, so a green CI run does not prove a green release build)
- **Triggers**: PRs to `main`, and pushes to `main`
- **Gate**: `mvn -B verify` — every unit *and* integration test runs on every PR (#250); it blocks
  the merge only once the ruleset requires `build-and-test` (below)
- **Services**: MongoDB 8 from `docker-compose.yaml`, started with `docker compose up -d --wait`
- **Artifacts**: compose logs and `docker ps` output, plus `target/surefire-reports` and
  `target/failsafe-reports`, uploaded on every run including failures

**Concurrency is keyed differently for PRs and pushes, and both halves are load-bearing.** PR runs
group on `github.ref` (`refs/pull/<n>/merge`) and cancel their superseded run; `github.head_ref` is
only the branch name, which two fork PRs can share. Push runs group on `github.sha`, so they never
share a group: a group holds one running and one *pending* run and a newer pending run replaces an
older one even with `cancel-in-progress: false`, which would leave a middle merge untested — the
combined-merge breakage the push trigger exists to catch.

**The job id `build-and-test` is the contract with the `main` ruleset**, whose required status
check matches it by name (added by #250 Task 7, after the `verify` switch ran green on `main`;
until then the suite runs on every PR but does not block a merge). Renaming a *step* is safe;
renaming the job leaves the rule waiting on a check that never reports, and every PR sits pending.
Change the ruleset in the same change.

**The worker poll timeout is what bounds integration-test teardown.** Idle `QueueHandlerBase`
workers check `shutdownRequested` only between polls, and `fini()` waits for all of them, so each
handler's shutdown takes up to `POLL_TIMEOUT_MILLIS`, four handlers per test. At the former 1 s this
was ~4 s of every test and most of the suite's run time (26 minutes at `rel-1.16.0`); at 100 ms the
full `verify` runs in about 5½ minutes locally and 6½ on CI (the `mvn verify` step; about 8 for
the whole job). **If the suite gets slow, check this first**, before reaching for `@BeforeClass`
fixtures (which give up per-test database isolation) or a curated IT subset (which silently drops
coverage from the gate). Do not replace the poll with `shutdownNow()`, which interrupts in-flight
jobs `fini()` lets finish, or with a sentinel job, which adds a second `requestQueue.put` site.

### Vendored dependency: `cisd:jhdf5` (do not remove)

`cisd:jhdf5` and its support library `cisd:base` are **not on Maven Central**; their only public
host is `maven.scijava.org`. Both jars and POMs are committed under `third-party/cisd-jhdf5/` and
`third-party/cisd-base/`, and `third-party/install-vendored.sh` installs them into the local Maven
repository before dp-service builds on **every** build path: a step in `ci.yml`, `release.yml` and
`release-image.yml`, and a `RUN` in the `Dockerfile`'s builder stage, which cannot see the runner's
`~/.m2`. Deleting either directory breaks all four; a new build path needs the same step.

Before #250 Task 4 only CI had the step, so a release depended on SciJava being up, and only jhdf5
was vendored: `cisd:base` had kept resolving during the outage below, so CI still needed SciJava
for it. A Docker build with `--add-host maven.scijava.org:127.0.0.1` failed on `cisd:base` until
that was vendored too.

**CI enforces the independence rather than trusting anyone to re-run that build:** `ci.yml`'s
"Block maven.scijava.org" step points the host at loopback in `/etc/hosts` for the whole job, so
`mvn verify` (every plugin included) must resolve without it. The `sci-java` repository stays in
`pom.xml` — Maven falls through a refused connection to Central, and developers with an empty
`~/.m2` still resolve — so the block fails exactly one thing: a new SciJava-only dependency, which
must then be vendored and added to `install-vendored.sh`. Do not remove the block to "fix" such a
failure. `release-image.yml`'s install step alone is guarded by an existence check, because a manual
dispatch can build a ref older than the script.

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
