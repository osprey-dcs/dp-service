# Issue #178: PV Metadata API — Implementation Plan

## Context

The dp-grpc project (companion protobuf/gRPC definition repo) has two categories of changes that require corresponding handler implementations in dp-service:

1. **Query service renames** — `queryPvMetadata()` and `queryProviderMetadata()` were poorly named because they return *archive ingestion statistics*, not user-defined metadata. They have been renamed `queryPvStats()` and `queryProviderStats()` with corresponding message type renames (`QueryPvStatsRequest/Response`, `QueryProviderStatsRequest/Response`, nested `StatsResult`, `PvStats`, `ProviderStats`). No behavioral change — purely nomenclature cleanup.

2. **New annotation service PV metadata API** — Six new RPC methods on `DpAnnotationService` for managing a new `pvMetadata` MongoDB collection: `savePvMetadata()`, `queryPvMetadata()`, `getPvMetadata()`, `deletePvMetadata()`, `patchPvMetadata()` (stub), and `bulkSavePvMetadata()` (stub). The `PvMetadata` message is defined in `common.proto` and stores canonical name, aliases, tags, attributes, description, modifiedBy, and managed timestamps.

dp-grpc has already been rebuilt; the generated Java classes are available locally.

---

## Phase 1 — Query Service Renames

Pure find-and-replace renames across the query service layer. No logic changes.

### 1.1 Rename classes and files

| Old name | New name |
|---|---|
| `QueryPvMetadataDispatcher` | `QueryPvStatsDispatcher` |
| `QueryPvMetadataJob` | `QueryPvStatsJob` |
| `QueryProviderMetadataDispatcher` | `QueryProviderStatsDispatcher` |
| `QueryProviderMetadataJob` | `QueryProviderStatsJob` |

Rename the `.java` source files to match.

### 1.2 Update `QueryServiceImpl`

**File:** `src/main/java/com/ospreydcs/dp/service/query/service/QueryServiceImpl.java`

- Rename `queryPvMetadata()` override → `queryPvStats()`
- Rename all helper methods (`QueryPvMetadataResponseReject` → `QueryPvStatsResponseReject`, etc.)
- Update all message type references:
  - `QueryPvMetadataRequest` → `QueryPvStatsRequest`
  - `QueryPvMetadataResponse` → `QueryPvStatsResponse`
  - `QueryPvMetadataResponse.MetadataResult` → `QueryPvStatsResponse.StatsResult`
  - `QueryPvMetadataResponse.MetadataResult.PvInfo` → `QueryPvStatsResponse.StatsResult.PvStats`
- Rename `queryProviderMetadata()` override → `queryProviderStats()`
- Rename corresponding helper methods
- Update `QueryProviderMetadataRequest/Response` → `QueryProviderStatsRequest/Response`
- Update `QueryProviderMetadataResponse.MetadataResult` → `QueryProviderStatsResponse.StatsResult`
- `ProviderMetadata` (standalone message) → `ProviderStats`

### 1.3 Update `QueryHandlerInterface`

**File:** `src/main/java/com/ospreydcs/dp/service/query/handler/interfaces/QueryHandlerInterface.java`

- Rename method signatures: `handleQueryPvMetadata` → `handleQueryPvStats`, `handleQueryProviderMetadata` → `handleQueryProviderStats`
- Update request/response generic types to new names

### 1.4 Update `MongoQueryHandler`

**File:** `src/main/java/com/ospreydcs/dp/service/query/handler/mongo/MongoQueryHandler.java`

- Rename `handleQueryPvMetadata` → `handleQueryPvStats`; update Job class instantiation to `QueryPvStatsJob`
- Rename `handleQueryProviderMetadata` → `handleQueryProviderStats`; update Job class instantiation to `QueryProviderStatsJob`

### 1.5 Update renamed Dispatcher classes

**Files (after rename):**
- `query/handler/mongo/dispatch/QueryPvStatsDispatcher.java`
- `query/handler/mongo/dispatch/QueryProviderStatsDispatcher.java`

- Update all references to old message types:
  - `QueryPvMetadataResponse` → `QueryPvStatsResponse`
  - `MetadataResult` → `StatsResult`
  - `PvInfo` → `PvStats`
  - `providerMetadataFromDocument()` helper in `QueryProviderStatsDispatcher`: `ProviderMetadata` → `ProviderStats`
  - `QueryProviderMetadataResponse` → `QueryProviderStatsResponse`
- Update calls to service response helper methods (now renamed in QueryServiceImpl)

### 1.6 Update renamed Job classes

**Files (after rename):**
- `query/handler/mongo/job/QueryPvStatsJob.java`
- `query/handler/mongo/job/QueryProviderStatsJob.java`

- Update request/response type references
- Update dispatcher class name references

### 1.7 Update `MongoQueryClientInterface` and `MongoSyncQueryClient`

**Files:**
- `query/handler/mongo/client/MongoQueryClientInterface.java`
- `query/handler/mongo/client/MongoSyncQueryClient.java`

- Rename method signatures: `executeQueryPvMetadata` → `executeQueryPvStats`, `executeQueryProviderMetadata` → `executeQueryProviderStats`
- Update request type parameters: `QueryPvMetadataRequest` → `QueryPvStatsRequest`, `QueryProviderMetadataRequest` → `QueryProviderStatsRequest`
- No changes to aggregation pipeline logic

### 1.8 Update integration tests

**Files:**
- `src/test/integration/java/com/ospreydcs/dp/service/integration/query/QueryPvMetadataIT.java` → rename to `QueryPvStatsIT.java`
- `src/test/integration/java/com/ospreydcs/dp/service/integration/query/QueryProviderMetadataIT.java` → rename to `QueryProviderStatsIT.java`
- `src/test/integration/java/com/ospreydcs/dp/service/integration/query/GrpcIntegrationQueryServiceWrapper.java`

Update all test classes:
- Rename test classes and files to `QueryPvStatsIT`, `QueryProviderStatsIT`
- Update all message type references and method calls in `GrpcIntegrationQueryServiceWrapper`
- Also update wrapper methods: `sendAndVerifyQueryPvMetadata` → `sendAndVerifyQueryPvStats`, etc.

**Open question:** The import at line 8 of `GrpcIntegrationQueryServiceWrapper.java` is `import com.ospreydcs.dp.grpc.v1.query.ProviderMetadata;` — confirm the generated class is now `ProviderStats` (it is, based on proto review).

---

## Phase 2 — New PV Metadata API (Annotation Service)

Implement the six new RPC methods as a complete CRUD pipeline following the existing annotation service patterns.

### 2.1 Create `PvMetadataDocument` BSON class

**New file:** `src/main/java/com/ospreydcs/dp/service/common/bson/pvmetadata/PvMetadataDocument.java`

Extends `DpBsonDocumentBase` (inherits `tags`, `attributes`, `createdAt`, `updatedAt`).

Additional fields:
- `ObjectId id`
- `String pvName` — canonical PV name, used as natural key for upsert
- `List<String> aliases`
- `String description`
- `String modifiedBy`

Key methods:
- `fromSavePvMetadataRequest(SavePvMetadataRequest)` — static factory; normalizes tags to lowercase unique sorted list; validates and converts attributes via `AttributesUtility.attributeMapFromList()`
- `toPvMetadata()` — converts to `common.PvMetadata` protobuf; restores `createdTime`/`updatedTime` from `createdAt`/`updatedAt` using `TimestampUtility`; restores attributes via `AttributesUtility.attributeListFromMap()`

**Note:** Tags must be normalized to lowercase unique set on save (per proto comment). `aliases` is a `List<String>` stored directly. Attributes stored as `Map<String,String>` (existing pattern from `DpBsonDocumentBase`/`ColumnMetadataDocument`).

### 2.2 Add `pvMetadata` collection and register POJO codec

**File:** `src/main/java/com/ospreydcs/dp/service/common/mongo/MongoClientBase.java`

- Add: `public static final String COLLECTION_NAME_PV_METADATA = "pvMetadata";`
- Register `PvMetadataDocument.class` in `getPojoCodecRegistry()`

**File:** `src/main/java/com/ospreydcs/dp/service/common/bson/BsonConstants.java`

Add new constants for the pvMetadata collection:
```java
public static final String BSON_KEY_USER_PV_METADATA_PV_NAME    = "pvName";
public static final String BSON_KEY_USER_PV_METADATA_ALIASES    = "aliases";
public static final String BSON_KEY_USER_PV_METADATA_DESCRIPTION = "description";
public static final String BSON_KEY_USER_PV_METADATA_MODIFIED_BY = "modifiedBy";
```
(tags, attributes, createdAt, updatedAt keys already exist in BsonConstants)

**Note:** MongoDB index strategy — `pvName` should have a unique index. `aliases` should also be indexed for `getPvMetadata` and `deletePvMetadata` lookup by alias. Consider whether to create indexes programmatically on collection init or rely on a migration script. Recommend creating in `MongoAnnotationClientInterface.init()` using `createIndex()` with a unique partial filter expression.

### 2.3 Add PV metadata methods to `MongoAnnotationClientInterface`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/client/MongoAnnotationClientInterface.java`

Add:
```java
MongoSaveResult savePvMetadata(PvMetadataDocument document);
MongoCursor<PvMetadataDocument> executeQueryPvMetadata(QueryPvMetadataRequest request);
PvMetadataDocument findPvMetadataByNameOrAlias(String pvNameOrAlias);
MongoDeleteResult deletePvMetadata(String pvNameOrAlias);
```

### 2.4 Implement PV metadata methods in `MongoSyncAnnotationClient`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/client/MongoSyncAnnotationClient.java`

#### `savePvMetadata(PvMetadataDocument document)`

Upsert semantics using `findOneAndReplace` with `upsert=true` on `pvName` filter:
- Filter: `eq(BSON_KEY_USER_PV_METADATA_PV_NAME, document.getPvName())`
- If existing document found: preserve `createdAt` from existing doc, set `updatedAt = Instant.now()`
- If new: set `createdAt = Instant.now()`, `updatedAt = null`
- Use `ReplaceOptions().upsert(true)` with `FindOneAndReplaceOptions().upsert(true).returnDocument(AFTER)`
- Return `MongoSaveResult` with `pvName` as document id field

#### `executeQueryPvMetadata(QueryPvMetadataRequest request)`

Build a MongoDB `Filters.and()` of per-criterion filters, then execute a `find()` cursor with optional `skip`/`limit` for pagination:

- **PvNameCriterion** → `Filters.or(exact matches via `in()`, prefix matches via `regex("^prefix")`, contains matches via `regex(".*contains.*")`)` on `pvName` field
- **AliasesCriterion** → same pattern but on `aliases` field
- **TagsCriterion** → `Filters.in(BSON_KEY_TAGS, values)` 
- **AttributesCriterion** → key-only: `Filters.exists("attributes." + key)`, key+values: `Filters.in("attributes." + key, values)`
- **Pagination**: decode `pageToken` as integer offset (base64-encoded integer); apply `skip(offset).limit(limit)`; return `nextPageToken` = base64-encode of `offset + limit` if result count equals limit (otherwise empty)
- Sort by `pvName` ascending for stable pagination

#### `findPvMetadataByNameOrAlias(String pvNameOrAlias)`

Single document lookup: `Filters.or(eq(pvName, value), eq(aliases, value))` → `find().first()`

#### `deletePvMetadata(String pvNameOrAlias)`

`deleteOne()` with filter from `findPvMetadataByNameOrAlias`; return `MongoDeleteResult` (new simple result class analogous to `MongoSaveResult` with `isError`, `msg`, `deletedPvName`).

### 2.5 Add validation in `AnnotationValidationUtility`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/AnnotationValidationUtility.java`

Add methods:
- `validateSavePvMetadataRequest(SavePvMetadataRequest)` — pvName must not be blank; attribute keys must be unique (reject duplicates); tags normalized downstream (no validation needed)
- `validateQueryPvMetadataRequest(QueryPvMetadataRequest)` — criteria list must not be empty; each criterion must have exactly one oneof set; `AttributesCriterion.key` must not be blank; `limit` may be 0 (means no limit) or a positive integer
- `validateGetPvMetadataRequest(GetPvMetadataRequest)` — `pvNameOrAlias` must not be blank
- `validateDeletePvMetadataRequest(DeletePvMetadataRequest)` — `pvNameOrAlias` must not be blank

### 2.6 Add methods to `AnnotationHandlerInterface`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/interfaces/AnnotationHandlerInterface.java`

Add:
```java
void handleSavePvMetadata(SavePvMetadataRequest request, StreamObserver<SavePvMetadataResponse> responseObserver);
void handleQueryPvMetadata(QueryPvMetadataRequest request, StreamObserver<QueryPvMetadataResponse> responseObserver);
void handleGetPvMetadata(GetPvMetadataRequest request, StreamObserver<GetPvMetadataResponse> responseObserver);
void handleDeletePvMetadata(DeletePvMetadataRequest request, StreamObserver<DeletePvMetadataResponse> responseObserver);
void handlePatchPvMetadata(PatchPvMetadataRequest request, StreamObserver<PatchPvMetadataResponse> responseObserver);
void handleBulkSavePvMetadata(BulkSavePvMetadataRequest request, StreamObserver<BulkSavePvMetadataResponse> responseObserver);
```

### 2.7 Create Dispatcher classes

**New files in** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/dispatch/`:

#### `SavePvMetadataDispatcher.java`
- Constructor: `(StreamObserver<SavePvMetadataResponse> responseObserver, SavePvMetadataRequest request)`
- `handleValidationError(ResultStatus)` — calls `AnnotationServiceImpl.sendSavePvMetadataResponseReject()`
- `handleResult(MongoSaveResult)` — on error calls `sendSavePvMetadataResponseError()`; on success calls `sendSavePvMetadataResponseSuccess(pvName)`

#### `QueryPvMetadataDispatcher.java`
- Constructor: `(StreamObserver<QueryPvMetadataResponse> responseObserver, QueryPvMetadataRequest request)`
- `handleResult(MongoCursor<PvMetadataDocument> cursor, String nextPageToken)` — iterates cursor, calls `doc.toPvMetadata()` for each, builds `PvMetadataResult`, calls `sendQueryPvMetadataResponse()`

#### `GetPvMetadataDispatcher.java`
- `handleResult(PvMetadataDocument doc)` — if null, sends `NOT_FOUND` exceptional result; else sends `GetPvMetadataResult`

#### `DeletePvMetadataDispatcher.java`
- `handleResult(MongoDeleteResult result)` — sends error or `DeletePvMetadataResult` with deleted pvName

#### `PatchPvMetadataDispatcher.java` (stub)
- `handle()` — immediately sends `RESULT_STATUS_ERROR` with "patchPvMetadata not yet implemented"

#### `BulkSavePvMetadataDispatcher.java` (stub)
- `handle()` — immediately sends `RESULT_STATUS_ERROR` with "bulkSavePvMetadata not yet implemented"

### 2.8 Create Job classes

**New files in** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/job/`:

#### `SavePvMetadataJob.java`
```
execute():
  1. validate via AnnotationValidationUtility.validateSavePvMetadataRequest()
  2. generate PvMetadataDocument.fromSavePvMetadataRequest(request) — normalizes tags
  3. call mongoClient.savePvMetadata(document)
  4. dispatcher.handleResult(result)
```

#### `QueryPvMetadataJob.java`
```
execute():
  1. validate via AnnotationValidationUtility.validateQueryPvMetadataRequest()
  2. call mongoClient.executeQueryPvMetadata(request) → returns (cursor, nextPageToken)
  3. dispatcher.handleResult(cursor, nextPageToken)
```

#### `GetPvMetadataJob.java`
```
execute():
  1. validate via AnnotationValidationUtility.validateGetPvMetadataRequest()
  2. call mongoClient.findPvMetadataByNameOrAlias(request.getPvNameOrAlias())
  3. dispatcher.handleResult(document)  // null = not found
```

#### `DeletePvMetadataJob.java`
```
execute():
  1. validate via AnnotationValidationUtility.validateDeletePvMetadataRequest()
  2. call mongoClient.deletePvMetadata(request.getPvNameOrAlias())
  3. dispatcher.handleResult(result)
```

#### `PatchPvMetadataJob.java` (stub)
- `execute()` → `dispatcher.handle()` (immediately returns "not implemented")

#### `BulkSavePvMetadataJob.java` (stub)
- `execute()` → `dispatcher.handle()` (immediately returns "not implemented")

### 2.9 Update `MongoAnnotationHandler`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/MongoAnnotationHandler.java`

Implement the six new `AnnotationHandlerInterface` methods following `handleSaveDataSet` pattern: create job, call `requestQueue.put(job)`.

For stub methods (`patchPvMetadata`, `bulkSavePvMetadata`): still create and queue a job (the job itself immediately responds with "not implemented") — or alternatively respond directly in the handler without queuing. Recommend responding directly in the handler to avoid queue overhead for permanent stubs.

### 2.10 Update `AnnotationServiceImpl`

**File:** `src/main/java/com/ospreydcs/dp/service/annotation/service/AnnotationServiceImpl.java`

For each of the six new RPC methods, add:
1. Override of the gRPC stub method
2. Static response helper methods (Reject/Error/Success variants following existing pattern)
3. Static `sendXxx()` convenience methods
4. Request validation → delegate to `handler.handleXxx()`

Specific method implementations:

#### `savePvMetadata()`
- Validate: pvName must be specified
- Then call `AnnotationValidationUtility.validateSavePvMetadataRequest()`
- Delegate to `handler.handleSavePvMetadata()`

#### `queryPvMetadata()`
- Validate: criteria list must not be empty (quick check before full validation)
- Delegate to `handler.handleQueryPvMetadata()`

#### `getPvMetadata()`
- Validate: pvNameOrAlias must not be blank
- Delegate to `handler.handleGetPvMetadata()`

#### `deletePvMetadata()`
- Validate: pvNameOrAlias must not be blank
- Delegate to `handler.handleDeletePvMetadata()`

#### `patchPvMetadata()` (stub)
- Immediately respond with `RESULT_STATUS_ERROR`: "patchPvMetadata not yet implemented"
- No handler delegation needed

#### `bulkSavePvMetadata()` (stub)
- Immediately respond with `RESULT_STATUS_ERROR`: "bulkSavePvMetadata not yet implemented"
- No handler delegation needed

---

## Phase 3 — Tests

### 3.1 Phase 1 test changes (query renames)

- Rename `QueryPvMetadataIT.java` → `QueryPvStatsIT.java` (rename class, update all type references)
- Rename `QueryProviderMetadataIT.java` → `QueryProviderStatsIT.java` (rename class, update all type references)
- Update `GrpcIntegrationQueryServiceWrapper.java`: rename all helper methods and update message type references

### 3.2 Phase 2 integration tests

**New file:** `src/test/integration/java/com/ospreydcs/dp/service/integration/annotation/PvMetadataIT.java`

Extends `AnnotationIntegrationTestIntermediate` (or directly `GrpcIntegrationTestBase`).

Uses `GrpcIntegrationAnnotationServiceWrapper` for gRPC calls and `MongoTestClient` for DB verification.

**Test scenarios (all round-trip):**

#### savePvMetadata
- Reject: empty pvName
- Reject: duplicate attribute keys
- Success: create new record (verify document in MongoDB via `mongoTestClient.findPvMetadata(pvName)`)
  - Verify `createdAt` set, `updatedAt` null
  - Verify tags normalized to lowercase unique sorted
  - Verify aliases, attributes, description, modifiedBy stored correctly
- Success: full-replace update of existing record (same pvName)
  - Verify `createdAt` preserved from original, `updatedAt` set
  - Verify all fields replaced with new values

#### queryPvMetadata
- Reject: empty criteria list
- Reject: AttributesCriterion with blank key
- Success (empty result): query with non-matching criteria
- Success: PvNameCriterion exact match
- Success: PvNameCriterion prefix match
- Success: PvNameCriterion contains match
- Success: AliasesCriterion exact match
- Success: TagsCriterion match
- Success: AttributesCriterion key-only match
- Success: AttributesCriterion key+value match
- Success: multi-criterion AND (pvName + tags)
- Success: pagination (create 3 records, query with limit=2, verify nextPageToken, retrieve page 2)

#### getPvMetadata
- Reject: blank pvNameOrAlias
- Not found: non-existent name (returns ExceptionalResult NOT_FOUND or empty result per proto)
- Success: lookup by canonical pvName
- Success: lookup by alias

#### deletePvMetadata
- Reject: blank pvNameOrAlias
- Success: delete by canonical pvName; verify document removed from MongoDB
- Success: delete by alias; verify document removed from MongoDB
- Error: delete non-existent record (returns error or not-found exceptional result)

#### patchPvMetadata / bulkSavePvMetadata stubs
- Single test each: verify RESULT_STATUS_ERROR response with appropriate message

### 3.3 Add `MongoTestClient.findPvMetadata()`

**File:** `src/test/java/com/ospreydcs/dp/service/common/mongo/MongoTestClient.java`

Add `findPvMetadata(String pvName)` method following the retry-loop pattern of `findAnnotation()`.

### 3.4 Update `GrpcIntegrationAnnotationServiceWrapper`

Add helper methods for the new API calls:
- `sendAndVerifySavePvMetadata(params, expectedSuccess, expectedErrorMsg)`
- `sendAndVerifyQueryPvMetadata(request, expectedSuccess, expectedPvNames)`
- `sendAndVerifyGetPvMetadata(pvNameOrAlias, expectedFound, expectedPvName)`
- `sendAndVerifyDeletePvMetadata(pvNameOrAlias, expectedSuccess, expectedPvName)`

---

## Design Decisions (resolved)

1. **Not-found behavior** (`getPvMetadata`, `deletePvMetadata`): Return `RESULT_STATUS_REJECT` with message `"no PvMetadata record found for: {pvNameOrAlias}"`.

2. **Alias uniqueness**: Aliases must be globally unique across all PV records. `savePvMetadata()` must check for alias conflicts (any alias already used by a *different* pvName record) and reject with a descriptive error. The `aliases` field has a MongoDB index, and this conflict check is done in `SavePvMetadataJob` before the upsert.

3. **Pagination token**: Base64-encoded integer skip offset. Stateless and simple.

4. **MongoDB indexes**: Created programmatically in `MongoSyncAnnotationClient.init()` via `createIndex()`. Safe to run on every startup (idempotent).

---

## Files Changed Summary

### Phase 1 (renames)
| Action | File |
|---|---|
| Modify | `query/service/QueryServiceImpl.java` |
| Modify | `query/handler/interfaces/QueryHandlerInterface.java` |
| Modify | `query/handler/mongo/MongoQueryHandler.java` |
| Rename+Modify | `query/handler/mongo/dispatch/QueryPvMetadataDispatcher.java` → `QueryPvStatsDispatcher.java` |
| Rename+Modify | `query/handler/mongo/dispatch/QueryProviderMetadataDispatcher.java` → `QueryProviderStatsDispatcher.java` |
| Rename+Modify | `query/handler/mongo/job/QueryPvMetadataJob.java` → `QueryPvStatsJob.java` |
| Rename+Modify | `query/handler/mongo/job/QueryProviderMetadataJob.java` → `QueryProviderStatsJob.java` |
| Modify | `query/handler/mongo/client/MongoQueryClientInterface.java` |
| Modify | `query/handler/mongo/client/MongoSyncQueryClient.java` |
| Rename+Modify | `integration/query/QueryPvMetadataIT.java` → `QueryPvStatsIT.java` |
| Rename+Modify | `integration/query/QueryProviderMetadataIT.java` → `QueryProviderStatsIT.java` |
| Modify | `integration/query/GrpcIntegrationQueryServiceWrapper.java` |

### Phase 2 (new PV metadata API)
| Action | File |
|---|---|
| New | `common/bson/pvmetadata/PvMetadataDocument.java` |
| Modify | `common/bson/BsonConstants.java` |
| Modify | `common/mongo/MongoClientBase.java` |
| Modify | `annotation/handler/AnnotationValidationUtility.java` |
| Modify | `annotation/handler/interfaces/AnnotationHandlerInterface.java` |
| Modify | `annotation/handler/mongo/MongoAnnotationHandler.java` |
| Modify | `annotation/handler/mongo/client/MongoAnnotationClientInterface.java` |
| Modify | `annotation/handler/mongo/client/MongoSyncAnnotationClient.java` |
| Modify | `annotation/service/AnnotationServiceImpl.java` |
| New | `annotation/handler/mongo/dispatch/SavePvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/dispatch/QueryPvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/dispatch/GetPvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/dispatch/DeletePvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/dispatch/PatchPvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/dispatch/BulkSavePvMetadataDispatcher.java` |
| New | `annotation/handler/mongo/job/SavePvMetadataJob.java` |
| New | `annotation/handler/mongo/job/QueryPvMetadataJob.java` |
| New | `annotation/handler/mongo/job/GetPvMetadataJob.java` |
| New | `annotation/handler/mongo/job/DeletePvMetadataJob.java` |
| New | `annotation/handler/mongo/job/PatchPvMetadataJob.java` |
| New | `annotation/handler/mongo/job/BulkSavePvMetadataJob.java` |
| Modify | `test/.../mongo/MongoTestClient.java` |
| Modify | `integration/annotation/GrpcIntegrationAnnotationServiceWrapper.java` |
| New | `integration/annotation/PvMetadataIT.java` |

All source file paths are relative to `src/main/java/com/ospreydcs/dp/service/` (or `src/test/...`).

---

## Verification

```bash
# Build without tests to confirm compilation after all renames
mvn clean package -DskipTests

# Run Phase 1 (renamed) integration tests
mvn test -Dtest=QueryPvStatsIT
mvn test -Dtest=QueryProviderStatsIT

# Run Phase 2 new integration tests
mvn test -Dtest=PvMetadataIT

# Run all annotation service integration tests to check for regressions
mvn test -Dtest=SaveAnnotationIT,SaveDataSetIT,QueryAnnotationsIT,QueryDataSetsIT,ExportDataIT,PvMetadataIT
```
