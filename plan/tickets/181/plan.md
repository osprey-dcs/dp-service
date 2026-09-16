# dp-service #181: Machine Configuration API — Implementation Plan

## Reference
- Issue description: `.dev/plan/issue-181/issue-description.md`
- dp-grpc #120 implementation notes: `~/dp.fork/dp-java/dp-grpc/.dev/plan/issue-120/implementation-notes.md`
- Reference implementation: PV Metadata API (dp-service #178)

---

## Overview

This plan is broken into four phases:

| Phase | Scope |
|---|---|
| 1 | Infrastructure: BSON documents, MongoDB collections, constants, codec registration, result model classes |
| 2 | Configuration CRUD: all jobs, dispatchers, handler, service methods (save/get/query/delete + stubs) |
| 3 | Configuration Activation CRUD: all jobs, dispatchers, handler, service methods (save/get/query/delete + stubs), including overlap enforcement and `getActiveConfigurations` |
| 4 | Integration testing: `ConfigurationIT`, test framework support (AnnotationTestBase, MongoTestClient, GrpcIntegrationAnnotationServiceWrapper), annotation regression suite |

Each phase ends with a clean build verification. Testing is front-loaded: the test framework additions happen at the start of Phase 4, before writing any test scenarios, following the pattern in PvMetadataIT.

---

## Phase 1 — Infrastructure

### Step 1.1 — New result model class: `ConfigurationQueryResult`

**File to create:**
`src/main/java/com/ospreydcs/dp/service/common/model/ConfigurationQueryResult.java`

Mirror `PvMetadataQueryResult`:
- `List<ConfigurationDocument> documents`
- `String nextPageToken`
- Constructor, getters

**File to create:**
`src/main/java/com/ospreydcs/dp/service/common/model/ConfigurationActivationQueryResult.java`

Mirror the same pattern:
- `List<ConfigurationActivationDocument> documents`
- `String nextPageToken`

> Note: `MongoSaveResult` and `MongoDeleteResult` are already in `common/model/` and can be reused as-is. No new save/delete result classes needed.

---

### Step 1.2 — BSON document class: `ConfigurationDocument`

**File to create:**
`src/main/java/com/ospreydcs/dp/service/common/bson/configuration/ConfigurationDocument.java`

Extend `DpBsonDocumentBase` (provides `tags`, `attributes`, `createdAt`, `updatedAt` with `addCreationTime()` / `addUpdatedTime()`).

Fields (all need getters **and** setters — POJO codec silently drops fields missing either):
```
String configurationName       // canonical key
String category                // required
String description             // optional
String parentConfigurationName // optional
String modifiedBy              // client-supplied, stored as-is
```
Inherited from `DpBsonDocumentBase`:
```
List<String> tags
Map<String,String> attributes
Instant createdAt
Instant updatedAt
```

Methods:
- `static ConfigurationDocument fromSaveConfigurationRequest(SaveConfigurationRequest request)` — normalizes tags, converts attributes via `AttributesUtility.attributeMapFromList()`; does NOT set `createdAt`/`updatedAt` (server-set in job)
- `Configuration toConfiguration()` — builds proto `Configuration` message; converts `createdAt`/`updatedAt` via `TimestampUtility.getTimestampFromInstant()`; converts attributes back via `AttributesUtility.attributeListFromMap()`

---

### Step 1.3 — BSON document class: `ConfigurationActivationDocument`

**File to create:**
`src/main/java/com/ospreydcs/dp/service/common/bson/configuration/ConfigurationActivationDocument.java`

Extend `DpBsonDocumentBase`.

Fields:
```
String  clientActivationId      // optional client-supplied or server-generated; stored as canonical key
String  configurationName       // FK to configurations collection
String  internalCategory        // denormalized from Configuration.category; internal field, not in API
Instant startTime               // required
Instant endTime                 // optional (null = open-ended)
String  description             // optional
String  modifiedBy              // client-supplied, stored as-is
```
Inherited from `DpBsonDocumentBase`:
```
List<String> tags
Map<String,String> attributes
Instant createdAt
Instant updatedAt
```

Methods:
- `static ConfigurationActivationDocument fromSaveConfigurationActivationRequest(SaveConfigurationActivationRequest request)` — normalizes tags, converts attributes, converts `startTime`/`endTime` from proto `Timestamp` to `Instant` via `TimestampUtility`; does NOT set `internalCategory`, `createdAt`, `updatedAt` (set in job)
- `ConfigurationActivation toConfigurationActivation()` — builds proto `ConfigurationActivation` message; converts `startTime`/`endTime`/`createdAt`/`updatedAt` to proto `Timestamp`

---

### Step 1.4 — BsonConstants

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/common/bson/BsonConstants.java`

Add constants:
```java
// configurations collection
public static final String BSON_KEY_CONFIGURATION_NAME = "configurationName";
public static final String BSON_KEY_CONFIGURATION_CATEGORY = "category";
public static final String BSON_KEY_CONFIGURATION_DESCRIPTION = "description";
public static final String BSON_KEY_CONFIGURATION_PARENT_NAME = "parentConfigurationName";
public static final String BSON_KEY_CONFIGURATION_MODIFIED_BY = "modifiedBy";

// configurationActivations collection
public static final String BSON_KEY_ACTIVATION_CLIENT_ID = "clientActivationId";
public static final String BSON_KEY_ACTIVATION_CONFIGURATION_NAME = "configurationName";
public static final String BSON_KEY_ACTIVATION_INTERNAL_CATEGORY = "internalCategory";
public static final String BSON_KEY_ACTIVATION_START_TIME = "startTime";
public static final String BSON_KEY_ACTIVATION_END_TIME = "endTime";
public static final String BSON_KEY_ACTIVATION_DESCRIPTION = "description";
public static final String BSON_KEY_ACTIVATION_MODIFIED_BY = "modifiedBy";
```

---

### Step 1.5 — MongoClientBase: collection names and codec registration

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/common/mongo/MongoClientBase.java`

Add collection name constants (alongside `COLLECTION_NAME_PV_METADATA`):
```java
public static final String COLLECTION_NAME_CONFIGURATIONS = "configurations";
public static final String COLLECTION_NAME_CONFIGURATION_ACTIVATIONS = "configurationActivations";
```

Register both new document classes in `getPojoCodecRegistry()`, after `PvMetadataDocument.class`:
```java
.register(ConfigurationDocument.class)
.register(ConfigurationActivationDocument.class)
```

---

### Phase 1 Build Verification

```
mvn clean package -DskipTests
```

Expected: clean compile; no runtime changes yet.

---

## Phase 2 — Configuration CRUD

This phase implements all 6 Configuration API methods: `saveConfiguration`, `getConfiguration`, `queryConfigurations`, `deleteConfiguration`, `patchConfiguration` (stub), `bulkSaveConfiguration` (stub).

---

### Step 2.1 — MongoSyncAnnotationClient: collection init and indexes

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/client/MongoSyncAnnotationClient.java`

In `init()`, after pvMetadata collection init, add:
```java
// configurations collection
MongoCollection<ConfigurationDocument> configCol =
    database.getCollection(COLLECTION_NAME_CONFIGURATIONS, ConfigurationDocument.class);
configCol.createIndex(Indexes.ascending(BSON_KEY_CONFIGURATION_NAME),
    new IndexOptions().unique(true));
configCol.createIndex(Indexes.ascending(BSON_KEY_CONFIGURATION_CATEGORY));
configCol.createIndex(Indexes.ascending(BSON_KEY_CONFIGURATION_PARENT_NAME));
configCol.createIndex(Indexes.ascending(BSON_KEY_TAGS));
configCol.createIndex(Indexes.ascending("attributes.name"));
mongoCollectionConfigurations = configCol;
```

Add instance field:
```java
private MongoCollection<ConfigurationDocument> mongoCollectionConfigurations;
```

---

### Step 2.2 — MongoSyncAnnotationClient: `saveConfiguration()`

**Method signature:**
```java
public MongoSaveResult saveConfiguration(ConfigurationDocument document)
```

Logic (mirror `savePvMetadata`):
1. Find existing document by `configurationName` (exact match, not alias)
2. If existing document has a different `category` AND activations exist for this name → return error: `"cannot change category for configurationName '<name>': existing activations must be deleted first"`
3. If **new** record: call `document.addCreationTime()`, call `mongoCollectionConfigurations.insertOne(document)`, return `MongoSaveResult(false, null, configurationName, true)`
4. If **existing** record: preserve original `createdAt` (`document.setCreatedAt(existing.getCreatedAt())`), call `document.addUpdatedTime()`, call `mongoCollectionConfigurations.replaceOne(filter, document)`, return `MongoSaveResult(false, null, configurationName, false)`
5. Wrap in try-catch for `MongoException`; return `MongoSaveResult(true, exceptionMsg, null, false)` on error

> Note: checking for existing activations (step 2) requires querying `configurationActivations` with filter `configurationName == name`. Add a private helper `boolean activationsExistForConfiguration(String configurationName)`.

---

### Step 2.3 — MongoSyncAnnotationClient: `findConfigurationByName()`

```java
public ConfigurationDocument findConfigurationByName(String configurationName)
```

Simple `mongoCollectionConfigurations.find(eq(BSON_KEY_CONFIGURATION_NAME, configurationName)).first()`.

---

### Step 2.4 — MongoSyncAnnotationClient: `executeQueryConfigurations()`

```java
public ConfigurationQueryResult executeQueryConfigurations(QueryConfigurationsRequest request)
```

Logic (mirror `executeQueryPvMetadata`):

1. Build `List<Bson> filterList` from each criterion in `request.getCriteriaList()`:
   - `NameCriterion`: map `matchType` to exact / `^prefix` / `.*contains.*` regex on `BSON_KEY_CONFIGURATION_NAME`
   - `CategoryCriterion`: `Filters.eq(BSON_KEY_CONFIGURATION_CATEGORY, value)` for each value (ORed if multiple — use `Filters.in`)
   - `ParentCriterion`: exact or regex match on `BSON_KEY_CONFIGURATION_PARENT_NAME`
   - `TagsCriterion`: `Filters.in(BSON_KEY_TAGS, tagsList)`
   - `AttributesCriterion`: key-only → `Filters.exists("attributes." + key)`; key+values → `Filters.in("attributes." + key, values)`
2. Combine with `Filters.and(filterList)` (or no filter if list is empty)
3. Decode `pageToken` → skip offset (Base64 int, 0 if absent/empty)
4. Apply default limit if `request.getLimit() == 0` (use 100)
5. Execute: `mongoCollectionConfigurations.find(filter).sort(ascending(BSON_KEY_CONFIGURATION_NAME)).skip(skip).limit(limit)`
6. Fetch all results; compute `nextPageToken` if `skip + results.size() < totalCount`

> For `totalCount`, execute a separate `countDocuments(filter)` call before the find. This is acceptable given Phase 1 scope.

---

### Step 2.5 — MongoSyncAnnotationClient: `deleteConfiguration()`

```java
public MongoDeleteResult deleteConfiguration(String configurationName)
```

Logic:
1. Check `activationsExistForConfiguration(configurationName)` → if true, return reject: `"cannot delete configurationName '<name>': existing activations must be deleted first"` (use `isError=false` to signal reject, per `MongoDeleteResult` convention — or add a `isReject` flag; match the pattern used by `deletePvMetadata`)
2. `mongoCollectionConfigurations.deleteOne(eq(BSON_KEY_CONFIGURATION_NAME, configurationName))`
3. If `deleteResult.getDeletedCount() == 0` → return not-found indicator
4. Return `MongoDeleteResult(false, null, configurationName)`

> Check how `deletePvMetadata` returns the "not found" case in `MongoDeleteResult` and mirror that exactly (likely `deletedPvName == null`).

---

### Step 2.6 — MongoAnnotationClientInterface: add Configuration signatures

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/client/MongoAnnotationClientInterface.java`

Add:
```java
MongoSaveResult saveConfiguration(ConfigurationDocument document);
ConfigurationDocument findConfigurationByName(String configurationName);
ConfigurationQueryResult executeQueryConfigurations(QueryConfigurationsRequest request);
MongoDeleteResult deleteConfiguration(String configurationName);
```

Add no-op stubs to `MongoAsyncAnnotationClient` (throws `UnsupportedOperationException` or returns null) since it implements the interface.

---

### Step 2.7 — Dispatchers for Configuration

**Files to create** (each mirrors corresponding PvMetadata dispatcher):

**`SaveConfigurationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → sendSaveConfigurationResponseReject
handleError(String)                  → sendSaveConfigurationResponseError
handleResult(MongoSaveResult)        → success with configurationName, or error
```

**`GetConfigurationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → sendGetConfigurationResponseReject
handleError(String)                  → sendGetConfigurationResponseError
handleResult(ConfigurationDocument)  → null → reject "no Configuration found for: <name>"; else → success
```

**`QueryConfigurationsDispatcher.java`**:
```
handleValidationError(ResultStatus)  → sendQueryConfigurationsResponseReject
handleError(String)                  → sendQueryConfigurationsResponseError
handleResult(ConfigurationQueryResult) → build QueryConfigurationsResult with Configuration list and nextPageToken
```

**`DeleteConfigurationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → sendDeleteConfigurationResponseReject
handleResult(MongoDeleteResult)      → not-found → reject; error → error; else → success with configurationName
```

---

### Step 2.8 — Jobs for Configuration

**Files to create:**

**`SaveConfigurationJob.java`** (mirror `SavePvMetadataJob`):
- Validate: `configurationName` not blank → dispatcher.handleValidationError
- Validate: `category` not blank → dispatcher.handleValidationError
- Validate: no duplicate attribute keys → dispatcher.handleValidationError
- Build `ConfigurationDocument.fromSaveConfigurationRequest(request)`
- Call `mongoClient.saveConfiguration(document)`
- Delegate to `dispatcher.handleResult(saveResult)`

**`GetConfigurationJob.java`** (mirror `GetPvMetadataJob`):
- Validate: `configurationName` not blank
- Call `mongoClient.findConfigurationByName(name)`
- Delegate to `dispatcher.handleResult(doc)` (dispatcher handles null → reject)

**`QueryConfigurationsJob.java`** (mirror `QueryPvMetadataJob`):
- Validate: criteria list not empty
- Validate each criterion (non-blank values where required)
- Call `mongoClient.executeQueryConfigurations(request)`
- Delegate to `dispatcher.handleResult(result)`

**`DeleteConfigurationJob.java`** (mirror `DeletePvMetadataJob`):
- Validate: `configurationName` not blank
- Call `mongoClient.deleteConfiguration(configurationName)`
- Delegate to `dispatcher.handleResult(deleteResult)`

---

### Step 2.9 — AnnotationHandlerInterface and MongoAnnotationHandler

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/annotation/handler/interfaces/AnnotationHandlerInterface.java`

Add:
```java
void handleSaveConfiguration(SaveConfigurationRequest request, StreamObserver<SaveConfigurationResponse> responseObserver);
void handleGetConfiguration(GetConfigurationRequest request, StreamObserver<GetConfigurationResponse> responseObserver);
void handleQueryConfigurations(QueryConfigurationsRequest request, StreamObserver<QueryConfigurationsResponse> responseObserver);
void handleDeleteConfiguration(DeleteConfigurationRequest request, StreamObserver<DeleteConfigurationResponse> responseObserver);
```

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/annotation/handler/mongo/MongoAnnotationHandler.java`

Implement each: create job, call `executeJob(job)` (mirror `handleSavePvMetadata` etc.).

---

### Step 2.10 — AnnotationServiceImpl: Configuration methods

**File to modify:**
`src/main/java/com/ospreydcs/dp/service/annotation/service/AnnotationServiceImpl.java`

Add static response helper methods for each of the four implemented Configuration operations (reject, error, success variants) — mirror the pattern used for PvMetadata methods.

Add static response helper methods for the two stubs that return error-only.

Override gRPC stub methods:
- `saveConfiguration` → validate then delegate to `handler.handleSaveConfiguration()`
- `getConfiguration` → delegate
- `queryConfigurations` → delegate
- `deleteConfiguration` → delegate
- `patchConfiguration` → call `sendPatchConfigurationResponseError("patchConfiguration() is not yet implemented", responseObserver)` immediately; return
- `bulkSaveConfiguration` → call `sendBulkSaveConfigurationResponseError("bulkSaveConfiguration() is not yet implemented", responseObserver)` immediately; return

---

### Phase 2 Build Verification

```
mvn clean package -DskipTests
```

Expected: clean compile.

---

## Phase 3 — Configuration Activation CRUD + `getActiveConfigurations`

This phase implements all 7 Activation API methods: `saveConfigurationActivation`, `getConfigurationActivation`, `queryConfigurationActivations`, `deleteConfigurationActivation`, `patchConfigurationActivation` (stub), `bulkSaveConfigurationActivation` (stub), `getActiveConfigurations`.

---

### Step 3.1 — MongoSyncAnnotationClient: configurationActivations collection init and indexes

In `init()`, after configurations collection init:
```java
MongoCollection<ConfigurationActivationDocument> actCol =
    database.getCollection(COLLECTION_NAME_CONFIGURATION_ACTIVATIONS, ConfigurationActivationDocument.class);
actCol.createIndex(Indexes.ascending(BSON_KEY_ACTIVATION_CLIENT_ID),
    new IndexOptions().unique(true).sparse(true));
actCol.createIndex(Indexes.compoundIndex(
    Indexes.ascending(BSON_KEY_ACTIVATION_CONFIGURATION_NAME),
    Indexes.ascending(BSON_KEY_ACTIVATION_START_TIME)));
actCol.createIndex(Indexes.ascending(BSON_KEY_ACTIVATION_INTERNAL_CATEGORY));
actCol.createIndex(Indexes.ascending(BSON_KEY_ACTIVATION_START_TIME));
actCol.createIndex(Indexes.ascending(BSON_KEY_ACTIVATION_END_TIME));
actCol.createIndex(Indexes.ascending(BSON_KEY_TAGS));
actCol.createIndex(Indexes.ascending("attributes.name"));
mongoCollectionConfigurationActivations = actCol;
```

Add instance field:
```java
private MongoCollection<ConfigurationActivationDocument> mongoCollectionConfigurationActivations;
```

---

### Step 3.2 — MongoSyncAnnotationClient: overlap detection helper

**Private helper:**
```java
private boolean overlapExists(String configurationName, String category,
                               Instant startTime, Instant endTime,
                               String excludeClientActivationId)
```

Build a MongoDB query that checks for an overlap for **either** the same `configurationName` **or** the same `internalCategory`, excluding the record being updated (if `excludeClientActivationId` is non-null). An overlap condition for a candidate activation [S, E) vs new activation [newS, newE) is:

```
candidate.startTime < newE  (or newE is null/open-ended → always true)
AND (candidate.endTime > newS OR candidate.endTime is absent/null)
```

Two overlap sub-queries (OR'd at the top level, or run separately and check either result):

**Query 1 — same configurationName overlap:**
```
configurationName == configurationName
AND (excludeClientActivationId != null → clientActivationId != excludeId)
AND startTime < newEndTime   (skip upper-bound clause if newEndTime is null)
AND (endTime > newStartTime OR endTime does not exist)
```

**Query 2 — same category overlap:**
```
internalCategory == category
AND (excludeClientActivationId != null → clientActivationId != excludeId)
AND startTime < newEndTime   (skip if newEndTime is null)
AND (endTime > newStartTime OR endTime does not exist)
```

Run both queries; return `true` if either returns at least one document.

> Implementation note: `Filters.or(Filters.exists(BSON_KEY_ACTIVATION_END_TIME, false), Filters.gt(BSON_KEY_ACTIVATION_END_TIME, newStartTime))` handles the open-ended case. For the upper-bound clause when `newEndTime` is null (new activation is open-ended), omit the `startTime < newEndTime` clause entirely (all existing activations starting after `newStartTime` are covered by the lower-bound clause).

---

### Step 3.3 — MongoSyncAnnotationClient: `saveConfigurationActivation()`

```java
public MongoSaveResult saveConfigurationActivation(ConfigurationActivationDocument document)
```

Logic:
1. Look up `Configuration` by `document.getConfigurationName()` — if not found, return error: `"no Configuration found for configurationName: '<name>'"`
2. Set `document.setInternalCategory(config.getCategory())`
3. Determine `excludeId` for the overlap check:
   - If `document.getClientActivationId()` is non-blank: look up existing activation by that ID to determine if this is an update
   - Otherwise: this is always an insert (new server-generated ID)
4. Call `overlapExists(...)`:
   - If overlap found → return `MongoSaveResult(true, "overlapping activation exists for ...", null, false)`
5. If `clientActivationId` is blank → generate a UUID: `document.setClientActivationId(UUID.randomUUID().toString())`
6. Find existing activation by `clientActivationId`:
   - If **new**: `document.addCreationTime()`, `insertOne(document)`, return `MongoSaveResult(false, null, clientActivationId, true)`
   - If **existing**: preserve `createdAt`, `document.addUpdatedTime()`, `replaceOne(filter, document)`, return `MongoSaveResult(false, null, clientActivationId, false)`
7. Wrap in try-catch for `MongoException`

---

### Step 3.4 — MongoSyncAnnotationClient: `findConfigurationActivation()`

```java
public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId)
public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(String configurationName, Instant startTime)
```

Simple `find(eq(...)).first()` queries using the appropriate filter.

---

### Step 3.5 — MongoSyncAnnotationClient: `executeQueryConfigurationActivations()`

```java
public ConfigurationActivationQueryResult executeQueryConfigurationActivations(
    QueryConfigurationActivationsRequest request)
```

Logic (mirror `executeQueryConfigurations`):

1. Build filters from each criterion:
   - `ConfigurationNameCriterion`: exact/prefix/contains regex on `BSON_KEY_ACTIVATION_CONFIGURATION_NAME`
   - `CategoryCriterion`: `Filters.eq(BSON_KEY_ACTIVATION_INTERNAL_CATEGORY, value)`
   - `TagsCriterion`: `Filters.in(BSON_KEY_TAGS, tagsList)`
   - `AttributesCriterion`: key-only or key+values on `attributes.<key>`
   - `TimestampCriterion` (point-in-time): 
     ```
     startTime <= timestamp
     AND (endTime > timestamp OR endTime does not exist)
     ```
     Translates to:
     ```java
     Filters.and(
       Filters.lte(BSON_KEY_ACTIVATION_START_TIME, criterionTimestamp),
       Filters.or(
         Filters.exists(BSON_KEY_ACTIVATION_END_TIME, false),
         Filters.gt(BSON_KEY_ACTIVATION_END_TIME, criterionTimestamp)
       )
     )
     ```
   - `TimeRangeCriterion` (window overlap):
     ```
     startTime < range.endTime
     AND (endTime > range.startTime OR endTime does not exist)
     ```
     Translates to:
     ```java
     Filters.and(
       Filters.lt(BSON_KEY_ACTIVATION_START_TIME, rangeEndTime),
       Filters.or(
         Filters.exists(BSON_KEY_ACTIVATION_END_TIME, false),
         Filters.gt(BSON_KEY_ACTIVATION_END_TIME, rangeStartTime)
       )
     )
     ```
2. Combine all filters with `Filters.and()`
3. Pagination (same skip-based Base64 pattern)
4. Sort by `startTime` ascending

---

### Step 3.6 — MongoSyncAnnotationClient: `deleteConfigurationActivation()`

```java
public MongoDeleteResult deleteConfigurationActivation(String clientActivationId)
public MongoDeleteResult deleteConfigurationActivationByCompositeKey(String configurationName, Instant startTime)
```

Each: delete by appropriate filter; return `MongoDeleteResult` with `clientActivationId` (or synthesized key) as identifier; return not-found indicator if `deletedCount == 0`.

---

### Step 3.7 — MongoSyncAnnotationClient: `getActiveConfigurations()`

```java
public ConfigurationActivationQueryResult getActiveConfigurations(Instant timestamp)
```

Filter:
```java
Filters.and(
  Filters.lte(BSON_KEY_ACTIVATION_START_TIME, timestamp),
  Filters.or(
    Filters.exists(BSON_KEY_ACTIVATION_END_TIME, false),
    Filters.gt(BSON_KEY_ACTIVATION_END_TIME, timestamp)
  )
)
```

No pagination (returns all matching activations). Sort by `startTime` ascending.

---

### Step 3.8 — MongoAnnotationClientInterface: add Activation signatures

Add method signatures for all Activation operations to the interface. Add no-op stubs to `MongoAsyncAnnotationClient`.

---

### Step 3.9 — Dispatchers for Configuration Activation

**Files to create:**

**`SaveConfigurationActivationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → sendSaveConfigurationActivationResponseReject
handleError(String)                  → sendSaveConfigurationActivationResponseError
handleResult(MongoSaveResult)        → success with clientActivationId, or error
```

**`GetConfigurationActivationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → reject
handleError(String)                  → error
handleResult(ConfigurationActivationDocument) → null → reject "no ConfigurationActivation found for: <key>"; else → success
```

**`QueryConfigurationActivationsDispatcher.java`**:
```
handleValidationError(ResultStatus)  → reject
handleError(String)                  → error
handleResult(ConfigurationActivationQueryResult) → build result list with nextPageToken
```

**`DeleteConfigurationActivationDispatcher.java`**:
```
handleValidationError(ResultStatus)  → reject
handleResult(MongoDeleteResult)      → not-found → reject; error → error; else → success
```

**`GetActiveConfigurationsDispatcher.java`**:
```
handleValidationError(ResultStatus)  → reject
handleError(String)                  → error
handleResult(ConfigurationActivationQueryResult) → build result list (empty list is success)
```

---

### Step 3.10 — Jobs for Configuration Activation

**`SaveConfigurationActivationJob.java`**:
- Validate: `configurationName` not blank
- Validate: `startTime` non-null and non-zero (epochSecond != 0 || nanos != 0)
- Validate: if `endTime` is set, it must be after `startTime`
- Validate: no duplicate attribute keys
- Build `ConfigurationActivationDocument.fromSaveConfigurationActivationRequest(request)`
- Call `mongoClient.saveConfigurationActivation(document)`
- Delegate to `dispatcher.handleResult(saveResult)`

**`GetConfigurationActivationJob.java`**:
- Validate: `oneof key` is set → fail if `getKeyCase() == KEY_NOT_SET`
- If `CLIENT_ACTIVATION_ID` case: validate non-blank, call `findConfigurationActivationById()`
- If `COMPOSITE_KEY` case: validate `configurationName` non-blank and `startTime` non-zero, call `findConfigurationActivationByCompositeKey()`
- Delegate to `dispatcher.handleResult(doc)`

**`QueryConfigurationActivationsJob.java`**:
- Validate: criteria list not empty
- Validate each criterion (non-blank values, non-zero timestamps)
- Call `mongoClient.executeQueryConfigurationActivations(request)`
- Delegate to `dispatcher.handleResult(result)`

**`DeleteConfigurationActivationJob.java`**:
- Same `oneof key` validation as Get job
- Call appropriate delete method based on key type
- Delegate to `dispatcher.handleResult(deleteResult)`

**`GetActiveConfigurationsJob.java`**:
- Validate: `timestamp` non-null and non-zero → reject with `"timestamp is required; supply the explicit point in time to query"`
- Convert proto `Timestamp` to `Instant`
- Call `mongoClient.getActiveConfigurations(timestamp)`
- Delegate to `dispatcher.handleResult(result)`

---

### Step 3.11 — AnnotationHandlerInterface and MongoAnnotationHandler: Activation methods

Add to interface and implement in handler:
```java
void handleSaveConfigurationActivation(...)
void handleGetConfigurationActivation(...)
void handleQueryConfigurationActivations(...)
void handleDeleteConfigurationActivation(...)
void handleGetActiveConfigurations(...)
```

---

### Step 3.12 — AnnotationServiceImpl: Activation methods

Add response helper static methods for each Activation operation.

Override gRPC stub methods:
- `saveConfigurationActivation` → delegate to handler
- `getConfigurationActivation` → delegate
- `queryConfigurationActivations` → delegate
- `deleteConfigurationActivation` → delegate
- `patchConfigurationActivation` → immediate error "patchConfigurationActivation() is not yet implemented"
- `bulkSaveConfigurationActivation` → immediate error "bulkSaveConfigurationActivation() is not yet implemented"
- `getActiveConfigurations` → delegate

---

### Phase 3 Build Verification

```
mvn clean package -DskipTests
```

Expected: clean compile; all 13 new gRPC methods wired up.

---

## Phase 4 — Integration Testing

### Step 4.1 — AnnotationTestBase: params records, request builders, response observers

**File to modify:**
`src/test/java/com/ospreydcs/dp/service/annotation/AnnotationTestBase.java`

Add the following, each modeled after the corresponding PvMetadata constructs:

**Params records:**
```java
public record SaveConfigurationParams(
    String configurationName,
    String category,
    String description,
    String parentConfigurationName,
    List<String> tags,
    List<Attribute> attributes,
    String modifiedBy
) {}

public record SaveConfigurationActivationParams(
    String clientActivationId,    // may be null/blank to trigger server generation
    String configurationName,
    Timestamp startTime,
    Timestamp endTime,            // may be null for open-ended
    String description,
    List<String> tags,
    List<Attribute> attributes,
    String modifiedBy
) {}
```

**Request builder methods:**
```java
static SaveConfigurationRequest buildSaveConfigurationRequest(SaveConfigurationParams params)
static GetConfigurationRequest buildGetConfigurationRequest(String configurationName)
static QueryConfigurationsRequest buildQueryConfigurationsRequest(List<QueryConfigurationsCriterion> criteria, int limit, String pageToken)
static DeleteConfigurationRequest buildDeleteConfigurationRequest(String configurationName)

static SaveConfigurationActivationRequest buildSaveConfigurationActivationRequest(SaveConfigurationActivationParams params)
static GetConfigurationActivationRequest buildGetConfigurationActivationByIdRequest(String clientActivationId)
static GetConfigurationActivationRequest buildGetConfigurationActivationByCompositeKeyRequest(String configurationName, Timestamp startTime)
static QueryConfigurationActivationsRequest buildQueryConfigurationActivationsRequest(List<QueryConfigurationActivationsCriterion> criteria, int limit, String pageToken)
static DeleteConfigurationActivationRequest buildDeleteConfigurationActivationByIdRequest(String clientActivationId)
static DeleteConfigurationActivationRequest buildDeleteConfigurationActivationByCompositeKeyRequest(String configurationName, Timestamp startTime)
static GetActiveConfigurationsRequest buildGetActiveConfigurationsRequest(Timestamp timestamp)
```

**Response observer inner classes** (each mirrors `SavePvMetadataResponseObserver`):
```java
SaveConfigurationResponseObserver       // tracks configurationName
GetConfigurationResponseObserver        // tracks Configuration proto
QueryConfigurationsResponseObserver     // tracks List<Configuration> + nextPageToken
DeleteConfigurationResponseObserver     // tracks configurationName

SaveConfigurationActivationResponseObserver   // tracks clientActivationId
GetConfigurationActivationResponseObserver    // tracks ConfigurationActivation proto
QueryConfigurationActivationsResponseObserver // tracks List<ConfigurationActivation> + nextPageToken
DeleteConfigurationActivationResponseObserver // tracks clientActivationId
GetActiveConfigurationsResponseObserver       // tracks List<ConfigurationActivation>

PatchConfigurationResponseObserver            // tracks error state only (stub test)
BulkSaveConfigurationResponseObserver         // tracks error state only (stub test)
PatchConfigurationActivationResponseObserver  // tracks error state only (stub test)
BulkSaveConfigurationActivationResponseObserver // tracks error state only (stub test)
```

---

### Step 4.2 — MongoTestClient: direct DB verification methods

**File to modify:**
`src/test/java/com/ospreydcs/dp/service/common/mongo/MongoTestClient.java`

Add collection fields and init:
```java
private MongoCollection<ConfigurationDocument> mongoCollectionConfigurations;
private MongoCollection<ConfigurationActivationDocument> mongoCollectionConfigurationActivations;
// init in setup alongside pvMetadata collection
```

Add retry-loop lookup methods (mirror `findPvMetadata`):
```java
public ConfigurationDocument findConfiguration(String configurationName)
    // retry loop: find(eq(BSON_KEY_CONFIGURATION_NAME, configurationName)).first()

public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId)
    // retry loop: find(eq(BSON_KEY_ACTIVATION_CLIENT_ID, clientActivationId)).first()

public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(String configurationName, Instant startTime)
    // retry loop: find(and(eq(...configurationName...), eq(...startTime...))).first()
```

---

### Step 4.3 — GrpcIntegrationAnnotationServiceWrapper: sendAndVerify helpers

**File to modify:**
`src/test/java/com/ospreydcs/dp/service/integration/annotation/GrpcIntegrationAnnotationServiceWrapper.java`

Add helper methods (each mirrors the corresponding PvMetadata sendAndVerify method):

```java
String sendAndVerifySaveConfiguration(
    SaveConfigurationParams params,
    boolean expectReject,
    String expectedMessage)

Configuration sendAndVerifyGetConfiguration(
    String configurationName,
    boolean expectReject,
    String expectedMessage)

List<Configuration> sendAndVerifyQueryConfigurations(
    List<QueryConfigurationsCriterion> criteria,
    int limit, String pageToken,
    boolean expectReject,
    String expectedMessage,
    int expectedCount)

String sendAndVerifyDeleteConfiguration(
    String configurationName,
    boolean expectReject,
    String expectedMessage)

String sendAndVerifySaveConfigurationActivation(
    SaveConfigurationActivationParams params,
    boolean expectReject,
    String expectedMessage)

ConfigurationActivation sendAndVerifyGetConfigurationActivationById(
    String clientActivationId,
    boolean expectReject,
    String expectedMessage)

ConfigurationActivation sendAndVerifyGetConfigurationActivationByCompositeKey(
    String configurationName, Timestamp startTime,
    boolean expectReject,
    String expectedMessage)

List<ConfigurationActivation> sendAndVerifyQueryConfigurationActivations(
    List<QueryConfigurationActivationsCriterion> criteria,
    int limit, String pageToken,
    boolean expectReject,
    String expectedMessage,
    int expectedCount)

String sendAndVerifyDeleteConfigurationActivationById(
    String clientActivationId,
    boolean expectReject,
    String expectedMessage)

String sendAndVerifyDeleteConfigurationActivationByCompositeKey(
    String configurationName, Timestamp startTime,
    boolean expectReject,
    String expectedMessage)

List<ConfigurationActivation> sendAndVerifyGetActiveConfigurations(
    Timestamp timestamp,
    boolean expectReject,
    String expectedMessage,
    int expectedCount)
```

---

### Step 4.4 — ConfigurationIT: test class skeleton

**File to create:**
`src/test/java/com/ospreydcs/dp/service/integration/annotation/ConfigurationIT.java`

Extend `AnnotationIntegrationTestIntermediate`. Group test methods by operation. Use JUnit 4 (`@Test`, ordered by dependencies where needed via shared scenario state).

---

### Step 4.5 — ConfigurationIT: `saveConfiguration` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testSaveConfigurationBlankName` | `configurationName = ""` | RESULT_STATUS_REJECT |
| `testSaveConfigurationBlankCategory` | `category = ""` | RESULT_STATUS_REJECT |
| `testSaveConfigurationDuplicateAttributeKeys` | Two attributes with same key | RESULT_STATUS_REJECT |
| `testSaveConfigurationCreateAndUpdate` | Create, verify in DB (`createdTime` set, `updatedTime` null, tags normalized); update, verify `createdTime` preserved, `updatedTime` set | Success both times |
| `testSaveConfigurationCategoryChangeWithActivations` | Create config, create activation, then try to change `category` | RESULT_STATUS_REJECT or error with "existing activations" message |

**Verification for create/update test:**
```java
// After create:
ConfigurationDocument doc = mongoTestClient.findConfiguration(configurationName);
assertNotNull(doc);
assertNotNull(doc.getCreatedAt());
assertNull(doc.getUpdatedAt());
assertEquals(normalizedTags, doc.getTags()); // sorted, lowercased

// After update:
ConfigurationDocument doc2 = mongoTestClient.findConfiguration(configurationName);
assertEquals(doc.getCreatedAt(), doc2.getCreatedAt()); // preserved
assertNotNull(doc2.getUpdatedAt());                     // set
```

---

### Step 4.6 — ConfigurationIT: `getConfiguration` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testGetConfigurationBlankName` | `configurationName = ""` | RESULT_STATUS_REJECT |
| `testGetConfigurationNotFound` | Name that does not exist | RESULT_STATUS_REJECT |
| `testGetConfigurationByName` | Name matching a previously saved config | Success; verify returned `Configuration` fields match saved values |

---

### Step 4.7 — ConfigurationIT: `queryConfigurations` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testQueryConfigurationsEmptyCriteria` | Empty criteria list | RESULT_STATUS_REJECT |
| `testQueryConfigurationsEmptyResult` | Valid criterion matching nothing | Success; empty list |
| `testQueryConfigurationsByNameExact` | `NameCriterion` exact match | Correct record returned |
| `testQueryConfigurationsByNamePrefix` | `NameCriterion` prefix match | All matching records returned |
| `testQueryConfigurationsByNameContains` | `NameCriterion` contains match | All matching records returned |
| `testQueryConfigurationsByCategory` | `CategoryCriterion` | All configs in that category |
| `testQueryConfigurationsByParent` | `ParentCriterion` | All direct children |
| `testQueryConfigurationsByTags` | `TagsCriterion` | Records with matching tags |
| `testQueryConfigurationsByAttributeKeyOnly` | `AttributesCriterion` key-only | Records with that attribute key |
| `testQueryConfigurationsByAttributeKeyAndValues` | `AttributesCriterion` key + values | Records with matching attribute value |
| `testQueryConfigurationsMultiCriterionAnd` | Two criteria (name prefix + category) | AND semantics: only intersection returned |
| `testQueryConfigurationsPagination` | 5 records, limit=2 | Three pages; correct `nextPageToken` on first two, absent on last |

---

### Step 4.8 — ConfigurationIT: `deleteConfiguration` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testDeleteConfigurationBlankName` | `configurationName = ""` | RESULT_STATUS_REJECT |
| `testDeleteConfigurationNotFound` | Name that does not exist | RESULT_STATUS_REJECT |
| `testDeleteConfigurationWithActivations` | Config has an activation | RESULT_STATUS_REJECT with "existing activations" message |
| `testDeleteConfigurationSuccess` | No activations exist | Success; subsequent `getConfiguration` returns not-found |

---

### Step 4.9 — ConfigurationIT: stub tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testPatchConfigurationStub` | Call `patchConfiguration` | RESULT_STATUS_ERROR, message contains "not yet implemented" |
| `testBulkSaveConfigurationStub` | Call `bulkSaveConfiguration` | RESULT_STATUS_ERROR, message contains "not yet implemented" |

---

### Step 4.10 — ConfigurationIT: `saveConfigurationActivation` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testSaveConfigurationActivationBlankConfigName` | `configurationName = ""` | RESULT_STATUS_REJECT |
| `testSaveConfigurationActivationZeroStartTime` | `startTime` = zero | RESULT_STATUS_REJECT |
| `testSaveConfigurationActivationEndTimeBeforeStart` | `endTime` < `startTime` | RESULT_STATUS_REJECT |
| `testSaveConfigurationActivationDuplicateAttributeKeys` | Two attributes with same key | RESULT_STATUS_REJECT |
| `testSaveConfigurationActivationUnknownConfig` | `configurationName` not in DB | RESULT_STATUS_ERROR with "no Configuration found" |
| `testSaveConfigurationActivationWithExplicitId` | `clientActivationId` supplied | Success; returned ID matches supplied ID; verify in DB |
| `testSaveConfigurationActivationServerGeneratedId` | `clientActivationId` blank | Success; non-blank ID returned; verify in DB |
| `testSaveConfigurationActivationOpenEnded` | No `endTime` | Success; `endTime` absent in DB |
| `testSaveConfigurationActivationCloseOpenEnded` | Update existing open-ended activation by setting `endTime` | Success; `endTime` now set in DB; `createdTime` preserved |
| `testSaveConfigurationActivationOverlapSameName` | Two overlapping activations for same `configurationName` | Second save → RESULT_STATUS_ERROR with "overlapping" message |
| `testSaveConfigurationActivationOverlapSameCategory` | Two configs same category, overlapping activations | Second save → RESULT_STATUS_ERROR with "overlapping" message |
| `testSaveConfigurationActivationNoOverlapSameNameAdjacentIntervals` | Back-to-back non-overlapping intervals for same name | Both saves succeed |
| `testSaveConfigurationActivationNoOverlapDifferentCategory` | Two configs different categories, same time window | Both saves succeed |

**Verification for explicit ID test:**
```java
String returnedId = sendAndVerifySaveConfigurationActivation(params, false, null);
assertEquals(params.clientActivationId(), returnedId);
ConfigurationActivationDocument doc = mongoTestClient.findConfigurationActivationById(returnedId);
assertNotNull(doc);
assertNotNull(doc.getCreatedAt());
assertNull(doc.getUpdatedAt());
assertEquals(config.getCategory(), doc.getInternalCategory()); // denormalized
```

---

### Step 4.11 — ConfigurationIT: `getConfigurationActivation` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testGetConfigurationActivationNoKeySet` | Neither key alternative provided | RESULT_STATUS_REJECT |
| `testGetConfigurationActivationCompositeKeyMissingStart` | Composite key with blank `startTime` | RESULT_STATUS_REJECT |
| `testGetConfigurationActivationNotFoundById` | Non-existent `clientActivationId` | RESULT_STATUS_REJECT |
| `testGetConfigurationActivationNotFoundByCompositeKey` | Non-existent `(configurationName, startTime)` | RESULT_STATUS_REJECT |
| `testGetConfigurationActivationById` | Existing record | Success; verify fields |
| `testGetConfigurationActivationByCompositeKey` | Existing record | Success; verify fields match those from get-by-id |

---

### Step 4.12 — ConfigurationIT: `queryConfigurationActivations` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testQueryConfigurationActivationsEmptyCriteria` | Empty criteria | RESULT_STATUS_REJECT |
| `testQueryConfigurationActivationsEmptyResult` | Criterion matching nothing | Success; empty list |
| `testQueryActivationsByConfigurationName` | `ConfigurationNameCriterion` | Correct records |
| `testQueryActivationsByCategory` | `CategoryCriterion` | Records with that category |
| `testQueryActivationsByTags` | `TagsCriterion` | Records with matching tags |
| `testQueryActivationsByAttributeKeyOnly` | `AttributesCriterion` key-only | Correct records |
| `testQueryActivationsByAttributeKeyAndValues` | `AttributesCriterion` key+values | Correct records |
| `testQueryActivationsByTimestamp` | `TimestampCriterion` — timestamp falls within active interval | Only active activation returned |
| `testQueryActivationsByTimestampOpenEnded` | `TimestampCriterion` — open-ended activation; timestamp after `startTime` | Open-ended activation returned |
| `testQueryActivationsByTimestampBeforeStart` | `TimestampCriterion` — timestamp before any activation | Empty result |
| `testQueryActivationsByTimeRange` | `TimeRangeCriterion` — window overlaps two activations | Both returned |
| `testQueryActivationsByTimeRangeNoOverlap` | `TimeRangeCriterion` — window between activations | Empty result |
| `testQueryActivationsPagination` | Multiple records, limit < total | Correct pagination |

---

### Step 4.13 — ConfigurationIT: `deleteConfigurationActivation` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testDeleteConfigurationActivationNoKeySet` | Neither key set | RESULT_STATUS_REJECT |
| `testDeleteConfigurationActivationNotFoundById` | Non-existent ID | RESULT_STATUS_REJECT |
| `testDeleteConfigurationActivationNotFoundByCompositeKey` | Non-existent composite key | RESULT_STATUS_REJECT |
| `testDeleteConfigurationActivationById` | Existing activation | Success; subsequent `get` returns not-found |
| `testDeleteConfigurationActivationByCompositeKey` | Existing activation | Success; subsequent `get` returns not-found |

---

### Step 4.14 — ConfigurationIT: `getActiveConfigurations` tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testGetActiveConfigurationsZeroTimestamp` | Zero timestamp | RESULT_STATUS_REJECT with required message |
| `testGetActiveConfigurationsEmptyResult` | Timestamp before any activation | Success; empty list |
| `testGetActiveConfigurationsAtTimestamp` | Multiple activations; some active, some not | Only currently active activations returned |
| `testGetActiveConfigurationsOpenEnded` | Open-ended activation; timestamp after `startTime` | Open-ended activation included |
| `testGetActiveConfigurationsClosedBeforeTimestamp` | Activation ended before timestamp | Not returned |
| `testGetActiveConfigurationsMultipleCategories` | Two active activations in different categories | Both returned |

---

### Step 4.15 — ConfigurationIT: activation stub tests

| Test method | Scenario | Expected outcome |
|---|---|---|
| `testPatchConfigurationActivationStub` | Call `patchConfigurationActivation` | RESULT_STATUS_ERROR, "not yet implemented" |
| `testBulkSaveConfigurationActivationStub` | Call `bulkSaveConfigurationActivation` | RESULT_STATUS_ERROR, "not yet implemented" |

---

### Step 4.16 — Annotation regression suite

After all new tests pass, run the full annotation regression:
```
mvn test -Dtest=SaveAnnotationIT,SaveDataSetIT,QueryAnnotationsIT,QueryDataSetsIT,ExportDataIT,PvMetadataIT,ConfigurationIT
```

All tests expected to pass.

---

### Phase 4 Final Build Verification

```
mvn clean package
```

All tests pass; clean build.

---

## Implementation Order Summary

| Order | Step | Key Files |
|---|---|---|
| 1 | Result model classes | `ConfigurationQueryResult.java`, `ConfigurationActivationQueryResult.java` |
| 2 | BSON doc: ConfigurationDocument | `common/bson/configuration/ConfigurationDocument.java` |
| 3 | BSON doc: ConfigurationActivationDocument | `common/bson/configuration/ConfigurationActivationDocument.java` |
| 4 | BsonConstants | `BsonConstants.java` |
| 5 | MongoClientBase: constants + codec | `MongoClientBase.java` |
| 6 | MongoSyncAnnotationClient: config collection init + indexes | `MongoSyncAnnotationClient.java` |
| 7 | MongoSyncAnnotationClient: save/find/query/delete Configuration | `MongoSyncAnnotationClient.java` |
| 8 | MongoAnnotationClientInterface + MongoAsyncAnnotationClient stubs | both files |
| 9 | Configuration Dispatchers (×4) | `dispatch/` package |
| 10 | Configuration Jobs (×4) | `job/` package |
| 11 | AnnotationHandlerInterface + MongoAnnotationHandler: Configuration | both files |
| 12 | AnnotationServiceImpl: Configuration methods + stubs | `AnnotationServiceImpl.java` |
| 13 | **Build verify** (`-DskipTests`) | |
| 14 | MongoSyncAnnotationClient: activation collection init + indexes | `MongoSyncAnnotationClient.java` |
| 15 | MongoSyncAnnotationClient: overlap helper | `MongoSyncAnnotationClient.java` |
| 16 | MongoSyncAnnotationClient: save/find/query/delete Activation + getActive | `MongoSyncAnnotationClient.java` |
| 17 | MongoAnnotationClientInterface + stubs: Activation methods | both files |
| 18 | Activation Dispatchers (×5) | `dispatch/` package |
| 19 | Activation Jobs (×5) | `job/` package |
| 20 | AnnotationHandlerInterface + MongoAnnotationHandler: Activation | both files |
| 21 | AnnotationServiceImpl: Activation methods + stubs | `AnnotationServiceImpl.java` |
| 22 | **Build verify** (`-DskipTests`) | |
| 23 | AnnotationTestBase: params, builders, observers | `AnnotationTestBase.java` |
| 24 | MongoTestClient: findConfiguration, findConfigurationActivation* | `MongoTestClient.java` |
| 25 | GrpcIntegrationAnnotationServiceWrapper: sendAndVerify* | wrapper file |
| 26 | ConfigurationIT: saveConfiguration tests | `ConfigurationIT.java` |
| 27 | ConfigurationIT: getConfiguration tests | `ConfigurationIT.java` |
| 28 | ConfigurationIT: queryConfigurations tests | `ConfigurationIT.java` |
| 29 | ConfigurationIT: deleteConfiguration tests | `ConfigurationIT.java` |
| 30 | ConfigurationIT: configuration stub tests | `ConfigurationIT.java` |
| 31 | ConfigurationIT: saveConfigurationActivation tests | `ConfigurationIT.java` |
| 32 | ConfigurationIT: getConfigurationActivation tests | `ConfigurationIT.java` |
| 33 | ConfigurationIT: queryConfigurationActivations tests | `ConfigurationIT.java` |
| 34 | ConfigurationIT: deleteConfigurationActivation tests | `ConfigurationIT.java` |
| 35 | ConfigurationIT: getActiveConfigurations tests | `ConfigurationIT.java` |
| 36 | ConfigurationIT: activation stub tests | `ConfigurationIT.java` |
| 37 | Annotation regression suite | all IT classes |
| 38 | **Final build** (`mvn clean package`) | |
