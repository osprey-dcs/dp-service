# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run specific service: 
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

## Architecture Overview
This is a Data Platform service implementation with three main services:
- **Ingestion Service**: Handles data ingestion with high-performance streaming APIs and comprehensive validation
- **Query Service**: Provides time-series data retrieval and metadata queries 
- **Annotation Service**: Manages data annotations, datasets, and data exports

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

## MongoDB Collections
- **buckets**: Time-series data storage (main data collection with embedded protobuf serialization)
- **providers**: Registered data providers
- **requestStatus**: Ingestion request tracking
- **dataSets**: Annotation dataset definitions (contains DataBlockDocuments for time ranges and PV names)
- **annotations**: Data annotations (references dataSets and optionally calculations)
- **calculations**: Associated calculation results (embedded CalculationsDataFrameDocuments)
- **pvMetadata**: PV metadata records managed by the Annotation Service (pvName, aliases, tags, attributes, description, modifiedBy, createdAt, updatedAt)

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

### Column Document Class Hierarchy
The ingestion service uses a sophisticated class hierarchy for MongoDB column document storage:

**Base Classes:**
- **`ColumnDocumentBase`**: Abstract base with `name` and `columnMetadata` fields and methods for protobuf/MongoDB conversion
- **`ScalarColumnDocumentBase<T>`**: Generic intermediate class for scalar column types

**Scalar Column Implementation Pattern:**
```java
@BsonDiscriminator(key = "_t", value = "columnType")
public class TypeColumnDocument extends ScalarColumnDocumentBase<JavaType> {
    
    // Static factory method — always extract metadata if present
    public static TypeColumnDocument fromTypeColumn(TypeColumn requestColumn) {
        TypeColumnDocument document = new TypeColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        if (requestColumn.hasMetadata()) {
            document.setColumnMetadata(ColumnMetadataDocument.fromColumnMetadata(requestColumn.getMetadata()));
        }
        return document;
    }
    
    // Protobuf builder creation
    @Override
    protected Message.Builder createColumnBuilder() {
        return TypeColumn.newBuilder();
    }
    
    // Add values to protobuf builder
    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((TypeColumn.Builder) builder).addAllValues(this.getValues());
    }
    
    // Convert scalar to DataValue for legacy compatibility
    @Override
    protected DataValue createDataValueFromScalar(JavaType value) {
        return DataValue.newBuilder().setTypeValue(value).build();
    }
    
    // Add column to DataBucket for ingestion response
    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        TypeColumn column = (TypeColumn) toProtobufColumn();
        bucketBuilder.setTypeColumn(column);
    }
}
```

**Column Type Implementation Status:**

**Scalar Columns:**
| Proto Message | Java Generic Type | Document Class | BSON Discriminator | Status |
|--------------|------------------|----------------|-------------------|--------|
| DoubleColumn | `ScalarColumnDocumentBase<Double>` | DoubleColumnDocument | "doubleColumn" | ✅ |
| FloatColumn | `ScalarColumnDocumentBase<Float>` | FloatColumnDocument | "floatColumn" | ✅ |
| Int64Column | `ScalarColumnDocumentBase<Long>` | Int64ColumnDocument | "int64Column" | ✅ |
| Int32Column | `ScalarColumnDocumentBase<Integer>` | Int32ColumnDocument | "int32Column" | ✅ |
| BoolColumn | `ScalarColumnDocumentBase<Boolean>` | BoolColumnDocument | "boolColumn" | ✅ |
| StringColumn | `ScalarColumnDocumentBase<String>` | StringColumnDocument | "stringColumn" | ✅ |
| EnumColumn | `ScalarColumnDocumentBase<Integer>` + `enumId` | EnumColumnDocument | "enumColumn" | ✅ |

**Array Columns:**
| Proto Message | Base Class | Document Class | BSON Discriminator | Status |
|--------------|-------------|----------------|-------------------|--------|
| DoubleArrayColumn | `ArrayColumnDocumentBase` | DoubleArrayColumnDocument | "doubleArrayColumn" | ✅ |
| FloatArrayColumn | `ArrayColumnDocumentBase` | FloatArrayColumnDocument | "floatArrayColumn" | ✅ |
| Int32ArrayColumn | `ArrayColumnDocumentBase` | Int32ArrayColumnDocument | "int32ArrayColumn" | ✅ |
| Int64ArrayColumn | `ArrayColumnDocumentBase` | Int64ArrayColumnDocument | "int64ArrayColumn" | ✅ |
| BoolArrayColumn | `ArrayColumnDocumentBase` | BoolArrayColumnDocument | "boolArrayColumn" | ✅ |

**Binary Columns:**
| Proto Message | Base Class | Document Class | BSON Discriminator | Special Features | Status |
|--------------|-------------|----------------|-------------------|------------------|--------|
| StructColumn | `BinaryColumnDocumentBase` | StructColumnDocument | "structColumn" | schemaId field | ✅ |
| ImageColumn | `BinaryColumnDocumentBase` | ImageColumnDocument | "imageColumn" | ImageDescriptor helper | ✅ |
| SerializedDataColumn | `BinaryColumnDocumentBase` | SerializedDataColumnDocument | "serializedDataColumn" | encoding field | ✅ |

**Legacy Columns:**
| Proto Message | Current Implementation | Migration Status | Status |
|--------------|----------------------|------------------|--------|
| DataColumn | Legacy pattern | Maintain for backward compatibility | ✅ Legacy |

### SerializedDataColumn Migration (Completed)

**SerializedDataColumn** has been successfully migrated from the legacy storage pattern to the modern BinaryColumnDocumentBase hierarchy for architectural consistency with other binary column types.

**Migration Details:**
- **From**: Legacy `DataColumnDocument.fromSerializedDataColumn()` approach
- **To**: Modern `SerializedDataColumnDocument extends BinaryColumnDocumentBase`
- **Benefits**: Consistent binary storage, GridFS readiness, eliminates architectural inconsistency
- **Backward Compatibility**: Existing legacy tests removed; new integration test `SerializedDataColumnIT` follows StructColumn pattern
- **Integration Test**: Full API coverage (ingestion, query, data subscription, data event subscription)

**EnumColumn Hybrid Design:**
EnumColumn uses a hybrid approach that extends the scalar pattern with additional semantic metadata:
- **Base Pattern**: Extends `ScalarColumnDocumentBase<Integer>` for `List<Integer> values` handling
- **Additional Field**: Adds `String enumId` field for enum semantic context (e.g., "epics:alarm_status:v1")
- **Custom Conversion**: Overrides `toProtobufColumn()` to include `enumId` field in protobuf output
- **Integer Triggers**: Uses integer comparison for data event triggers since Integer implements Comparable
- **Semantic Preservation**: Maintains enum meaning while leveraging scalar column efficiency

**Benefits of Generic Base Class:**
- **Code Reuse**: `List<T> values` field and common methods inherited from base
- **Type Safety**: Compile-time type checking with generic parameter `<T>`
- **Consistent API**: All scalar columns follow same conversion patterns
- **Memory Efficiency**: Maintains column-oriented storage for high-frequency ingestion
- **Legacy Compatibility**: Generic `toDataColumn()` converts to sample-oriented DataColumn

**Inherited Methods from ScalarColumnDocumentBase:**
- `getValues()` / `setValues()` - Generic value list accessors
- `toDataColumn()` - Converts to legacy DataColumn with DataValue objects
- `getBytes()` - Serializes protobuf column to byte array
- `toProtobufColumn()` - Template method for creating typed protobuf column
- `addColumnToBucket()` - Abstract method implementation for query result API integration

## Systematic Process for Adding New Protobuf Column Types

The ingestion service uses a proven **7-step systematic process** for adding complete support for new protobuf column types. This process has been successfully applied to scalar columns (DoubleColumn, FloatColumn, etc.), array columns (DoubleArrayColumn, FloatArrayColumn, etc.), and binary columns (StructColumn, ImageColumn).

### Column Type Categories and Base Classes

**Scalar Columns**: Simple value types that extend `ScalarColumnDocumentBase<T>`
- DoubleColumn → DoubleColumnDocument extends `ScalarColumnDocumentBase<Double>`
- FloatColumn → FloatColumnDocument extends `ScalarColumnDocumentBase<Float>`
- Int64Column → Int64ColumnDocument extends `ScalarColumnDocumentBase<Long>`
- Int32Column → Int32ColumnDocument extends `ScalarColumnDocumentBase<Integer>`
- BoolColumn → BoolColumnDocument extends `ScalarColumnDocumentBase<Boolean>`
- StringColumn → StringColumnDocument extends `ScalarColumnDocumentBase<String>`
- EnumColumn → EnumColumnDocument extends `ScalarColumnDocumentBase<Integer>`

**Array Columns**: Multi-dimensional arrays that extend `ArrayColumnDocumentBase`
- DoubleArrayColumn → DoubleArrayColumnDocument extends `ArrayColumnDocumentBase`
- FloatArrayColumn → FloatArrayColumnDocument extends `ArrayColumnDocumentBase`
- Int32ArrayColumn → Int32ArrayColumnDocument extends `ArrayColumnDocumentBase`
- Int64ArrayColumn → Int64ArrayColumnDocument extends `ArrayColumnDocumentBase`
- BoolArrayColumn → BoolArrayColumnDocument extends `ArrayColumnDocumentBase`

**Binary Columns**: Variable-length binary data that extend `BinaryColumnDocumentBase`
- StructColumn → StructColumnDocument extends `BinaryColumnDocumentBase` (with schemaId)
- ImageColumn → ImageColumnDocument extends `BinaryColumnDocumentBase` (with ImageDescriptor)

### 7-Step Implementation Process

**Step 1: Create Document Class**
- Choose appropriate base class (Scalar/Array/BinaryColumnDocumentBase)
- Add `@BsonDiscriminator(key = "_t", value = "columnType")` annotation
- Implement required abstract methods from base class
- Add static factory method `fromProtobufColumn(ProtobufColumn column)`
- For binary columns: implement `toProtobufColumn()` and `deserializeToProtobufColumn()`

**Step 2: Update BucketDocument Generation**
- Add new column type handling in `BucketDocument.generateBucketsFromRequest()`
- Follow pattern: `for (NewColumn column : request.getNewColumnsList()) { ... }`
- Create document using factory method: `ColumnDocumentBase columnDocument = NewColumnDocument.fromNewColumn(column)`

**Step 3: Register POJO Class in MongoDB Codec**
- Add document class to `MongoClientBase.getPojoCodecRegistry()`
- For embedded helper classes (e.g., `ImageDescriptorDocument`), register separately before the parent class
- **Warning**: MongoDB POJO codec silently skips any field missing a getter or setter — `insertMany` succeeds but the field is not written to the database. Every instance variable on every registered BSON class must have both getter and setter.

**Step 4: Add Data Subscription Support**
- Update `SourceMonitorManager.publishDataSubscriptions()`
- Add new column type case in switch statement for DataBucket creation
- Pattern: `case NEWCOLUMN -> bucketBuilder.setNewColumn((NewColumn) columnDocument.toProtobufColumn())`

**Step 5: Add Event Subscription Framework Support**
- Update `ColumnTriggerUtility` for trigger support (scalar columns only)
- Update `DataBuffer` for size estimation and storage
- Binary and array columns serve as targets only, not triggers

**Step 6: Update Test Framework Support**
- Add `List<NewColumn> newColumnList` field to `IngestionTestBase.IngestionRequestParams`
- Add getter/setter methods: `newColumnList()` and `setNewColumnList()`
- Update `buildIngestionRequest()` to include new column data: `dataFrameBuilder.addAllNewColumns(params.newColumnList())`
- Add verification logic to `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`

**Step 7: Create Integration Test Coverage**
- Create `NewColumnIT` class extending `GrpcIntegrationTestBase`
- **Scalar Columns**: Follow `DoubleColumnIT` pattern with single PV approach
- **Array/Binary Columns**: Follow dual PV approach (scalar trigger + array/binary target)
- Test coverage: ingestion → query → data subscription → event subscription
- Verify round-trip data integrity using `assertEquals(originalColumn, retrievedColumn)`

### Class Hierarchy Design Issues

**Known Technical Debt**: The methods `createColumnBuilder()` and `addAllValuesToBuilder()` are defined at `ColumnDocumentBase` level but only apply to scalar columns. Array and binary columns must implement these with `UnsupportedOperationException`. Future refactoring should move these methods to `ScalarColumnDocumentBase`.

### Integration Test Patterns

**Single PV Approach (Scalar Columns)**:
- Use the column type directly as both ingestion data and trigger/target
- Simpler test structure with direct column verification

**Dual PV Approach (Array/Binary Columns)**:
- Scalar PV serves as trigger for event subscriptions
- Array/binary PV serves as target for event subscriptions
- Required because array/binary columns cannot function as trigger PVs
- Both PVs included in initial ingestion for proper validation

**Test Verification Pattern**:
```java
// Standard verification for all column types
DataColumnDocument dataColumnDocument = bucketDocument.getDataColumnDocument();
NewColumn storedColumn = (NewColumn) dataColumnDocument.toProtobufColumn();
assertTrue(request.getIngestionDataFrame().getNewColumnsList().contains(storedColumn));
assertEquals(originalColumn, retrievedColumn); // Round-trip integrity
```

### Benefits of Systematic Approach

- **Consistency**: All column types follow identical implementation patterns
- **Completeness**: Covers full pipeline from ingestion through query and subscriptions
- **Maintainability**: Centralized patterns make debugging and enhancements easier  
- **Extensibility**: Adding new column types requires minimal framework changes
- **Quality**: Comprehensive test coverage catches integration issues early
- **Documentation**: Self-documenting code patterns reduce learning curve

### Binary Column Document Class Hierarchy

The ingestion service uses a two-level hierarchy for binary data columns that require specialized storage handling:

**Base Classes:**
- **`BinaryColumnDocumentBase`**: Abstract base for columns storing binary data (arrays, images, structs)
- **`ArrayColumnDocumentBase`**: Specialized base for array column types with dimensional metadata

**Storage Abstraction:**
```java
public class StorageDocument {
    public enum StorageKind { INLINE, GRIDFS }
    private StorageKind kind;
    private byte[] data;        // Used when kind = INLINE
    private ObjectId fileId;    // Used when kind = GRIDFS
    private Long sizeBytes;     // Size tracking for both storage types
}
```

**Array Column Implementation Pattern:**
```java
@BsonDiscriminator(key = "_t", value = "arrayType")
public class TypeArrayColumnDocument extends ArrayColumnDocumentBase {
    
    @Override
    protected int getElementSizeBytes() {
        return ELEMENT_SIZE; // e.g., 8 for double, 4 for float
    }
    
    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) {
        // Type-specific binary serialization with little-endian order
    }
    
    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        // Type-specific binary deserialization to protobuf column
    }
}
```

**Array Column Type Mappings:**
| Proto Message | Element Size | Document Class | BSON Discriminator | Status |
|--------------|--------------|----------------|-------------------|--------|
| DoubleArrayColumn | 8 bytes | DoubleArrayColumnDocument | "doubleArrayColumn" | ✅ |
| FloatArrayColumn | 4 bytes | FloatArrayColumnDocument | "floatArrayColumn" | |
| Int64ArrayColumn | 8 bytes | Int64ArrayColumnDocument | "int64ArrayColumn" | |
| Int32ArrayColumn | 4 bytes | Int32ArrayColumnDocument | "int32ArrayColumn" | |
| BoolArrayColumn | 1 byte | BoolArrayColumnDocument | "boolArrayColumn" | |

**Binary Storage Features:**
- **Little-Endian Serialization**: Optimized ByteBuffer serialization with `ByteOrder.LITTLE_ENDIAN`
- **Row-Major Flattening**: Multi-dimensional arrays stored as `sample_count × product(dimensions)` elements
- **Storage Flexibility**: Inline storage for small data, GridFS ready for large arrays (>16MB)
- **Memory Efficiency**: Binary storage avoids per-element BSON overhead for array data
- **No Trigger Support**: Array columns can only be targets in data event subscriptions, not triggers

**Benefits of Two-Level Hierarchy:**
- **Code Reuse**: BinaryColumnDocumentBase shared by array, image, struct, and serialized columns
- **Type Safety**: ArrayColumnDocumentBase provides array-specific validation and serialization
- **Extensibility**: Easy to add new binary column types (ImageColumn, StructColumn)
- **Performance**: Binary serialization optimized for high-frequency array ingestion scenarios

### Column-Level Metadata

All 16 column proto types carry an optional `metadata = 10` field (`ColumnMetadata`) added in dp-grpc issue #116. The ingestion service persists this metadata alongside column data in MongoDB.

**Proto Messages:**
```protobuf
message ColumnProvenance { string source = 1; string process = 2; }
message ColumnMetadata {
  ColumnProvenance provenance = 1;
  repeated string tags = 2;
  repeated Attribute attributes = 3;
}
```

**BSON Document Classes** (`common/bson/`):
- `ColumnProvenanceDocument` — fields: `source`, `process`; factory `fromColumnProvenance()`, conversion `toColumnProvenance()`
- `ColumnMetadataDocument` — fields: `provenance`, `tags`, `Map<String,String> attributes`; factory `fromColumnMetadata()`, conversion `toColumnMetadata()`
- Attributes stored as `Map<String, String>` using `AttributesUtility.attributeMapFromList()` / `attributeListFromMap()`
- Both classes **must** be registered in `MongoClientBase.getPojoCodecRegistry()` before `ColumnDocumentBase.class`

**Storage Location:**
Metadata is stored as `columnMetadata` on `ColumnDocumentBase` (not on `BucketDocument`). Since there is one `BucketDocument` per column, metadata is semantically owned by the column document. This also enables future MongoDB queries via dot-notation paths defined in `BsonConstants`:
```java
BSON_KEY_BUCKET_COLUMN_METADATA             = "dataColumn.columnMetadata"
BSON_KEY_BUCKET_COLUMN_METADATA_PROVENANCE  = "dataColumn.columnMetadata.provenance"
BSON_KEY_BUCKET_COLUMN_METADATA_TAGS        = "dataColumn.columnMetadata.tags"
BSON_KEY_BUCKET_COLUMN_METADATA_ATTRIBUTES  = "dataColumn.columnMetadata.attributes"
```

**Factory Method Pattern:**
Every `fromXxxColumn()` factory must check `hasMetadata()` and call `setColumnMetadata()`:
```java
if (requestColumn.hasMetadata()) {
    document.setColumnMetadata(ColumnMetadataDocument.fromColumnMetadata(requestColumn.getMetadata()));
}
```
This is present in all 16 column document factory methods. Omitting it silently drops metadata.

**Round-Trip Restoration via `applyMetadataToProto()`:**
`toProtobufColumn()` reconstructs the proto column from stored fields and must re-attach metadata. `ColumnDocumentBase.applyMetadataToProto(Message)` handles this via reflection (`setMetadata` on the proto builder). It is called automatically by both `ScalarColumnDocumentBase.toProtobufColumn()` and `BinaryColumnDocumentBase.toProtobufColumn()` — no action needed in individual column document classes. Without this, the stored proto round-trip would not equal the original request column (breaking `verifyIngestionRequestHandling` equality checks).

**Validation Limits** (enforced in `IngestionValidationUtility.validateAllColumnMetadata()`):
- `provenance.source` / `provenance.process`: ≤ 256 characters
- `tags`: ≤ 20 entries, each ≤ 256 characters
- `attributes`: ≤ 20 entries, each key and value ≤ 256 characters

**MongoDB POJO Codec Silent Failure Warning:**
The MongoDB POJO codec silently skips fields that lack getter/setter methods — `insertMany` succeeds but the field is not written. Any new BSON document class added to the hierarchy (including helper classes like `ColumnProvenanceDocument`) **must** have both a getter and setter for every instance variable, or the field will be silently dropped in MongoDB.

## Export Framework Architecture
The Annotation Service includes a sophisticated export framework with format-specific support for different column types:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → format-specific jobs
- **Format Jobs**: `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **File Interfaces**: `TabularDataExportFileInterface` implemented by `DataExportXlsxFile`, etc.
- **Data Processing**: `TimestampDataMap` for tabular data assembly, `TabularDataUtility` for data manipulation
- **Excel Implementation**: Uses Apache POI with `XSSFWorkbook` for reliable XLSX generation

### Export Format Compatibility by Column Type
- **Scalar Columns** (DoubleColumn, StringColumn, etc.): Support all export formats (CSV, Excel, HDF5)
  - **Tabular Formats**: CSV and Excel export via `toDataColumn()` conversion to sample-oriented format
  - **HDF5**: Native support for efficient columnar storage of scalar time-series data
- **Binary Array Columns** (DoubleArrayColumn, etc.): **HDF5 export only**
  - **HDF5**: Optimal format for multi-dimensional array data with native array storage
  - **Tabular Limitation**: Cannot export to CSV/Excel as binary columns cannot convert to legacy DataColumn format
  - **Future Enhancement**: Array columns require specialized tabular export implementation for flattened element columns

### Excel File Generation
The `DataExportXlsxFile` class uses `XSSFWorkbook` (non-streaming) for better reliability:
- Suitable for small to medium datasets (up to ~50K-100K rows)
- For very large files, consider switching to properly configured `SXSSFWorkbook`
- Uses proper resource management with workbook.close() in finally blocks

### Data Import Framework
The client utilities include Excel data import capabilities:
- `DataImportUtility` provides static methods for importing time-series data
- Uses Apache POI `XSSFWorkbook` for consistent Excel handling across import/export
- Supports automatic type detection (numeric → double, string → string, boolean → boolean)
- Formula evaluation supported for calculated Excel cells

## Annotation Service CRUD API Pattern

This section documents the standard pattern for implementing new CRUD APIs on the Annotation Service, established by the `pvMetadata` API (dp-service #178). Follow this pattern for all new annotation service APIs and enhancements to existing ones.

### Full Implementation Pipeline

A new annotation service API method follows this end-to-end pipeline:

```
AnnotationServiceImpl (gRPC stub override)
  → validates request fields
  → calls AnnotationHandlerInterface method
  → MongoAnnotationHandler (enqueues job)
  → XxxJob.execute() (validates, queries/mutates MongoDB, dispatches result)
  → MongoAnnotationClientInterface / MongoSyncAnnotationClient (MongoDB operations)
  → XxxDispatcher (sends gRPC response to StreamObserver)
```

**Stub methods** (not yet implemented) skip the queue entirely: `AnnotationServiceImpl` calls the dispatcher directly and returns `RESULT_STATUS_ERROR` "not yet implemented" without queuing a job.

### Step-by-Step: Adding a New API Method

**Step 1 — BSON Document Class** (`common/bson/<entity>/XxxDocument.java`)
- Extend `DpBsonDocumentBase` if the entity has tags, attributes, createdAt, or updatedAt (inherited automatically)
- Add entity-specific fields with getters and setters (**every** field must have both — the MongoDB POJO codec silently drops fields missing either)
- Add a static factory method `fromSaveXxxRequest(SaveXxxRequest)` to populate from a proto request
- Add a `toXxx()` method to convert back to the proto message
- Register the class in `MongoClientBase.getPojoCodecRegistry()`; register embedded helper classes before their parent

**Step 2 — MongoDB Collection** (`common/mongo/MongoClientBase.java` and `common/bson/BsonConstants.java`)
- Add `public static final String COLLECTION_NAME_XXX = "xxxCollection";` to `MongoClientBase`
- Add field-name constants to `BsonConstants` for each queryable field
- In `MongoSyncAnnotationClient.init()`, call `createIndex()` for any unique or query-critical fields — this runs at startup and is idempotent

**Step 3 — MongoClient interface and implementation**
- Add method signatures to `MongoAnnotationClientInterface`
- Implement in `MongoSyncAnnotationClient`
- Add no-op stubs to `MongoAsyncAnnotationClient` (required since it implements the interface)

**Step 4 — Dispatcher** (`annotation/handler/mongo/dispatch/XxxDispatcher.java`)
- Extend `Dispatcher` and hold a `StreamObserver<XxxResponse>`
- Add `handleValidationError(ResultStatus)`, `handleError(String)`, and `handleResult(...)` methods
- For not-found cases on get/delete, use `RESULT_STATUS_REJECT` (not error) — call `AnnotationServiceImpl.sendXxxResponseReject()`

**Step 5 — Job** (`annotation/handler/mongo/job/XxxJob.java`)
- Extend `HandlerJob`; hold references to request, responseObserver, mongoClient, and dispatcher
- `execute()` performs validation first (fail-fast), then calls mongoClient, then dispatches
- Validation lives directly in `execute()` — no separate utility class for annotation service jobs

**Step 6 — Handler** (`annotation/handler/mongo/MongoAnnotationHandler.java`)
- Add the new method to `AnnotationHandlerInterface`
- Implement in `MongoAnnotationHandler` following the `handleSaveDataSet` queue pattern: create a job and call `executeJob(job)`

**Step 7 — Service Implementation** (`annotation/service/AnnotationServiceImpl.java`)
- Add static response helper methods: `sendXxxResponseReject(...)`, `sendXxxResponseError(...)`, `sendXxxResponseSuccess(...)`
- Override the gRPC stub method; validate, then delegate to the handler
- For stubs: immediately call `sendXxxResponseError(...)` "not yet implemented" and return — no handler call

### BSON Document Base Class: `DpBsonDocumentBase`

Documents that need tags, attributes, or managed timestamps should extend `DpBsonDocumentBase`:

```java
public class XxxDocument extends DpBsonDocumentBase {
    // Inherited: List<String> tags, Map<String,String> attributes, Instant createdAt, Instant updatedAt
    private String specificField;
    // ... getters and setters for all fields
}
```

- **Attributes** are stored as `Map<String, String>` in MongoDB; use `AttributesUtility.attributeMapFromList()` (proto `List<Attribute>` → map) and `AttributesUtility.attributeListFromMap()` (map → proto `List<Attribute>`) to convert
- **Timestamps** (createdAt/updatedAt) are `Instant`; use `TimestampUtility.getTimestampFromInstant()` to convert to proto `Timestamp` when building the response proto

### Standard Conventions

**Tag normalization:** Tags are normalized on save — lowercase, deduplicated, sorted:
```java
List<String> normalizedTags = new ArrayList<>(
    new TreeSet<>(request.getTagsList().stream().map(String::toLowerCase).toList()));
```

**Upsert with `createdAt` preservation:** On first save, set `createdAt = Instant.now()` and leave `updatedAt` null. On update (upsert of existing record), preserve the original `createdAt` and set `updatedAt = Instant.now()`. Use `findOneAndReplace` with `upsert=true`, or a find-then-insert/replace approach.

**Not-found → RESULT_STATUS_REJECT:** When a get or delete lookup finds no matching record, return `RESULT_STATUS_REJECT` (not `RESULT_STATUS_ERROR`). Error message: `"no XxxRecord found for: {identifier}"`.

**Stub methods → immediate RESULT_STATUS_ERROR:** API methods not yet implemented respond immediately in `AnnotationServiceImpl` with `RESULT_STATUS_ERROR` and message `"not yet implemented"`. They do not enqueue a job.

**Validation in Job.execute():** Request validation lives directly in `execute()`. Call `dispatcher.handleValidationError(new ResultStatus(true, "message"))` and return early for each violation. No separate validation utility class.

**Result wrapper classes:** Use these standard wrappers for MongoDB operation results:
- `MongoSaveResult` — holds document identifier (e.g., pvName) and error state
- `MongoDeleteResult` — holds deleted document identifier and error state
- `PvMetadataQueryResult` — holds `List<PvMetadataDocument>` and `String nextPageToken` for paginated queries

### Pagination Pattern

Paginated queries use a base64-encoded integer skip offset as the page token:
```java
// Decode incoming token (null/empty → skip 0)
int skipOffset = (pageToken == null || pageToken.isEmpty()) ? 0
    : Integer.parseInt(new String(Base64.getDecoder().decode(pageToken)));
// Execute: collection.find(filter).sort(...).skip(skipOffset).limit(limit)
// Compute next token
String nextPageToken = (skipOffset + results.size() < totalCount)
    ? Base64.getEncoder().encodeToString(String.valueOf(skipOffset + results.size()).getBytes())
    : null;
```

### Query Criteria → MongoDB Filter Pattern

Build a compound `Filters.and()` from a list of criteria; each criterion maps to one or more `Bson` filters:

| Criterion type | MongoDB filter |
|---|---|
| Exact match | `Filters.eq(field, value)` |
| Prefix match | `Filters.regex(field, "^prefix")` |
| Contains match | `Filters.regex(field, ".*substring.*")` |
| Tags `$in` | `Filters.in(BSON_KEY_TAGS, values)` |
| Attribute key-only | `Filters.exists("attributes." + key)` |
| Attribute key+values | `Filters.in("attributes." + key, values)` |

Multiple match types within one criterion are combined with `Filters.or()`.

### Adding a New MongoDB Collection

When a new API requires a dedicated collection:

1. Add `public static final String COLLECTION_NAME_XXX = "xxx";` to `MongoClientBase`
2. Add field-name constants to `BsonConstants` (prefix: `BSON_KEY_XXX_...`)
3. In `MongoSyncAnnotationClient.init()`, get the collection and call `createIndex()` for key fields:
   ```java
   MongoCollection<XxxDocument> col = database.getCollection(COLLECTION_NAME_XXX, XxxDocument.class);
   col.createIndex(Indexes.ascending(BSON_KEY_XXX_NATURAL_KEY),
       new IndexOptions().unique(true));
   col.createIndex(Indexes.ascending(BSON_KEY_XXX_SECONDARY_FIELD));
   ```
4. Store the collection reference as an instance field on `MongoSyncAnnotationClient`
5. Register `XxxDocument.class` in `MongoClientBase.getPojoCodecRegistry()`

`createIndex()` is idempotent — safe to call on every startup.

## Code Style Guidelines
- Java 21 is used for this project
- MongoDB is used for persistence with embedded protobuf serialization
- Package structure: `com.ospreydcs.dp.service.<component>`
- Follow existing naming conventions (CamelCase for classes, lowerCamelCase for methods)
- API method implementations follow: Handler → Job → Database Client → Dispatcher pattern
- Jobs named as `<APIMethod>Job`, Dispatchers as `<APIMethod>Dispatcher`
- Error handling uses DpException and structured logging
- Integration tests located in `integration.<service>` packages
- Follow existing patterns for protobuf ↔ MongoDB document conversion
- Result objects use `ResultStatus` class with `isError` (Boolean) and `msg` (String) fields

## API Method Naming Conventions
Recent API evolution has moved from "create" to "save" semantics:
- `saveDataSet()` performs upsert operations (create or update)
- `saveAnnotation()` performs upsert operations (create or update)
- `savePvMetadata()` performs upsert operations (create or update)
- Request/Response/Result types follow `Save*Request`, `Save*Response`, `Save*Result` patterns
- Legacy "create" references should be updated to "save" when encountered

**CRUD method naming for new annotation APIs:**
- `saveXxx` — upsert (create or update) by natural key
- `queryXxx` — search with filter criteria; returns a list, paginated
- `getXxx` — single-record lookup by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `deleteXxx` — remove by natural key or alias; returns `RESULT_STATUS_REJECT` if not found
- `patchXxx` / `bulkSaveXxx` — reserved for partial update and bulk operations; implement as stubs returning `RESULT_STATUS_ERROR` "not yet implemented" until ready

## Client API Utilities
- **Data Import**: `DataImportUtility.importXlsxData()` for importing time-series data from Excel files
  - Located in `com.ospreydcs.dp.client.utility.DataImportUtility`
  - Returns `DataImportResult` with timestamps and DataColumn objects
  - Uses Apache POI for Excel file processing
  - Expects format: `[seconds, nanos, pv_data_columns...]` with header row

## Ingestion Validation Framework
The ingestion service implements comprehensive validation for all column-oriented data structures to support high-frequency data ingestion (4000 PVs at 1 KHz) with proper memory management and data integrity.

### Validation Architecture
- **Location**: `com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility`
- **Approach**: Layered validation with fail-fast error handling
- **Error Messages**: Detailed field paths with expected vs actual values

### Validation Layers
1. **Basic Request Validation**: Provider ID, client request ID, frame presence
2. **Timestamp Validation**: SamplingClock and TimestampList validation with ordering checks
3. **Legacy Column Validation**: DataColumn and SerializedDataColumn backward compatibility
4. **New Column Validation**: All column-oriented data structures
5. **Cross-Cutting Validation**: Unique PV names across all column types

### Supported Column Types
**Scalar Columns**: DoubleColumn, FloatColumn, Int32Column, Int64Column, BoolColumn, StringColumn, EnumColumn
**Array Columns**: DoubleArrayColumn, FloatArrayColumn, Int32ArrayColumn, Int64ArrayColumn, BoolArrayColumn
**Complex Columns**: ImageColumn, StructColumn, SerializedDataColumn

### Validation Constraints
- **String Length**: 256 character maximum for StringColumn values
- **Array Dimensions**: 1-3 dimensions maximum, all dimension values > 0
- **Array Elements**: 10 million element maximum per array column
- **Image Size**: 50MB maximum per image payload  
- **Struct Size**: 1MB maximum per struct payload
- **Timestamp Integrity**: Non-decreasing timestamps, valid nanosecond ranges (0-999,999,999)
- **Sample Consistency**: All columns must have values matching timestamp count
- **Unique PV Names**: No duplicate PV names across any column type in a single frame

### Column Counting Logic
Updated `IngestionServiceImpl.ingestionResponseAck()` to count all column types:
```java
int numColumns = frame.getDataColumnsCount() + frame.getSerializedDataColumnsCount()
    + frame.getDoubleColumnsCount() + frame.getFloatColumnsCount() 
    + frame.getInt64ColumnsCount() + frame.getInt32ColumnsCount()
    + frame.getBoolColumnsCount() + frame.getStringColumnsCount()
    + frame.getEnumColumnsCount() + frame.getImageColumnsCount()
    + frame.getStructColumnsCount() + frame.getDoubleArrayColumnsCount()
    + frame.getFloatArrayColumnsCount() + frame.getInt32ArrayColumnsCount()
    + frame.getInt64ArrayColumnsCount() + frame.getBoolArrayColumnsCount();
```

## Performance Benchmarking Framework
The ingestion service includes a sophisticated benchmarking framework for performance comparison between different column-oriented data structures, particularly for high-frequency scenarios (4000 PVs at 1 KHz).

### Benchmark Architecture
- **Base Class**: `IngestionBenchmarkBase` provides common infrastructure
- **Strategy Pattern**: `ColumnBuilder` interface with implementation-specific builders
- **Factory Method**: `getColumnBuilder()` creates appropriate builder based on `ColumnDataType`
- **Threading**: Configurable multi-threaded execution with executor service pools

### Available Benchmarks
- **`BenchmarkIngestDataStream`**: Unidirectional streaming ingestion performance
- **`BenchmarkIngestDataBidiStream`**: Bidirectional streaming ingestion performance  
- **`BenchmarkIngestDataStreamBytes`**: Specialized streaming for serialized data

### Column Data Types
- **`DATA_COLUMN`**: Legacy sample-oriented DataColumn/DataValue structure (default)
- **`DOUBLE_COLUMN`**: New column-oriented DoubleColumn with packed double arrays
- **`SERIALIZED_DATA_COLUMN`**: SerializedDataColumn structure for custom serialization

### Column Builders
- **`DataColumnBuilder`**: Creates legacy DataColumn structures with individual DataValue objects per sample
- **`DoubleColumnBuilder`**: Creates efficient DoubleColumn with packed double arrays (avoids per-sample allocation)
- **`SerializedDataColumnBuilder`**: Creates SerializedDataColumn with custom serialized payload

### Usage Examples
```bash
# Run benchmark with legacy DataColumn structure (default)
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream

# Run benchmark with new efficient DoubleColumn structure
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --double-column

# Run benchmark with SerializedDataColumn structure
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --serialized-column

# Display usage help
java -cp target/dp-service-shaded.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream --help
```

### Performance Comparison
The framework enables direct memory allocation and throughput comparison:
- **Legacy DataColumn**: Creates individual DataValue objects for each sample (high memory allocation)
- **New DoubleColumn**: Uses packed double arrays (minimal allocation, better cache locality)
- **Memory Impact**: At 4000 PVs × 1000 samples/sec, DataColumn creates 4M objects/sec vs DoubleColumn's 4K arrays/sec

### Benchmark Configuration
Key parameters configured in benchmark classes:
- **`numThreads`**: Executor service thread pool size (typically 7)
- **`numStreams`**: Concurrent gRPC streams (typically 20)  
- **`numRows`**: Samples per ingestion request (typically 1000)
- **`numColumns`**: PVs per stream (typically 200, total 4000 PVs)
- **`numSeconds`**: Duration of benchmark run (typically 60 seconds)

## Testing Strategy
- **Framework**: JUnit 4 (imports `org.junit.*`, uses `@Test`, `@Before`, `@After`)
- **Integration Tests**: Located in `src/test/java/com/ospreydcs/dp/service/integration/`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase` provide common utilities
- **Test Database**: Uses "dp-test" database (cleaned between tests)
- **Scenario Methods**: Reusable test data generation (e.g., `simpleIngestionScenario()`, `createDataSetScenario()`)
- **Test Naming**: Test classes typically named `<APIMethod>Test`
- **Temporary Files**: Use `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();` for test files

### Annotation Service Test Framework

Integration tests for annotation service APIs follow a layered structure:

**Test base classes:**
- `AnnotationTestBase` — request builders, response observer inner classes, and `*Params` records for each API method
- `AnnotationIntegrationTestIntermediate` — extends `AnnotationTestBase`, starts the service and wires up the wrapper
- `GrpcIntegrationAnnotationServiceWrapper` — `sendAndVerifyXxx()` helper methods that send a request, await the response, and assert success/failure

**Response observer pattern** (defined as inner classes in `AnnotationTestBase`):
```java
public static class SaveXxxResponseObserver implements StreamObserver<SaveXxxResponse> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicBoolean isError = new AtomicBoolean(false);
    private final List<String> errorMessages = new ArrayList<>();
    private final List<SaveXxxResponse> responses = new ArrayList<>();

    @Override public void onNext(SaveXxxResponse r) { responses.add(r); }
    @Override public void onError(Throwable t) { isError.set(true); errorMessages.add(t.getMessage()); latch.countDown(); }
    @Override public void onCompleted() { latch.countDown(); }

    public void await() throws InterruptedException { latch.await(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS); }
    public boolean isError() { return isError.get(); }
    public String getErrorMessage() { return errorMessages.isEmpty() ? null : errorMessages.get(0); }
    public List<SaveXxxResponse> getResponses() { return responses; }
}
```

**`sendAndVerifyXxx()` wrapper pattern:**
```java
public String sendAndVerifyXxx(XxxParams params, boolean expectError, String expectedMsg) {
    XxxResponseObserver observer = new XxxResponseObserver();
    stub.saveXxx(buildSaveXxxRequest(params), observer);
    observer.await();
    if (expectError) {
        assertTrue(observer.isError() || responseHasReject(observer));
        if (expectedMsg != null) assertTrue(observer.getErrorMessage().contains(expectedMsg));
        return null;
    } else {
        assertFalse(observer.isError());
        // extract and return key identifier from response
    }
}
```

**`MongoTestClient` verification pattern:**
Add a `findXxx(String key)` method following the retry-loop pattern used by `findAnnotation()`:
```java
public XxxDocument findXxx(String key) throws InterruptedException {
    for (int i = 0; i < RETRY_ATTEMPTS; i++) {
        XxxDocument doc = mongoCollectionXxx.find(eq("keyField", key)).first();
        if (doc != null) return doc;
        Thread.sleep(RETRY_SLEEP_MS);
    }
    return null;
}
```
This handles asynchronous insertion: the job runs on a worker thread and the document may not yet exist when the test first queries MongoDB.

**Integration test structure** (see `PvMetadataIT` as the reference implementation):
- Extend `AnnotationIntegrationTestIntermediate`
- Group tests by operation: save/create, query (all criterion types), get, delete, stubs
- Use `sendAndVerify*` wrappers for the happy path and error cases
- Use `MongoTestClient.findXxx()` to verify direct DB state after saves
- For pagination, use `DpAnnotationServiceGrpc.newStub(channel)` directly with an inline `StreamObserver` and `CountDownLatch`

### Ingestion Test Framework
The ingestion test framework has been streamlined to support systematic addition of new protobuf column types with minimal boilerplate code.

**Framework Components:**
- **`IngestionTestBase.IngestionRequestParams`**: Simplified parameter object with dedicated fields for each column type
- **`buildIngestionRequest()`**: Streamlined method that uses column lists from params object
- **`GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`**: Enhanced verification logic for all column types

**Adding New Protobuf Column Types:**
Follow this systematic 7-step process for complete implementation:

**Implementation Steps (1-5):**
1. **Create Document Class**: Implement `ScalarColumnDocumentBase<T>` with `addColumnToBucket()` method
2. **Add Ingestion Handling**: Update `BucketDocument.generateBucketsFromRequest()` to handle new column type
3. **Register POJO Class**: Add document class to `MongoClientBase.getPojoCodecRegistry()`
4. **Data Subscription**: Update `SourceMonitorManager.publishDataSubscriptions()` for new column type
5. **Event Subscription**: Update `ColumnTriggerUtility` and `DataBuffer` for trigger and size estimation support

**Testing Steps (6-7):**
6. **Test Framework Support**: 
   - Add `List<NewColumnType>` field to `IngestionTestBase.IngestionRequestParams`
   - Update `buildIngestionRequest()` to include new columns in `IngestDataRequest`
   - Add verification logic to `GrpcIntegrationIngestionServiceWrapper.verifyIngestionRequestHandling()`
7. **Integration Test**: Create `<ColumnType>IT` test covering ingestion, query, subscription, and event APIs

**Query API Integration:**
New protobuf column types automatically work in query results through the `addColumnToBucket()` method:
- No additional query API code required
- Document classes implement abstract `addColumnToBucket()` from `ColumnDocumentBase`
- Query results assemble `DataBucket` using column-specific `addColumnToBucket()` implementations

**Verification Pattern:**
The verification logic follows a consistent pattern for each column type:
- Retrieve `DataColumnDocument` from stored `BucketDocument`
- Convert document to corresponding protobuf column using `toProtobufColumn()`
- Match protobuf column against original columns from the request's column list
- Verify data integrity through protobuf round-trip comparison

**Example Verification Flow:**
```java
// For FloatColumn verification:
DataColumnDocument dataColumnDocument = bucketDocument.getDataColumnDocument();
FloatColumn storedColumn = (FloatColumn) dataColumnDocument.toProtobufColumn();
// Find matching column from request.getFloatColumnsList()
// Verify storedColumn matches original request data
```

**Benefits:**
- **Systematic**: Same 7-step pattern for every new column type
- **Comprehensive**: Tests full pipeline from ingestion → storage → query → subscription → events
- **Maintainable**: Centralized verification logic in wrapper class
- **Extensible**: Easy to add new column types without modifying existing infrastructure

### Ingestion Validation Test Coverage
- **Test Location**: `IngestionValidationUtilityTest` (22 test cases)
- **Legacy Validation**: Provider ID, request ID, DataColumn validation (6 tests)
- **New Column Types**: DoubleColumn, StringColumn, EnumColumn, Array, Image, Struct validation (10 tests)  
- **Advanced Scenarios**: Duplicate PV names, timestamp integrity, multi-column success cases (6 tests)
- **Error Message Testing**: Validates detailed field paths and constraint violations
- **Boundary Testing**: String length limits, array dimension limits, timestamp ordering

### V2 API Integration Test Coverage
- **Test Location**: `src/test/java/com/ospreydcs/dp/service/integration/v2api/`
- **Naming Convention**: `<ColumnType>IT` (e.g., `DoubleColumnIT`, `DoubleArrayColumnIT`, `StructColumnIT`, `ImageColumnIT`, `SerializedDataColumnIT`)
- **Comprehensive Coverage**: Each test class covers ingestion, query, and subscription APIs for one column type
- **Query API Integration**: Tests verify `addColumnToBucket()` method implementation for query result assembly
- **Framework Pattern**: Same integration test structure applies to scalar, array, and binary column types
- **SerializedDataColumn Support**: Complete integration test framework support added, including `setSerializedDataColumnList()` in `IngestionRequestParams`

#### Array and Binary Column Test Patterns
- **Dual PV Approach**: Array and binary integration tests use scalar columns as triggers and array/binary columns as targets
- **Event Subscriptions**: Array and binary columns cannot serve as trigger PVs, requiring scalar trigger + target column pattern
- **PV Validation**: Both scalar and target PVs included in initial ingestion for proper subscription validation
- **Export Testing**: Array and binary columns skip tabular export tests due to binary storage limitations
- **Binary Column Types**: StructColumn (with schemaId) and ImageColumn (with ImageDescriptor) extend BinaryColumnDocumentBase

### Scalar Column Document Test Coverage  
- **Unit Tests**: `ScalarColumnDocumentBaseTest` - Basic functionality of generic base class
- **Protobuf Conversion Tests**: `ScalarColumnDocumentBaseProtobufTest` (7 test cases)
- **Integration Tests**: `integration/v2api/DoubleColumnIT`, `integration/v2api/FloatColumnIT`, `integration/v2api/Int64ColumnIT` - End-to-end column pipelines (ingestion, query, subscription)

**ScalarColumnDocumentBaseProtobufTest Coverage:**
- **Core Functionality**: Document → protobuf conversion via `toProtobufColumn()`
- **Round-trip Integrity**: Protobuf → document → protobuf data integrity
- **Legacy Compatibility**: DataColumn conversion using inherited methods
- **Serialization**: Byte array serialization through `getBytes()`
- **Edge Cases**: Empty values, null names, large datasets (1000+ values)
- **Error Handling**: Null name handling with safe fallback to empty string

**Critical for Query/Export Pipeline:**
The `toProtobufColumn()` method is essential for:
- Tabular query results (MongoDB → protobuf API responses)
- CSV export (document → protobuf → tabular assembly)
- Excel export (document → protobuf → XLSX generation)  
- HDF5 export (document → protobuf → HDF5 file creation)

**Testing Strategy Benefits:**
- **Direct Coverage**: Tests core conversion logic without full query/export complexity
- **Fast Execution**: Unit tests vs slow integration test pipelines
- **Comprehensive**: All scalar column types use same base class logic
- **Template Pattern**: Same test structure applies to all ScalarColumnDocumentBase implementations

## Continuous Integration
- **GitHub Actions**: Automated CI/CD pipeline in `.github/workflows/ci.yml`
- **Multi-Repository Setup**: Automatically builds dp-grpc dependency before testing dp-service
- **Triggers**: 
  - Automatic testing on pushes to main/master branch
  - Automatic testing on pull requests to main/master
  - Manual workflow dispatch for testing dev branches or ad-hoc validation
- **Services**: Uses MongoDB 8.0 service container for integration tests
- **Test Reports**: Uploads Surefire and Failsafe test reports as workflow artifacts
- **Dependencies**: Builds and installs dp-grpc to local Maven repository before running dp-service tests