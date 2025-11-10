# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Build without tests: `mvn clean package -DskipTests`
- Run unit tests only: `mvn test`
- Run unit + integration tests: `mvn verify`
- Run integration tests only: `mvn failsafe:integration-test`
- Run single unit test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run single integration test: `mvn failsafe:integration-test -Dit.test=ClassNameIT`
- Run specific service: 
  - Ingestion: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer`
  - Query: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer`
  - Annotation: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.11.0-shaded.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer`

## Architecture Overview
This is a Data Platform service implementation with three main services:
- **Ingestion Service**: Handles data ingestion with high-performance streaming APIs
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
4. Follow systematic renaming pattern: Service → Handler → Jobs → Dispatchers → Tests

## MongoDB Collections
- **buckets**: Time-series data storage (main data collection with embedded protobuf serialization)
- **providers**: Registered data providers
- **requestStatus**: Ingestion request tracking
- **dataSets**: Annotation dataset definitions (contains DataBlockDocuments for time ranges and PV names)
- **annotations**: Data annotations (references dataSets and optionally calculations)
- **calculations**: Associated calculation results (embedded CalculationsDataFrameDocuments)

### Document Embedding Pattern
MongoDB documents use embedded protobuf serialization:
- `BucketDocument` contains embedded `DataTimestampsDocument` and `DataColumnDocument`
- `CalculationsDocument` contains embedded `CalculationsDataFrameDocument` list
- `DataSetDocument` contains embedded `DataBlockDocument` list
- Protobuf objects serialized to `bytes` field, with convenience fields for queries

## Export Framework Architecture
The Annotation Service includes a sophisticated export framework:
- **Base Classes**: `ExportDataJobBase` → `ExportDataJobAbstractTabular` → format-specific jobs
- **Format Jobs**: `ExportDataJobCsv`, `ExportDataJobExcel`, `ExportDataJobHdf5`
- **File Interfaces**: `TabularDataExportFileInterface` implemented by `DataExportXlsxFile`, etc.
- **Data Processing**: `TimestampDataMap` for tabular data assembly, `TabularDataUtility` for data manipulation
- **Excel Implementation**: Uses Apache POI with `XSSFWorkbook` for reliable XLSX generation

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

## Code Style Guidelines
- Java 21 is used for this project
- MongoDB is used for persistence with embedded protobuf serialization
- Package structure: `com.ospreydcs.dp.service.<component>`
- Follow existing naming conventions (CamelCase for classes, lowerCamelCase for methods)
- API method implementations follow: Handler → Job → Database Client → Dispatcher pattern
- Jobs named as `<APIMethod>Job`, Dispatchers as `<APIMethod>Dispatcher`
- Error handling uses DpException and structured logging
- Integration tests located in `src/test/integration/java/com/ospreydcs/dp/service/integration/` with `*IT.java` naming
- Unit tests located in `src/test/java/` with `*Test.java` naming
- Follow existing patterns for protobuf ↔ MongoDB document conversion
- Result objects use `ResultStatus` class with `isError` (Boolean) and `msg` (String) fields

## API Method Naming Conventions
Recent API evolution has moved from "create" to "save" semantics:
- `saveDataSet()` performs upsert operations (create or update)
- `saveAnnotation()` performs upsert operations (create or update)
- Request/Response/Result types follow `Save*Request`, `Save*Response`, `Save*Result` patterns
- Legacy "create" references should be updated to "save" when encountered

## Client API Utilities
- **Data Import**: `DataImportUtility.importXlsxData()` for importing time-series data from Excel files
  - Located in `com.ospreydcs.dp.client.utility.DataImportUtility`
  - Returns `DataImportResult` with timestamps and DataColumn objects
  - Uses Apache POI for Excel file processing
  - Expects format: `[seconds, nanos, pv_data_columns...]` with header row

## Testing Strategy
- **Framework**: JUnit 4 (imports `org.junit.*`, uses `@Test`, `@Before`, `@After`)
- **Test Separation**: Directory-based separation for optimal CI workflows
  - **Unit Tests**: Located in `src/test/java/` with `*Test.java` naming - run with `mvn test`
  - **Integration Tests**: Located in `src/test/integration/java/com/ospreydcs/dp/service/integration/` with `*IT.java` naming - run with `mvn verify`
- **Test Base Classes**: `AnnotationTestBase`, `QueryTestBase`, `IngestionTestBase` provide common utilities
- **Integration Test Base Classes**: `GrpcIntegrationTestBase`, `GrpcIntegrationServiceWrapperBase` in integration package
- **Test Database**: Uses "dp-test" database (cleaned between tests)
- **Scenario Methods**: Reusable test data generation (e.g., `simpleIngestionScenario()`, `createDataSetScenario()`)
- **Maven Plugins**: Surefire for unit tests, Failsafe for integration tests
- **Temporary Files**: Use `@Rule public TemporaryFolder tempFolder = new TemporaryFolder();` for test files