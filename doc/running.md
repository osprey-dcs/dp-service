## core services

Below are java command lines for running the Data Platform Core Services, using the standard "dp" MongoDB database.  Use these commands for production and test systems.

The [dp-support repo](https://github.com/osprey-dcs/dp-support?tab=readme-ov-file#data-platform-server-scripts) includes bash scripts for starting the services.

#### Ingestion Service
```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer"
```

#### Query Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer"
```

#### Annotation Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer"
```

## performance benchmark applications

The Data Platform includes tools for measuring performance of the Ingestion and Query Services.  There are special versions of the server applications that override the standard network port number and database, using the MongoDB database "dp-benchmark".  To run a benchmark for either service you must run both the server and client application.  Java command lines are given below.

The [dp-support repo](https://github.com/osprey-dcs/dp-support?tab=readme-ov-file#data-platform-performance-benchmarks) includes bash scripts for starting the benchmark servers and running the client applications.

### ingestion performance benchmark

#### ingestion benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer"
```

#### ingestion benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkBidiStreamingIngestion
```

### query performance benchmark

#### query benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryGrpcServer"
```

#### query benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataStream
```

## sample data generator

Recognizing the previous use of the Ingestion Service performance benchmark to generate sample data for web development and demo purposes, a sample data generator application was created to generate sample data for one minute and 4,000 signals each sampled at 1 KHz.  The data generator uses the standard "dp" database and data is created with a fixed date/time starting at "2023-10-31T15:51:00.000+00:00".  Happy Halloween...

To use the sample data generator, run the standard Ingestion Service as shown above, and use the Java command line below to run the client application.

The [dp-support repo](https://github.com/osprey-dcs/dp-support?tab=readme-ov-file#data-platform-sample-data-generator) includes bash scripts for starting the standard Ingestion Service and running the sample data generator application.

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.utility.TestDataGenerator
```

## Custom Configuration via Environment Variables

- **What:** The service reads configuration values from `application.yml` using the ${ENV_VAR:default} substitution syntax. If an environment variable is set, it overrides the default value declared in the YAML. If it is not set, the YAML default is used.
- **How:** Set environment variables before starting the JVM or provide them in your container/compose environment. Example (shell):

```bash
export DP_MONGO_DB_HOST=mongo.example.com
export DP_MONGO_DB_USER=dp_user
export DP_MONGO_DB_PASSWORD=secret
java <other options> com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```

- **Docker / docker-compose:** Put the same variables in the `environment:` block for the container. Example (docker-compose.yml):

```yaml
services:
	dp-service:
		image: your-image
		environment:
			- DP_MONGO_DB_HOST=mongo.example.com
			- DP_MONGO_DB_USER=dp_user
			- DP_MONGO_DB_PASSWORD=secret
```

- **Notes:**
	- Boolean values should be set as the strings `true` or `false` (unquoted or quoted are both accepted by the shell/container). Numeric values should be plain numbers. Strings containing colons or special characters should be quoted.
	- `DP_MONGO_DB_URI` is a full connection string and, when provided, takes precedence over the simpler host/port/user/password fields (see `application.yml`).

**Accepted environment variables**

Below is the list of environment variables referenced in the project's `application.yml`. Each variable maps to a configuration property; if not set, the YAML default applies.

- `DP_MONGO_DB_URI` : Optional full MongoDB connection string (overrides host/port/user/password). Example: `mongodb://user:pass@host:27017/?authSource=admin`
- `DP_MONGO_DB_HOST` : MongoDB host (default: `localhost`)
- `DP_MONGO_DB_PORT` : MongoDB port (default: `27017`)
- `DP_MONGO_DB_USER` : MongoDB username (default: `admin`)
- `DP_MONGO_DB_PASSWORD` : MongoDB password (default: `admin`)
- `DP_GRPC_CLIENT_HOSTNAME` : gRPC client hostname (default: `localhost`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIME_SECONDS` : gRPC client keepalive time in seconds (default: `45`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC client keepalive timeout in seconds (default: `20`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC client keepalive without calls (default: `true`)
- `DP_GRPC_SERVER_INCOMING_MESSAGE_SIZE_LIMIT_BYTES` : gRPC incoming message size limit in bytes (default: `4096000`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIME_SECONDS` : gRPC server keepalive time in seconds (default: `60`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC server keepalive timeout in seconds (default: `20`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS` : gRPC server permit-keepalive time in seconds (default: `30`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC server permit keepalive without calls (default: `true`)
- `DP_INGESTION_SERVER_PORT` : Ingestion Service gRPC port (default: `50051`)
- `DP_INGESTION_HANDLER_NUM_WORKERS` : Number of ingestion handler worker threads (default: `7`)
- `DP_INGESTION_HANDLER_SOURCEMONITOR_VALIDATE_PVS` : Ingestion SourceMonitor validate PVs (default: `true`)
- `DP_INGESTION_BENCHMARK_GRPC_CONNECT_STRING` : Ingestion benchmark gRPC `host:port` connect string (default: `localhost:50051`)
- `DP_INGESTION_BENCHMARK_START_SECONDS` : Ingestion benchmark fixed start time (epoch seconds)
- `DP_QUERY_SERVER_PORT` : Query Service gRPC port (default: `50052`)
- `DP_QUERY_HANDLER_NUM_WORKERS` : Number of query handler worker threads (default: `7`)
- `DP_QUERY_HANDLER_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES` : Query handler outgoing message size limit (default: `4096000`)
- `DP_QUERY_BENCHMARK_GRPC_CONNECT_STRING` : Query benchmark gRPC `host:port` connect string (default: `localhost:50052`)
- `DP_ANNOTATION_SERVER_PORT` : Annotation Service gRPC port (default: `50053`)
- `DP_ANNOTATION_HANDLER_NUM_WORKERS` : Number of annotation handler worker threads (default: `7`)
- `DP_EXPORT_SERVER_MOUNT_POINT` : Export server mount point (default: `/tmp`)
- `DP_EXPORT_SHARE_MOUNT_POINT` : Export share mount point (defaults to server mount point if unset)
- `DP_EXPORT_URL_BASE` : Base URL for accessing export files (default: `http://localhost:8081`)
- `DP_EXPORT_TABULAR_EXPORT_FILE_SIZE_LIMIT_BYTES` : Tabular export file size limit (default: `4096000`)
- `DP_INGESTION_STREAM_SERVER_PORT` : Ingestion Stream Service gRPC port (default: `50054`)
- `DP_INGESTION_STREAM_HANDLER_NUM_WORKERS` : Number of ingestion stream handler worker threads (default: `7`)
- `DP_INGESTION_STREAM_EVENTMONITOR_MAX_MESSAGE_SIZE_BYTES` : EventMonitor max message size (default: `4096000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_FLUSH_INTERVAL_MILLIS` : DataBuffer flush interval in ms (default: `500`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_BYTES` : DataBuffer max bytes (default: `524288`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_ITEMS` : DataBuffer max items (default: `50`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_LIMIT_NANOS` : DataBuffer age limit in nanoseconds (default: `2000000000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_CUSHION_NANOS` : DataBuffer age cushion in nanoseconds (default: `1000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_EXPIRATION_NANOS` : TriggeredEventManager expiration in nanoseconds (default: `5000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_CLEANUP_INTERVAL_MILLIS` : TriggeredEventManager cleanup interval in ms (default: `5000`)

If you want me to also add a short example `docker-compose.yml` snippet that demonstrates mounting configuration files while still using environment overrides, I can add that as a follow-up.
