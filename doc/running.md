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
