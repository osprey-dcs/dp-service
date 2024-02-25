# Service Configuration

## Default config file

The default configuration file for the Data Platform services is in [application.yml](https://github.com/osprey-dcs/dp-service/blob/main/src/main/resources/application.yml).  The initial configuration options are minimal.  The config file contains in-line comments for each section and individual configuration options.  Contents are pasted below for convenience.

```
# MongoClient: Settings for connection to MongoDB.
MongoClient:

  # MongoClient.dbHost: The host that is running the MongoDB server.
  # Default value is "localhost", specifying that the database is running on the same host as the Data Platform services.
  dbHost: localhost

  # MongoClient.dbPort: Network port on dbHost for the MongoDB service.  MongoDB default installation is 27017.
  dbPort: 27017

  # MongoClient.dbUser: Username for connection to MongoDB by Data Platform services.
  dbUser: admin

  # MongoClient.dbPassword: Password for dbUser for connection to MongoDB by Data Platform services.
  dbPassword: admin

# IngestionServer: Settings for the Ingestion server process.
IngestionServer:

  # IngestionServer.port: Network port for Ingestion Service gRPC.
  port: 50051

# IngestionHandler: Settings for the Ingestion Service request handler.
IngestionHandler:

  # IngestionHandler.numWorkers: Number of worker threads used by the Ingestion Service request handler.
  # This parameter might take some tuning on deployments to get the best performance.
  numWorkers: 7

# IngestionBenchmark: Settings for the Ingestion Service performance benchmark application.
IngestionBenchmark:

  # IngestionBenchmark.grpcConnectString: String used for gRPC connection to Ingestion Service including "host:port".
  grpcConnectString: "localhost:50051"

  # IngestionBenchmark.startSeconds: Specifies fixed start time for data created by Ingestion benchmark application.
  startSeconds: 1698767462

# QueryServer: Settings for the Query server process.
QueryServer:

  # QueryServer.port: Network port for Query Service gRPC.
  port: 50052

# QueryHandler: Settings for the Query Service request handler.
QueryHandler:

  # QueryHandler.numWorkers: Number of worker threads used by the Query Service request handler.
  # This parameter might take some tuning on deployments to get the best performance.
  numWorkers: 7

# QueryBenchmark: Settings for the Query Service performance benchmark applications.
QueryBenchmark:

  # QueryBenchmark.grpcConnectString: String used for gRPC connection to Query Service including "host:port".
  grpcConnectString: "localhost:50052"

```

The default settings are probably reasonable for most development systems, though you'll want to override the MongoDB uername and password to match your configuration (or make them both "admin").  Options for overriding are discussed below.

## Overriding config file

The default configuration file can be overridden in two different ways, by specifying an alternative file on either the command line used to start the application, or as an environment variable.

To specify an alternative on the command line, add a VM option (e.g., on the command line BEFORE the class name) like the following: "-Ddp.config=/path/to/config/override.yml".

To specify an alternative via an envoronment variable, define a variable in the environment "DP.CONFIG=/path/to/config/override.yml" before running the Ingestion Service application.  For example:
```
java -Ddp.config=$DP_HOME/config/dp.yml -Dlog4j.configurationFile=$DP_HOME/config/log4j2.xml -cp $DP_HOME/lib/dp-service/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```

## Overriding individual configuration properties

In addition to overriding the default config file, individual configuration settings can be overridden on the command line.  To do so, use a VM option (in the java command line BEFORE the class name), prefixing the configuration setting name with "dp.".  For example, to override the gRPC port for the Ingestion Service, use "-Ddp.IngestionServer.port=50052".