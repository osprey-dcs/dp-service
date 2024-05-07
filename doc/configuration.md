# Service Configuration

## Default config file

The default configuration file for the Data Platform services is in [application.yml](https://github.com/osprey-dcs/dp-service/blob/main/src/main/resources/application.yml).  The initial configuration options are minimal.  The config file contains in-line comments for each section and individual configuration options.

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