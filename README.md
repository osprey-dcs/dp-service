# Data Platform Services

This repo includes Java implementations of the Data Platform services described in the [data-platform project repo](https://github.com/osprey-dcs/data-platform).

The Data Platform services are focused on capturing data from an experimental research facility, such as a particle accelerator, and making that data available for use in control systems applications.

The repo includes two service implementations, an Ingestion Service and a Query Service.  The Ingestion Service provides a variety of APIs for use in capturing data to the archive with a focus on the performance required to handle the data rates in an accelerator facility.  The Query Service provides APIs for retrieving raw time-series data for use in machine learning applications, and higher-level APIs for retrieving tabular time-series data as well as for querying metadata and annotations in the archive.

The service APIs are built using [gRPC](https://grpc.io/docs/what-is-grpc/introduction/) for both interface definition and message interchange.  The latest gRPC proto files for the service APIs are contained and documented in the [dp-grpc repo].  Using gRPC, client applications can be built to interact with the Data Platform services using practically any programming language. 

Additional user and developer documentation for the dp-service repo is provided below.

# User Documentation

## Service configuration options

Options for configuring Data Platform services are desribed in more detail in [the configuration documentation](./doc/configuration.md).

## Running services and applications

TODO: documentation for runnable applications coming soon!

# Developer Documentation

This section contains links to developer-oriented documentation with details about various parts of the dp-service repository.

TODO: documentation of service framework, performance benchmark applications, and integration tests coming soon!

## Ingestion service

* [initial ingestion service implementation](doc/ingestion.md)
* [refactoring of ingestion service](doc/refactor.md)