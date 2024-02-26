# Data Platform Overview

This repo is part of the Data Platform project.  The Data Platform consists of services for capturing and providing access to data captured from a particle accelerator facility.  The [data-platform repo](https://github.com/osprey-dcs/data-platform) provides a project overview and links to the various project componnents, as well as an installer for running the latest version.

This repo includes Java implementations of the Data Platform services described in the [data-platform project repo](https://github.com/osprey-dcs/data-platform).

The service APIs are built using [gRPC](https://grpc.io/docs/what-is-grpc/introduction/) for both interface definition and message interchange.  The latest gRPC proto files for the service APIs are contained and documented in the [dp-grpc repo](https://github.com/osprey-dcs/dp-grpc).  Using gRPC, client applications can be built to interact with the Data Platform services using practically any programming language.

The [dp-support repo](https://github.com/osprey-dcs/dp-support) includes a set of utilities for managing the processes comprising the Data Platform ecosystem.

The [dp-web-app repo](https://github.com/osprey-dcs/dp-web-app) contains a JavaScript web application that utilizes the Data Platform Query Service to navigate the archive.

# dp-service

The dp-service repo includes two service implementations, an Ingestion Service and a Query Service.  The Ingestion Service provides a variety of APIs for use in capturing data to the archive with a focus on the performance required to handle the data rates in an accelerator facility.  The Query Service provides APIs for retrieving raw time-series data for use in machine learning applications, and higher-level APIs for retrieving tabular time-series data as well as for querying metadata and annotations in the archive.

Additional user-oriented and developer-oriented documentation for the dp-service repo is provided below.

# User Documentation

## Service configuration options

Options for configuring Data Platform services are desribed in more detail in [the configuration documentation](./doc/configuration.md).

## Running services and applications

Notes for running the services and applications are linked [here](doc/running.md).

# Developer Documentation

This section contains links to developer-oriented documentation with details about various parts of the dp-service repository.

TODO: documentation of service framework, performance benchmark applications, and integration tests coming soon!

## Ingestion service

* [initial ingestion service implementation](doc/ingestion.md)
* [refactoring of ingestion service](doc/refactor.md)