# dp-service overview

This repo is part of the Data Platform project.  The Data Platform consists of services for capturing and providing access to data captured from a particle accelerator facility.  The [data-platform repo](https://github.com/osprey-dcs/data-platform) provides a project overview and links to the various project componnents, as well as an installer for running the latest version.

This dp-service repo contains Java implementations of the Data Platform services, including Ingestion, Query, and Annotation Services.  The Ingestion Service provides a variety of APIs for use in capturing data to the archive with a focus on the performance required to handle the data rates in an accelerator facility.  The Query Service provides APIs for retrieving raw time-series data for use in machine learning applications, and higher-level APIs for retrieving tabular time-series data as well as for querying metadata and annotations in the archive.  The Annotation Service provides APIs for annotating the data in the archive.


# User Documentation

## Running server applications, performance benchmarks, and sample data generator

Notes for running the Data Platform server and client applications are linked [here](doc/running.md).

## Service configuration options

Options for configuring Data Platform services are desribed in more detail in [the configuration documentation](doc/configuration.md).


# Technical Details

The [Data Platform Technical Overview](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#data-platform-service-implementations) contains details about the Data Platform Service implementations including the following sections:

## [dp-service patterns and frameworks](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#dp-service-patterns-and-frameworks)

* [overview of key classes](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#dp-service-patterns-and-frameworks)
* [gRPC server](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#grpc-server)
* [service request handling framework](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#service-request-handling-framework)
* [handling for bidirectional streaming API methods](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#handling-for-bidirectional-streaming-api-methods)
* [MongoDB interface](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#mongodb-interface)
* [serialization of protobuf objects to MongoDB documents](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#serialization-of-protobuf-objects-to-mongodb-documents)
* [configuration](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#configuration)
* [exporting data](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#exporting-data)
* [performance benchmarking](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#performance-benchmarking)
  * [ingestion service performance benchmarking](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#ingestion-service-performance-benchmarking)
  * [query service performance benchmarking](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#query-service-performance-benchmarking)
* [generating sample data](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#generating-sample-data)
* [regression testing](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#regression-testing)
  * [ConfigurationManager tests](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#configurationmanager-tests-comospreydcsdpservicecommonconfig)
  * [mongo tests](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#mongo-tests-comospreydcsdpservicecommonmongo)
  * [ingestion service tests](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#ingestion-service-tests-comospreydcsdpserviceingest)
  * [query service tests](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#query-service-tests-comospreydcsdpservicequery)
  * [annotation service tests](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#annotation-service-tests-comospreydcsdpserviceannotation)
* [integration testing](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#integration-testing)
  * [benchmark integration test](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#benchmark-integration-test)

## [dp-service MongoDB schema and data flow](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#dp-service-mongodb-schema-and-data-flow)

* [providers](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#providers)
* [buckets](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#buckets)
* [requestStatus](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#requeststatus)
* [dataSets](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#datasets)
* [annotations](https://github.com/osprey-dcs/data-platform/blob/main/doc/documents/dp/dp-tech.md#annotations)