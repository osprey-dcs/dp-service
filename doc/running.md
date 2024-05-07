### running the ingestion server application

Here is the Java command line to run the server directly (update file paths as appropriate for your installation):

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer"
```

### running the ingestion performance benchmark application

Here is the Java command line to run the application directly (update file paths as appropriate for your installation):
```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.ingest.benchmark.IngestionPerformanceBenchmark
```

### running the query server application

Here is the Java command line to run the server directly (update file paths as appropriate for your installation):

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer"
```

### running the query performance benchmark application

Here is the Java command line to run the application directly (update file paths as appropriate for your installation):
```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataStream
```

### running the annotation server application

Here is the Java command line to run the server directly (update file paths as appropriate for your installation):

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer"
```
