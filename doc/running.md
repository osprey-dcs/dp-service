### running the ingestion server application

Here is the Java command line to run the server directly (update file paths as appropriate for your installation):

```
java -Ddp.config=~/data-platform/config/dp-ingest.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -jar ~/data-platform/lib/dp-ingest/dp-ingest.jar
```

### running the ingestion performance benchmark application

Here is the Java command line to run the application directly (update file paths as appropriate for your installation):
```
java -Ddp.config=~/data-platform/config/dp-ingest.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-ingest/dp-ingest.jar com.ospreydcs.dp.ingest.benchmark.IngestionPerformanceBenchmark
```
