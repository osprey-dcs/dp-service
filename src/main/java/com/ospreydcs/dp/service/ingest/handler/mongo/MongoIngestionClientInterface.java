package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;

import java.util.List;

public interface MongoIngestionClientInterface {
    boolean init();
    boolean fini();
    MongoIngestionHandler.IngestionTaskResult insertBatch(IngestDataRequest request, List<BucketDocument> dataDocumentBatch);
    InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument);
}
