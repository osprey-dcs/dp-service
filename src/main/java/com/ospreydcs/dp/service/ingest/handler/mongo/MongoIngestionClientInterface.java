package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;

import java.util.List;

public interface MongoIngestionClientInterface {
    boolean init();
    boolean fini();
    MongoIngestionHandler.IngestionTaskResult insertBatch(IngestionRequest request, List<BucketDocument> dataDocumentBatch);
    InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument);
}
