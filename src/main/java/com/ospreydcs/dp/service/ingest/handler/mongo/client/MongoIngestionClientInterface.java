package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.UpdateResultWrapper;
import com.ospreydcs.dp.service.ingest.handler.model.FindProviderResult;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;

import java.util.List;

public interface MongoIngestionClientInterface {

    boolean init();
    boolean fini();

    UpdateResultWrapper upsertProvider(RegisterProviderRequest request);
    FindProviderResult findProvider(String providerName);
    boolean validateProviderId(String providerId);

    IngestionTaskResult insertBatch(IngestDataRequest request, List<BucketDocument> dataDocumentBatch);

    InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument);

    MongoCursor<RequestStatusDocument> executeQueryRequestStatus(QueryRequestStatusRequest request);
}
