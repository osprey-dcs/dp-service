package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;

import java.util.Collection;

public interface MongoQueryClientInterface {

    boolean init();
    boolean fini();

    MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument dataBlock);

    MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec);

    MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(QueryPvMetadataRequest request);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(Collection<String> pvNameList);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(String pvNamePatternString);

    MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderMetadata(QueryProviderMetadataRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderMetadata(String id);
}
