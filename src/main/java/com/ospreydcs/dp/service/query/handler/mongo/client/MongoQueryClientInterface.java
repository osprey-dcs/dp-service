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

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(QueryPvStatsRequest request);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(Collection<String> pvNameList);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(String pvNamePatternString);

    /**
     * Returns the subset of the specified PV names that exist in the archive. This is a cheap
     * existence check backed by a {@code distinct} on the pvName index, avoiding the full stat
     * aggregation (sort + group over all buckets for each PV) performed by executeQueryPvStats().
     * Returns null if a database error occurs.
     */
    Collection<String> executeQueryPvExistence(Collection<String> pvNameList);

    MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id);
}
