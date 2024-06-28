package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.bson.MetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;

import java.util.Collection;

public interface MongoQueryClientInterface {

    boolean init();
    boolean fini();

    MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec);

    MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request);

    MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(QueryMetadataRequest request);

    MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(Collection<String> pvNameList);

    MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(String pvNamePatternString);

}
