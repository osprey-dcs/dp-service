package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;

public interface MongoQueryClientInterface {
    boolean init();
    boolean fini();
    MongoCursor<BucketDocument> executeQuery(QueryDataByTimeRequest request);
}
