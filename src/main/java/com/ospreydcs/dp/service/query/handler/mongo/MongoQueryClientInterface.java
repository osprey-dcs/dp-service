package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;

public interface MongoQueryClientInterface {
    boolean init();
    boolean fini();
    MongoCursor<BucketDocument> executeQuery(HandlerQueryRequest request);
}
