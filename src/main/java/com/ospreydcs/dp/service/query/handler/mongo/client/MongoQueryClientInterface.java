package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import org.bson.Document;

public interface MongoQueryClientInterface {
    boolean init();
    boolean fini();
    MongoCursor<BucketDocument> executeQuery(QueryRequest.QuerySpec querySpec);
    MongoCursor<Document> getColumnInfo(QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec);
}
