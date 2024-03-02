package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;

public interface MongoAnnotationClientInterface {
    boolean init();
    boolean fini();
    MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument);
}
