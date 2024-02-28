package com.ospreydcs.dp.service.annotation.handler.mongo;

import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoSyncAnnotationClient;

public class MongoAnnotationHandler implements AnnotationHandlerInterface {

    private final MongoAnnotationClientInterface mongoClient;

    public MongoAnnotationHandler(MongoAnnotationClientInterface clientInterface) {
        this.mongoClient = clientInterface;
    }

    public static MongoAnnotationHandler newMongoSyncAnnotationHandler() {
        return new MongoAnnotationHandler(new MongoSyncAnnotationClient());
    }

}
