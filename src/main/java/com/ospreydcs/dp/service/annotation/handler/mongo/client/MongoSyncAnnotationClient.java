package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Override
    public InsertOneResult insertAnnotation(AnnotationDocument annotationDocument) {

        logger.debug(
                "inserting AnnotationDocument document to mongo author: {} type: {}",
                annotationDocument.getAuthorId(), annotationDocument.getType());

        // insert AnnotationDocument to mongodb
        InsertOneResult result = null;
        try {
            result = mongoCollectionAnnotations.insertOne(annotationDocument);
        } catch (MongoException ex) {
            final String errorMsg = "insertRequestStatus MongoException: " + ex.getMessage();
            logger.error(errorMsg);
            return null;
        }

        return result;
    }

}
