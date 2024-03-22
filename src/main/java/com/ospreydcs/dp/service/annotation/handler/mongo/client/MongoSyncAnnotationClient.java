package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Override
    public MongoInsertOneResult insertDataSet(DataSetDocument dataSetDocument) {

        logger.debug("inserting DataSetDocument to mongo");

        // insert document to mongodb
        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionDataSets.insertOne(dataSetDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting DataSet: " + ex.getMessage();
            logger.error(errorMsg);
        }

        return new MongoInsertOneResult(isError, errorMsg, result);

    }

    @Override
    public MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument) {

        logger.debug(
                "inserting AnnotationDocument to mongo owner: {} type: {}",
                annotationDocument.getOwnerId(), annotationDocument.getType());

        // insert AnnotationDocument to mongodb
        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionAnnotations.insertOne(annotationDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting Annotation: " + ex.getMessage();
            logger.error(errorMsg);
        }

        return new MongoInsertOneResult(isError, errorMsg, result);
    }

}
