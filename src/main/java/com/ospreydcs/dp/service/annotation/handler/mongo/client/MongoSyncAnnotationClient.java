package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Indexes.ascending;
import static com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument.ANNOTATION_TYPE_COMMENT;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Override
    public DataSetDocument findDataSet(String dataSetId) {
        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a dataSetId, it already exists in the database.
        List<DataSetDocument> matchingDocuments = new ArrayList<>();
        mongoCollectionDataSets.find(eq(BsonConstants.BSON_KEY_DATA_SET_ID, dataSetId)).into(matchingDocuments);
        if (matchingDocuments.size() > 0) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
    }

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

    @Override
    public MongoCursor<AnnotationDocument> executeQueryAnnotations(QueryAnnotationsRequest request) {

        // create query filter from request search criteria
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();
        final List<QueryAnnotationsRequest.QueryAnnotationsCriterion> criterionList = request.getCriteriaList();
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : criterionList) {
            switch (criterion.getCriterionCase()) {

                case OWNERCRITERION -> {
                    // update ownerFilter from OwnerCriterion
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if (! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case COMMENTCRITERION -> {
                    final String commentText = criterion.getCommentCriterion().getCommentText();
                    if (! commentText.isBlank()) {
                        final Bson typeFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_TYPE, ANNOTATION_TYPE_COMMENT);
                        final Bson commentFilter = Filters.text(commentText);
                        final Bson criterionFilter = and(typeFilter, commentFilter);
                        criteriaFilterList.add(criterionFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryAnnotations unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryAnnotationsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryAnnotations filter: " + queryFilter.toString());

        final MongoCursor<AnnotationDocument> resultCursor = mongoCollectionAnnotations
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_ANNOTATION_ID))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryAnnotations received null cursor from mongodb.find");
        }

        return resultCursor;
    }

}
