package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Override
    public DataSetDocument findDataSet(String dataSetId) {
        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a dataSetId, it already exists in the database.
        List<DataSetDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionDataSets.find(
                    eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(dataSetId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findDataSet: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

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
    public MongoCursor<DataSetDocument> executeQueryDataSets(QueryDataSetsRequest request) {

        // create query filter from request search criteria
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();

        final List<QueryDataSetsRequest.QueryDataSetsCriterion> criterionList = request.getCriteriaList();
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : criterionList) {
            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final String datasetId = criterion.getIdCriterion().getId();
                    if (! datasetId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(datasetId));
                        globalFilterList.add(idFilter);
                    }
                }

                case OWNERCRITERION -> {
                    // update ownerFilter from OwnerCriterion
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if (! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_DATA_SET_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case NAMECRITERION -> {
                    final String namePatternString = criterion.getNameCriterion().getNamePattern();
                    if (! namePatternString.isBlank()) {
                        final Pattern namePattern = Pattern.compile(namePatternString, Pattern.CASE_INSENSITIVE);
                        final Bson nameFilter = Filters.regex(BsonConstants.BSON_KEY_DATA_SET_NAME, namePattern);
                        criteriaFilterList.add(nameFilter);
                    }
                }

                case DESCRIPTIONCRITERION -> {
                    final String descriptionText = criterion.getDescriptionCriterion().getDescriptionText();
                    if (! descriptionText.isBlank()) {
                        final Bson descriptionFilter = Filters.text(descriptionText);
                        criteriaFilterList.add(descriptionFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryDataSets unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryDataSetsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryDataSets filter: " + queryFilter.toString());

        final MongoCursor<DataSetDocument> resultCursor = mongoCollectionDataSets
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_DATA_SET_ID))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryDataSets received null cursor from mongodb.find");
        }

        return resultCursor;
    }

    @Override
    public MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument) {

        logger.debug("inserting AnnotationDocument id: {}", annotationDocument.getId());

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

                case IDCRITERION -> {
                    final String annotationId = criterion.getIdCriterion().getId();
                    if (! annotationId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId));
                        globalFilterList.add(idFilter);
                    }
                }

                case OWNERCRITERION -> {
                    // update ownerFilter from OwnerCriterion
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if (! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case DATASETCRITERION -> {
                    final String dataSetId = criterion.getDataSetCriterion().getDataSetId();
                    if (! dataSetId.isBlank()) {
                        Bson dataSetIdFilter = Filters.in(BsonConstants.BSON_KEY_ANNOTATION_DATASET_IDS, dataSetId);
                        globalFilterList.add(dataSetIdFilter);
                    }
                }

                case COMMENTCRITERION -> {
                    final String commentText = criterion.getCommentCriterion().getCommentText();
                    if (! commentText.isBlank()) {
                        final Bson commentFilter = Filters.text(commentText);
                        criteriaFilterList.add(commentFilter);
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
