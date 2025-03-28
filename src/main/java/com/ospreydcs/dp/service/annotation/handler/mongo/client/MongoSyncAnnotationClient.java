package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

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

        // set createdAt field for document
        dataSetDocument.addCreationTime();

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

                case TEXTCRITERION -> {
                    final String text = criterion.getTextCriterion().getText();
                    if (! text.isBlank()) {
                        final Bson descriptionFilter = Filters.text(text);
                        criteriaFilterList.add(descriptionFilter);
                    }
                }

                case PVNAMECRITERION -> {
                    final String name = criterion.getPvNameCriterion().getName();
                    if (! name.isBlank()) {
                        final Bson descriptionFilter = Filters.in(BsonConstants.BSON_KEY_DATA_SET_BLOCK_PV_NAMES, name);
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
    public AnnotationDocument findAnnotation(String annotationId) {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has an annotationId, it already exists in the database.
        List<AnnotationDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionAnnotations.find(
                    eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findAnnotation: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (!matchingDocuments.isEmpty()) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
     }

    @Override
    public MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument) {

        logger.debug("inserting AnnotationDocument id: {}", annotationDocument.getId());

        // set createdAt field for document
        annotationDocument.addCreationTime();

        // insert AnnotationDocument to mongodb
        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionAnnotations.insertOne(annotationDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting AnnotationDocument: " + ex.getMessage();
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
                    // annotation id filter, combined with other filters by AND operator
                    final String annotationId = criterion.getIdCriterion().getId();
                    if ( ! annotationId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId));
                        globalFilterList.add(idFilter);
                    }
                }

                case OWNERCRITERION -> {
                    // owner id filter, combined with other filters by AND operator
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if ( ! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case DATASETSCRITERION -> {
                    // associated dataset id filter, combined with other filters by AND operator
                    final String dataSetId = criterion.getDataSetsCriterion().getDataSetId();
                    if ( ! dataSetId.isBlank()) {
                        Bson dataSetIdFilter = Filters.in(BsonConstants.BSON_KEY_ANNOTATION_DATASET_IDS, dataSetId);
                        globalFilterList.add(dataSetIdFilter);
                    }
                }

                case ANNOTATIONSCRITERION -> {
                    // assciated annotation ids filter, combined with other filters by OR operator
                    final String annotationId = criterion.getAnnotationsCriterion().getAnnotationId();
                    if ( ! annotationId.isBlank()) {
                        Bson associatedAnnotationFilter = Filters.in(BsonConstants.BSON_KEY_ANNOTATION_ANNOTATION_IDS, annotationId);
                        criteriaFilterList.add(associatedAnnotationFilter);
                    }
                }

                case TEXTCRITERION -> {
                    // name filter, combined with other filters by AND operator
                    final String nameText = criterion.getTextCriterion().getText();
                    if ( ! nameText.isBlank()) {
                        final Bson nameFilter = Filters.text(nameText);
                        globalFilterList.add(nameFilter);
                    }
                }

                case TAGSCRITERION -> {
                    // tags filter, combined with other filters by OR operator
                    final String tagValue = criterion.getTagsCriterion().getTagValue();
                    if ( ! tagValue.isBlank()) {
                        Bson tagsFilter = Filters.in(BsonConstants.BSON_KEY_TAGS, tagValue);
                        criteriaFilterList.add(tagsFilter);
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    // attributes filter, combined with other filters by OR operator
                    final String attributeKey = criterion.getAttributesCriterion().getKey();
                    final String attributeValue = criterion.getAttributesCriterion().getValue();
                    if ( ! attributeKey.isBlank() && ! attributeValue.isBlank()) {
                        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + attributeKey;
                        Bson attributesFilter = Filters.eq(mapKey, attributeValue);
                        criteriaFilterList.add(attributesFilter);
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

    @Override
    public MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument) {

        logger.debug("inserting CalculationsDocument id: {}", calculationsDocument.getId());

        // set createdAt field for document
        calculationsDocument.addCreationTime();

        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionCalculations.insertOne(calculationsDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting CalculationsDocument: " + ex.getMessage();
            logger.error(errorMsg);
        }

        return new MongoInsertOneResult(isError, errorMsg, result);
    }

    @Override
    public CalculationsDocument findCalculations(String calculationsId) {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a calculationsId, it already exists in the database.
        List<CalculationsDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionCalculations.find(
                    eq(BsonConstants.BSON_KEY_CALCULATIONS_ID, new ObjectId(calculationsId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findCalculations: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (!matchingDocuments.isEmpty()) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
    }

}
