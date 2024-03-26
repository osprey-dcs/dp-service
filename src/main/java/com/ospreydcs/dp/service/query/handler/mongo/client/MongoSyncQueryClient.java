package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;
import static com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument.ANNOTATION_TYPE_COMMENT;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = querySpec.getBeginTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getBeginTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_BUCKET_NAME, querySpec.getPvNamesList());
        final Bson endTimeFilter =
                or(lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                and(eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                        lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endTimeNanos)));
        final Bson startTimeFilter =
                or(gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                                gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, startTimeNanos)));
        final Bson filter = and(columnNameFilter, endTimeFilter, startTimeFilter);

        logger.debug("executing query columns: " + querySpec.getPvNamesList()
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds);

        return mongoCollectionBuckets
                .find(filter)
                .sort(ascending(
                        BsonConstants.BSON_KEY_BUCKET_NAME,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS
                ))
                .cursor();
    }

    private MongoCursor<Document> executeQueryMetadata(Bson columnNameFilter) {

        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE,
                BsonConstants.BSON_KEY_BUCKET_NUM_SAMPLES,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_FREQUENCY
        ));

        Bson bucketSort = ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);

        logger.debug("executing getColumnInfo query: {}", columnNameFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(Document.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(columnNameFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort),
                                Aggregates.group(
                                        "$columnName",
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_BUCKET_NAME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_NAME),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_BUCKET_NUM_SAMPLES,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_NUM_SAMPLES),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_BUCKET_SAMPLE_FREQUENCY,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_FREQUENCY),
                                        Accumulators.first(
                                                // save the first time of the first document in group to the firstTime field
                                                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the last time of the last document to the lastTime field
                                                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME)
                                )
                        ));

//        aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

    @Override
    public MongoCursor<Document> executeQueryMetadata(Collection<String> pvNameList) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_BUCKET_NAME, pvNameList);
        return executeQueryMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<Document> executeQueryMetadata(String pvNamePatternString) {
        final Pattern pvNamePattern = Pattern.compile(pvNamePatternString, Pattern.CASE_INSENSITIVE);
        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_BUCKET_NAME, pvNamePattern);
        return executeQueryMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<Document> executeQueryMetadata(QueryMetadataRequest request) {
        if (request.hasPvNameList()) {
            return executeQueryMetadata(request.getPvNameList().getPvNamesList());
        } else {
            return executeQueryMetadata(request.getPvNamePattern().getPattern());
        }
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
