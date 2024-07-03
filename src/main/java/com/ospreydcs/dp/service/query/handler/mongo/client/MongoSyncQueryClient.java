package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.MetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    private MongoCursor<BucketDocument> executeBucketDocumentQuery(
            Bson columnNameFilter,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos
    ) {
        final Bson endTimeFilter =
                or(lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                                lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endTimeNanos)));
        final Bson startTimeFilter =
                or(gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                                gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, startTimeNanos)));
        final Bson filter = and(columnNameFilter, endTimeFilter, startTimeFilter);

        logger.debug("executing query columns: " + columnNameFilter
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds);

        return mongoCollectionBuckets
                .find(filter)
                .sort(ascending(
                        BsonConstants.BSON_KEY_PV_NAME,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS
                ))
                .cursor();
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = querySpec.getBeginTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getBeginTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, querySpec.getPvNamesList());
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request) {
        
        final long startTimeSeconds = request.getBeginTime().getEpochSeconds();
        final long startTimeNanos = request.getBeginTime().getNanoseconds();
        final long endTimeSeconds = request.getEndTime().getEpochSeconds();
        final long endTimeNanos = request.getEndTime().getNanoseconds();

        // create name filter using either list of pv names, or pv name pattern
        Bson columnNameFilter = null;
        switch (request.getPvNameSpecCase()) {
            case PVNAMELIST -> {
                columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, request.getPvNameList().getPvNamesList());
            }
            case PVNAMEPATTERN -> {
                final Pattern pvNamePattern = Pattern.compile(
                        request.getPvNamePattern().getPattern(), Pattern.CASE_INSENSITIVE);
                columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
            }
            case PVNAMESPEC_NOT_SET -> {
                return null;
            }
        }

        // execute query
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    private MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(Bson columnNameFilter) {

        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_ID,
                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE_CASE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD
        ));

        Bson bucketSort = ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS);

        logger.debug("executing getColumnInfo query: {}", columnNameFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(MetadataQueryResultDocument.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(columnNameFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort), // sort here so that records are ordered for group opeator
                                Aggregates.group(
                                        "$" + BsonConstants.BSON_KEY_PV_NAME,
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_PV_NAME,
                                                "$" + BsonConstants.BSON_KEY_PV_NAME),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_ID,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_ID),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_DATA_TYPE_CASE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TYPE_CASE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_DATA_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_CASE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_SAMPLE_COUNT,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_METADATA_LAST_BUCKET_SAMPLE_PERIOD,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD),
                                        Accumulators.first(
                                                // save the first time of the first document in group to the firstTime field
                                                BsonConstants.BSON_KEY_METADATA_FIRST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the last time of the last document to the lastTime field
                                                BsonConstants.BSON_KEY_METADATA_LAST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME)
                                ),
                                Aggregates.sort(bucketSort) // sort again so result is sorted
                                ));

//        aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

    @Override
    public MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(Collection<String> pvNameList) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, pvNameList);
        return executeQueryMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(String pvNamePatternString) {
        final Pattern pvNamePattern = Pattern.compile(pvNamePatternString, Pattern.CASE_INSENSITIVE);
        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
        return executeQueryMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<MetadataQueryResultDocument> executeQueryMetadata(QueryMetadataRequest request) {
        if (request.hasPvNameList()) {
            return executeQueryMetadata(request.getPvNameList().getPvNamesList());
        } else {
            return executeQueryMetadata(request.getPvNamePattern().getPattern());
        }
    }

}
