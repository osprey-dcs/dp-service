package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        final Date startTimeDate = GrpcUtility.dateFromTimestamp(querySpec.getBeginTime());
        final Date endTimeDate = GrpcUtility.dateFromTimestamp(querySpec.getEndTime());

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

    @Override
    public MongoCursor<Document> executeQueryMetadata(QueryMetadataRequest.QuerySpec querySpec) {

        Bson columnNameFilter;
        if (querySpec.hasPvNameList()) {
            columnNameFilter =
                    in(BsonConstants.BSON_KEY_BUCKET_NAME,
                            querySpec.getPvNameList().getPvNamesList());
        } else {
            final String columnNamePatternString = querySpec.getPvNamePattern().getPattern();
            final Pattern columnNamePattern = Pattern.compile(columnNamePatternString, Pattern.CASE_INSENSITIVE);
            columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_BUCKET_NAME, columnNamePattern);
        }

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

}
