package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.DoubleBucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import io.opencensus.metrics.export.Distribution;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public MongoCursor<BucketDocument> executeQuery(QueryRequest.QuerySpec querySpec) {

        final Date startTimeDate = GrpcUtility.dateFromTimestamp(querySpec.getStartTime());
        final Date endTimeDate = GrpcUtility.dateFromTimestamp(querySpec.getEndTime());

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = querySpec.getStartTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getStartTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_BUCKET_NAME, querySpec.getColumnNamesList());
        final Bson endTimeFilter =
                or(lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                and(eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                        lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endTimeNanos)));
        final Bson startTimeFilter =
                or(gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                                gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, startTimeNanos)));
        final Bson filter = and(columnNameFilter, endTimeFilter, startTimeFilter);

        logger.debug("executing query columns: " + querySpec.getColumnNamesList()
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
    public MongoCursor<Document> getColumnInfo(QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec) {

        Bson columnNameFilter;
        if (columnInfoQuerySpec.hasColumnNameList()) {
            columnNameFilter =
                    in(BsonConstants.BSON_KEY_BUCKET_NAME,
                            columnInfoQuerySpec.getColumnNameList().getColumnNamesList());
        } else {
            final String columnNamePatternString = columnInfoQuerySpec.getColumnNamePattern().getPattern();
            final Pattern columnNamePattern = Pattern.compile(columnNamePatternString, Pattern.CASE_INSENSITIVE);
            columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_BUCKET_NAME, columnNamePattern);
        }

        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
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
                                                // confusing but we're saving the first time of the last document to the lastTime field
                                                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME)
                                )
                        ));

//        aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

}
