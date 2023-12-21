package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Date;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    public MongoCursor<BucketDocument> executeQuery(HandlerQueryRequest handlerQueryRequest) {

        final QueryRequest request = handlerQueryRequest.request;

        final Date startTimeDate = GrpcUtility.dateFromTimestamp(request.getStartTime());
        final Date endTimeDate = GrpcUtility.dateFromTimestamp(request.getEndTime());

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = request.getStartTime().getEpochSeconds();
        final long startTimeNanos = request.getStartTime().getNanoseconds();
        final long endTimeSeconds = request.getEndTime().getEpochSeconds();
        final long endTimeNanos = request.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_BUCKET_NAME, request.getColumnNamesList());
        final Bson endTimeFilter =
                or(lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                and(eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endTimeSeconds),
                        lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endTimeNanos)));
        final Bson startTimeFilter =
                or(gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                        and(eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, startTimeSeconds),
                                gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, startTimeNanos)));
        final Bson filter = and(columnNameFilter, endTimeFilter, startTimeFilter);

        LOGGER.debug("query column: " + request.getColumnNamesList()
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds);

        return mongoCollectionBuckets
                .find(filter)
                .sort(ascending(
                        BsonConstants.BSON_KEY_BUCKET_NAME,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS))
                .cursor();
    }
}
