package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsResponse;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Date;

public class QueryPvStatsDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryPvStatsRequest request;
    private final StreamObserver<QueryPvStatsResponse> responseObserver;

    public QueryPvStatsDispatcher(
            StreamObserver<QueryPvStatsResponse> responseObserver, QueryPvStatsRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<PvMetadataQueryResultDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "pv stats query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryPvStatsResponseError(msg, this.responseObserver);
            return;
        }

        QueryPvStatsResponse.StatsResult.Builder statsResultBuilder =
                QueryPvStatsResponse.StatsResult.newBuilder();

        while (cursor.hasNext()) {
            // add grpc object for each document in cursor

            final PvMetadataQueryResultDocument metadataDocument = cursor.next();

            final QueryPvStatsResponse.StatsResult.PvStats.Builder pvStatsBuilder =
                    QueryPvStatsResponse.StatsResult.PvStats.newBuilder();

            pvStatsBuilder.setPvName(metadataDocument.getPvName());
            pvStatsBuilder.setLastBucketId(metadataDocument.getLastBucketId());

            // last data type
            final String lastDataType = metadataDocument.getLastBucketDataType();
            if (lastDataType != null) {
                pvStatsBuilder.setLastBucketDataType(lastDataType);
            }

            // last data timestamps case and type
            final Integer lastDataTimestampsCase =
                    metadataDocument.getLastBucketDataTimestampsCase();
            if (lastDataTimestampsCase != null) {
                pvStatsBuilder.setLastBucketDataTimestampsCase(lastDataTimestampsCase);
            }
            final String lastDataTimestampsType =
                    metadataDocument.getLastBucketDataTimestampsType();
            if (lastDataTimestampsType != null) {
                pvStatsBuilder.setLastBucketDataTimestampsType(lastDataTimestampsType);
            }

            // set numBuckets
            pvStatsBuilder.setNumBuckets(metadataDocument.getNumBuckets());

            // set sampling clock details
            pvStatsBuilder.setLastBucketSampleCount(metadataDocument.getLastBucketSampleCount());
            pvStatsBuilder.setLastBucketSamplePeriod(metadataDocument.getLastBucketSamplePeriod());

            final Date firstTimeDate = metadataDocument.getFirstDataTimestamp();
            final Instant firstTimeInstant = firstTimeDate.toInstant();
            final Timestamp.Builder firstTimeBuilder = Timestamp.newBuilder();
            firstTimeBuilder.setEpochSeconds(firstTimeInstant.getEpochSecond());
            firstTimeBuilder.setNanoseconds(firstTimeInstant.getNano());
            firstTimeBuilder.build();
            pvStatsBuilder.setFirstDataTimestamp(firstTimeBuilder);

            final Date lastTimeDate = metadataDocument.getLastDataTimestamp();
            final Instant lastTimeInstant = lastTimeDate.toInstant();
            final Timestamp.Builder lastTimeBuilder = Timestamp.newBuilder();
            lastTimeBuilder.setEpochSeconds(lastTimeInstant.getEpochSecond());
            lastTimeBuilder.setNanoseconds(lastTimeInstant.getNano());
            lastTimeBuilder.build();
            pvStatsBuilder.setLastDataTimestamp(lastTimeBuilder);

            // set provider details
            pvStatsBuilder.setLastProviderId(metadataDocument.getLastProviderId());
            pvStatsBuilder.setLastProviderName(metadataDocument.getLastProviderName());

            pvStatsBuilder.build();
            statsResultBuilder.addPvStats(pvStatsBuilder);
        }

        // send response and close response stream
        final QueryPvStatsResponse.StatsResult statsResult = statsResultBuilder.build();
        QueryServiceImpl.sendQueryPvStatsResponse(statsResult, this.responseObserver);
    }
}
