package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsResponse;
import com.ospreydcs.dp.grpc.v1.query.ProviderStats;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class QueryProviderStatsDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProviderStatsRequest request;
    private final StreamObserver<QueryProviderStatsResponse> responseObserver;

    public QueryProviderStatsDispatcher(
            StreamObserver<QueryProviderStatsResponse> responseObserver, QueryProviderStatsRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public static ProviderStats providerStatsFromDocument(
            ProviderMetadataQueryResultDocument providerMetadataDocument
    ) {
        final ProviderStats.Builder providerStatsBuilder =
                ProviderStats.newBuilder();

        providerStatsBuilder.setId(providerMetadataDocument.getId());

        providerStatsBuilder.addAllPvNames(providerMetadataDocument.getPvNames());

        final Instant firstTimeInstant = providerMetadataDocument.getFirstBucketTimestamp().toInstant();
        providerStatsBuilder.setFirstBucketTime(
                TimestampUtility.timestampFromSeconds(
                        firstTimeInstant.getEpochSecond(), firstTimeInstant.getNano()));

        final Instant lastTimeInstant = providerMetadataDocument.getLastBucketTimestamp().toInstant();
        providerStatsBuilder.setLastBucketTime(
                TimestampUtility.timestampFromSeconds(
                        lastTimeInstant.getEpochSecond(), lastTimeInstant.getNano()));

        providerStatsBuilder.setNumBuckets(providerMetadataDocument.getNumBuckets());

        return providerStatsBuilder.build();
    }

    public void handleResult(MongoCursor<ProviderMetadataQueryResultDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "providerStats query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryProviderStatsResponseError(msg, this.responseObserver);
            return;
        }

        QueryProviderStatsResponse.StatsResult.Builder providerStatsResultBuilder =
                QueryProviderStatsResponse.StatsResult.newBuilder();

        while (cursor.hasNext()) {
            // add protobuf object for each document in result cursor
            final ProviderMetadataQueryResultDocument providerMetadataDocument = cursor.next();
            providerStatsResultBuilder.addProviderStats(providerStatsFromDocument(providerMetadataDocument));
        }

        // send response and close response stream
        final QueryProviderStatsResponse.StatsResult statsResult = providerStatsResultBuilder.build();
        QueryServiceImpl.sendQueryProviderStatsResponse(statsResult, this.responseObserver);
    }

}
