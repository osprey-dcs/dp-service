package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataResponse;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class QueryProviderMetadataResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProviderMetadataRequest request;
    private final StreamObserver<QueryProviderMetadataResponse> responseObserver;

    public QueryProviderMetadataResponseDispatcher(
            StreamObserver<QueryProviderMetadataResponse> responseObserver, QueryProviderMetadataRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<ProviderMetadataQueryResultDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "providerMetadata query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryProviderMetadataResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("providerMetadata query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryProviderMetadataResponseEmpty(this.responseObserver);
            return;
        }

        QueryProviderMetadataResponse.MetadataResult.Builder providerMetadataResultBuilder =
                QueryProviderMetadataResponse.MetadataResult.newBuilder();

        while (cursor.hasNext()) {
            // add grpc object for each document in cursor

            final ProviderMetadataQueryResultDocument providerMetadataDocument = cursor.next();

            final QueryProviderMetadataResponse.MetadataResult.ProviderMetadata.Builder providerMetadataBuilder =
                    QueryProviderMetadataResponse.MetadataResult.ProviderMetadata.newBuilder();

            providerMetadataBuilder.setId(providerMetadataDocument.getId());
            
            providerMetadataBuilder.addAllPvNames(providerMetadataDocument.getPvNames());
            
            final Instant firstTimeInstant = providerMetadataDocument.getFirstBucketTimestamp().toInstant();
            providerMetadataBuilder.setFirstBucketTime(
                    TimestampUtility.timestampFromSeconds(
                            firstTimeInstant.getEpochSecond(), firstTimeInstant.getNano()));
            
            final Instant lastTimeInstant = providerMetadataDocument.getLastBucketTimestamp().toInstant();
            providerMetadataBuilder.setLastBucketTime(
                    TimestampUtility.timestampFromSeconds(
                            lastTimeInstant.getEpochSecond(), lastTimeInstant.getNano()));
            
            providerMetadataBuilder.setNumBuckets(providerMetadataDocument.getNumBuckets());
            providerMetadataResultBuilder.addProviderMetadatas(providerMetadataBuilder.build());
        }
        
        // send response and close response stream
        final QueryProviderMetadataResponse.MetadataResult metadataResult = providerMetadataResultBuilder.build();
        QueryServiceImpl.sendQueryProviderMetadataResponse(metadataResult, this.responseObserver);
    }

}
