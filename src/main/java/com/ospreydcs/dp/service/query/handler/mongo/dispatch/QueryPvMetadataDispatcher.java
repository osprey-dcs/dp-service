package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvMetadataResponse;
import com.ospreydcs.dp.service.common.bson.MetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Date;

public class QueryPvMetadataDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryPvMetadataRequest request;
    private final StreamObserver<QueryPvMetadataResponse> responseObserver;

    public QueryPvMetadataDispatcher(
            StreamObserver<QueryPvMetadataResponse> responseObserver, QueryPvMetadataRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<MetadataQueryResultDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "metadata query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryPvMetadataResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("metadata query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryPvMetadataResponseEmpty(this.responseObserver);
            return;
        }

        QueryPvMetadataResponse.MetadataResult.Builder metadataResultBuilder =
                QueryPvMetadataResponse.MetadataResult.newBuilder();
        
        while (cursor.hasNext()) {
            // add grpc object for each document in cursor
            
            final MetadataQueryResultDocument metadataDocument = cursor.next();
            
            final QueryPvMetadataResponse.MetadataResult.PvInfo.Builder pvInfoBuilder =
                    QueryPvMetadataResponse.MetadataResult.PvInfo.newBuilder();
            
            pvInfoBuilder.setPvName(metadataDocument.getPvName());
            pvInfoBuilder.setLastBucketId(metadataDocument.getLastBucketId());

            // last data type case and type
            final Integer lastDataTypeCase =
                    metadataDocument.getLastBucketDataTypeCase();
            if (lastDataTypeCase != null) {
                pvInfoBuilder.setLastBucketDataTypeCase(lastDataTypeCase);
            }
            final String lastDataType = metadataDocument.getLastBucketDataType();
            if (lastDataType != null) {
                pvInfoBuilder.setLastBucketDataType(lastDataType);
            }

            // last data timestamps case and type
            final Integer lastDataTimestampsCase =
                    metadataDocument.getLastBucketDataTimestampsCase();
            if (lastDataTimestampsCase != null) {
                pvInfoBuilder.setLastBucketDataTimestampsCase(lastDataTimestampsCase);
            }
            final String lastDataTimestampsType =
                    metadataDocument.getLastBucketDataTimestampsType();
            if (lastDataTimestampsType != null) {
                pvInfoBuilder.setLastBucketDataTimestampsType(lastDataTimestampsType);
            }

            // set sampling clock details
            pvInfoBuilder.setLastBucketSampleCount(metadataDocument.getLastBucketSampleCount());
            pvInfoBuilder.setLastBucketSamplePeriod(metadataDocument.getLastBucketSamplePeriod());

            final Date firstTimeDate = metadataDocument.getFirstDataTimestamp();
            final Instant firstTimeInstant = firstTimeDate.toInstant();
            final Timestamp.Builder firstTimeBuilder = Timestamp.newBuilder();
            firstTimeBuilder.setEpochSeconds(firstTimeInstant.getEpochSecond());
            firstTimeBuilder.setNanoseconds(firstTimeInstant.getNano());
            firstTimeBuilder.build();
            pvInfoBuilder.setFirstDataTimestamp(firstTimeBuilder);

            final Date lastTimeDate = metadataDocument.getLastDataTimestamp();
            final Instant lastTimeInstant = lastTimeDate.toInstant();
            final Timestamp.Builder lastTimeBuilder = Timestamp.newBuilder();
            lastTimeBuilder.setEpochSeconds(lastTimeInstant.getEpochSecond());
            lastTimeBuilder.setNanoseconds(lastTimeInstant.getNano());
            lastTimeBuilder.build();
            pvInfoBuilder.setLastDataTimestamp(lastTimeBuilder);

            pvInfoBuilder.build();
            metadataResultBuilder.addPvInfos(pvInfoBuilder);
        }

        // send response and close response stream
        final QueryPvMetadataResponse.MetadataResult metadataResult = metadataResultBuilder.build();
        QueryServiceImpl.sendQueryPvMetadataResponse(metadataResult, this.responseObserver);
    }
}
