package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataResponse;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;

public class MetadataResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryMetadataRequest request;
    private final StreamObserver<QueryMetadataResponse> responseObserver;

    public MetadataResponseDispatcher(
            StreamObserver<QueryMetadataResponse> responseObserver, QueryMetadataRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<Document> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "metadata query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryMetadataResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("metadata query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryMetadataResponseEmpty(this.responseObserver);
            return;
        }

        QueryMetadataResponse.MetadataResult.Builder metadataResultBuilder =
                QueryMetadataResponse.MetadataResult.newBuilder();
        
        while (cursor.hasNext()) {
            // add grpc object for each document in cursor
            
            final Document metadataDocument = cursor.next();
            
            final QueryMetadataResponse.MetadataResult.PvInfo.Builder pvInfoBuilder =
                    QueryMetadataResponse.MetadataResult.PvInfo.newBuilder();
            
            pvInfoBuilder.setPvName((String)metadataDocument.get(BsonConstants.BSON_KEY_PV_NAME));

            final Integer lastDataTypeCase =
                    (Integer) metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_DATA_TYPE_CASE);
            if (lastDataTypeCase != null) {
                pvInfoBuilder.setLastBucketDataTypeCase(lastDataTypeCase);
            }

            final String lastDataType = (String) metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_DATA_TYPE);
            if (lastDataType != null) {
                pvInfoBuilder.setLastBucketDataType(lastDataType);
            }

            // set sampling clock details
            final SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
            samplingClockBuilder.setPeriodNanos((Long)metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD));
            samplingClockBuilder.setCount((Integer)metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT));
            pvInfoBuilder.setLastSamplingClock(samplingClockBuilder);
            
            final Date firstTimeDate = (Date) metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);
            final Instant firstTimeInstant = firstTimeDate.toInstant();
            final Timestamp.Builder firstTimeBuilder = Timestamp.newBuilder();
            firstTimeBuilder.setEpochSeconds(firstTimeInstant.getEpochSecond());
            firstTimeBuilder.setNanoseconds(firstTimeInstant.getNano());
            firstTimeBuilder.build();
            pvInfoBuilder.setFirstTimestamp(firstTimeBuilder);

            final Date lastTimeDate = (Date) metadataDocument.get(BsonConstants.BSON_KEY_BUCKET_LAST_TIME);
            final Instant lastTimeInstant = lastTimeDate.toInstant();
            final Timestamp.Builder lastTimeBuilder = Timestamp.newBuilder();
            lastTimeBuilder.setEpochSeconds(lastTimeInstant.getEpochSecond());
            lastTimeBuilder.setNanoseconds(lastTimeInstant.getNano());
            lastTimeBuilder.build();
            pvInfoBuilder.setLastTimestamp(lastTimeBuilder);

            pvInfoBuilder.build();
            metadataResultBuilder.addPvInfos(pvInfoBuilder);
        }

        // send response and close response stream
        final QueryMetadataResponse.MetadataResult metadataResult = metadataResultBuilder.build();
        QueryServiceImpl.sendQueryMetadataResponse(metadataResult, this.responseObserver);
    }
}
