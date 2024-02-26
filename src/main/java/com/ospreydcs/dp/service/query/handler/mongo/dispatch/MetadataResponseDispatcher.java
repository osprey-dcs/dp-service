package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataValueType;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataResponse;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
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
    private final QueryMetadataRequest.QuerySpec querySpec;
    private final StreamObserver<QueryMetadataResponse> responseObserver;

    public MetadataResponseDispatcher(
            StreamObserver<QueryMetadataResponse> responseObserver, QueryMetadataRequest.QuerySpec querySpec
    ) {
        this.querySpec = querySpec;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<Document> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "column info query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryResponseMetadataError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("column info query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryResponseMetadataEmpty(this.responseObserver);
            return;
        }

        QueryMetadataResponse.QueryResult.MetadataResult.Builder metadataResultBuilder =
                QueryMetadataResponse.QueryResult.MetadataResult.newBuilder();
        
        while (cursor.hasNext()) {
            // build ColumnInfo grpc object for each document in cursor
            
            final Document columnInfoDocument = cursor.next();
            
            final QueryMetadataResponse.QueryResult.MetadataResult.PvInfo.Builder pvInfoBuilder =
                    QueryMetadataResponse.QueryResult.MetadataResult.PvInfo.newBuilder();
            
            pvInfoBuilder.setPvName((String)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_NAME));
            pvInfoBuilder.setLastBucketDataType((String)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_DATA_TYPE));

            // set sampling clock details
            final SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
            samplingClockBuilder.setPeriodNanos((Long)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_SAMPLE_FREQUENCY));
            samplingClockBuilder.setCount((Integer)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_NUM_SAMPLES));
            pvInfoBuilder.setLastSamplingClock(samplingClockBuilder);
            
            final Date firstTimeDate = (Date) columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);
            final Instant firstTimeInstant = firstTimeDate.toInstant();
            final Timestamp.Builder firstTimeBuilder = Timestamp.newBuilder();
            firstTimeBuilder.setEpochSeconds(firstTimeInstant.getEpochSecond());
            firstTimeBuilder.setNanoseconds(firstTimeInstant.getNano());
            firstTimeBuilder.build();
            pvInfoBuilder.setFirstTimestamp(firstTimeBuilder);

            final Date lastTimeDate = (Date) columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_LAST_TIME);
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
        final QueryMetadataResponse.QueryResult.MetadataResult metadataResult = metadataResultBuilder.build();
        QueryServiceImpl.sendQueryResponseMetadata(metadataResult, this.responseObserver);
    }
}
