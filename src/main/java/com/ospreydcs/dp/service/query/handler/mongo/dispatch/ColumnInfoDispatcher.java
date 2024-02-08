package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.google.protobuf.Message;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;

public class ColumnInfoDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec;
    private final StreamObserver<QueryResponse> responseObserver;

    public ColumnInfoDispatcher(
            StreamObserver<QueryResponse> responseObserver, QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec
            ) {
        this.columnInfoQuerySpec = columnInfoQuerySpec;
        this.responseObserver = responseObserver;
    }

    public void handleResult(MongoCursor<Document> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "column info query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("column info query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryResponseEmpty(this.responseObserver);
            return;
        }

        QueryResponse.QueryReport.ColumnInfoList.Builder columnInfoListBuilder =
                QueryResponse.QueryReport.ColumnInfoList.newBuilder();
        
        while (cursor.hasNext()) {
            // build ColumnInfo grpc object for each document in cursor
            
            final Document columnInfoDocument = cursor.next();
            
            final QueryResponse.QueryReport.ColumnInfoList.ColumnInfo.Builder columnInfoBuilder =
                    QueryResponse.QueryReport.ColumnInfoList.ColumnInfo.newBuilder();
            
            columnInfoBuilder.setColumnName((String)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_NAME));
            columnInfoBuilder.setDataType((String)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_DATA_TYPE));
            columnInfoBuilder.setSamplingClock((Long)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_SAMPLE_FREQUENCY));
            columnInfoBuilder.setBucketSize((Integer)columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_NUM_SAMPLES));
            
            final Date firstTimeDate = (Date) columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);
            final Instant firstTimeInstant = firstTimeDate.toInstant();
            final Timestamp.Builder firstTimeBuilder = Timestamp.newBuilder();
            firstTimeBuilder.setEpochSeconds(firstTimeInstant.getEpochSecond());
            firstTimeBuilder.setNanoseconds(firstTimeInstant.getNano());
            firstTimeBuilder.build();
            columnInfoBuilder.setFirstTime(firstTimeBuilder);

            final Date lastTimeDate = (Date) columnInfoDocument.get(BsonConstants.BSON_KEY_BUCKET_LAST_TIME);
            final Instant lastTimeInstant = lastTimeDate.toInstant();
            final Timestamp.Builder lastTimeBuilder = Timestamp.newBuilder();
            lastTimeBuilder.setEpochSeconds(lastTimeInstant.getEpochSecond());
            lastTimeBuilder.setNanoseconds(lastTimeInstant.getNano());
            lastTimeBuilder.build();
            columnInfoBuilder.setLastTime(lastTimeBuilder);

            columnInfoBuilder.build();
            columnInfoListBuilder.addColumnInfoList(columnInfoBuilder);
        }

        // send response and close response stream
        final QueryResponse.QueryReport.ColumnInfoList columnInfoList = columnInfoListBuilder.build();
        QueryServiceImpl.sendQueryResponseColumnInfo(columnInfoList, this.responseObserver);
    }
}
