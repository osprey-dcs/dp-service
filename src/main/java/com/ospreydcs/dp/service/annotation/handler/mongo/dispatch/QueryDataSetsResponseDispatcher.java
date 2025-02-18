package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataSetsResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataSetsRequest request;
    private final StreamObserver<QueryDataSetsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;

    public QueryDataSetsResponseDispatcher(
            StreamObserver<QueryDataSetsResponse> responseObserver,
            QueryDataSetsRequest request,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    public void handleResult(MongoCursor<DataSetDocument> cursor) {
        
        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "query returned null cursor";
            logger.debug(msg);
            AnnotationServiceImpl.sendQueryDataSetsResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            logger.trace("query matched no data, cursor is empty");
            AnnotationServiceImpl.sendQueryDataSetsResponseEmpty(this.responseObserver);
            return;
        }

        final QueryDataSetsResponse.DataSetsResult.Builder queryDataSetsResultBuilder =
                QueryDataSetsResponse.DataSetsResult.newBuilder();

        while (cursor.hasNext()) {

            // add grpc object for each document in cursor
            final DataSetDocument dataSetDocument = cursor.next();

            // build grpc response and add to result
            final DataSet responseDataSet = dataSetDocument.toDataSet();
            queryDataSetsResultBuilder.addDataSets(responseDataSet);
        }

        // send response and close response stream
        final QueryDataSetsResponse.DataSetsResult queryDataSetsResult = queryDataSetsResultBuilder.build();
        AnnotationServiceImpl.sendQueryDataSetsResponse(queryDataSetsResult, this.responseObserver);
    }
    
}
