package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.DataSetQueryResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataSetsDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataSetsRequest request;
    private final StreamObserver<QueryDataSetsResponse> responseObserver;

    public QueryDataSetsDispatcher(
            StreamObserver<QueryDataSetsResponse> responseObserver,
            QueryDataSetsRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQueryDataSetsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(DataSetQueryResult queryResult) {

        final QueryDataSetsResponse.DataSetsResult.Builder dataSetsResultBuilder =
                QueryDataSetsResponse.DataSetsResult.newBuilder();

        for (DataSetDocument dataSetDocument : queryResult.getDocuments()) {
            dataSetsResultBuilder.addDataSets(dataSetDocument.toDataSet());
        }

        dataSetsResultBuilder.setNextPageToken(
                queryResult.getNextPageToken() != null ? queryResult.getNextPageToken() : "");

        // send response and close response stream
        AnnotationServiceImpl.sendQueryDataSetsResponse(dataSetsResultBuilder.build(), this.responseObserver);
    }

}
