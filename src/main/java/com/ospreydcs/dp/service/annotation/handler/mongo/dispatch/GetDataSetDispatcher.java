package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.GetDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetDataSetResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetDataSetDispatcher extends Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<GetDataSetResponse> responseObserver;
    private final GetDataSetRequest request;

    public GetDataSetDispatcher(
            StreamObserver<GetDataSetResponse> responseObserver,
            GetDataSetRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetDataSetResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetDataSetResponseError(errorMsg, responseObserver);
    }

    public void handleResult(DataSetDocument document) {
        if (document == null) {
            final String msg = "no DataSet record found for id: " + request.getDataSetId();
            AnnotationServiceImpl.sendGetDataSetResponseReject(msg, responseObserver);
            return;
        }

        // Contain conversion failures: a malformed stored document must produce a reportable error,
        // never an unchecked throw that terminates the response stream with no payload.
        final DataSet dataSet;
        try {
            dataSet = document.toDataSet();
        } catch (RuntimeException ex) {
            final String errorMsg = "error building DataSet from document id "
                    + request.getDataSetId() + ": " + ex.getMessage();
            logger.error("handleResult conversion error: {}", ex.getMessage(), ex);
            AnnotationServiceImpl.sendGetDataSetResponseError(errorMsg, responseObserver);
            return;
        }
        AnnotationServiceImpl.sendGetDataSetResponseSuccess(dataSet, responseObserver);
    }
}
