package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.interfaces.ResultCursorInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryResponseCursorRequestStreamObserver implements StreamObserver<QueryRequest> {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // instance variables
    private final StreamObserver<QueryResponse> responseObserver;
    private final QueryHandlerInterface handler;
    private ResultCursorInterface cursor = null;

    public QueryResponseCursorRequestStreamObserver(
            StreamObserver<QueryResponse> responseObserver,
            QueryHandlerInterface handler
    ) {
        this.responseObserver = responseObserver;
        this.handler = handler;
    }

    @Override
    public void onNext(QueryRequest request) {

        // handling depends on type of request
        switch (request.getRequestCase()) {

            case QUERYSPEC -> {
                // handle new query spec

                // make sure request contains a query spec
                if (!request.hasQuerySpec()) {
                    String errorMsg = "QueryRequest does not contain a QuerySpec";
                    QueryServiceImpl.sendQueryResponseReject(
                            errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
                    return;
                }

                // extract query spec
                QueryRequest.QuerySpec querySpec = request.getQuerySpec();

                LOGGER.info("query columnNames: {} startSeconds: {} endSeconds: {}",
                        querySpec.getColumnNamesList(),
                        querySpec.getStartTime().getEpochSeconds(),
                        querySpec.getEndTime().getEpochSeconds());

                // validate request
                ValidationResult validationResult = handler.validateQuerySpec(querySpec);

                // send reject if request is invalid
                if (validationResult.isError) {
                    String validationMsg = validationResult.msg;
                    QueryServiceImpl.sendQueryResponseReject(
                            validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
                    return;
                }

                // otherwise handle new query request
                this.cursor = handler.handleQueryResponseCursor(querySpec, responseObserver);
            }

            case CURSOROP -> {
            }

            case REQUEST_NOT_SET -> {
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onCompleted() {
    }

}
