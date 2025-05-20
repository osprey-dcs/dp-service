package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.interfaces.ResultCursorInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataBidiStreamRequestObserver implements StreamObserver<QueryDataRequest> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryServiceImpl serviceImpl;
    private final StreamObserver<QueryDataResponse> responseObserver;
    private final QueryHandlerInterface handler;
    private ResultCursorInterface cursor = null;

    public QueryDataBidiStreamRequestObserver(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryHandlerInterface handler,
            QueryServiceImpl serviceImpl
    ) {
        this.responseObserver = responseObserver;
        this.handler = handler;
        this.serviceImpl = serviceImpl;
    }

    private void closeCursor() {
        if (this.cursor != null) {
            this.cursor.close();
        }
    }

    @Override
    public void onNext(QueryDataRequest request) {

        // handling depends on type of request
        switch (request.getRequestCase()) {

            case QUERYSPEC -> {
                // handle new query spec

                // log and validate request
                QueryDataRequest.QuerySpec querySpec =
                        serviceImpl.validateQueryDataRequest(
                                QueryServiceImpl.REQUEST_BIDI_STREAM, request, responseObserver);

                // handle new query request
                if (querySpec != null) {
                    this.cursor = handler.handleQueryDataBidiStream(querySpec, responseObserver);
                }
            }

            case CURSOROP -> {
                // handle cursor operation on query result

                switch (request.getCursorOp().getCursorOperationType()) {

                    case CURSOR_OP_NEXT -> {
                        logger.trace("handling cursor operation: CURSOR_OP_NEXT");

                        if (this.cursor != null) {
                            this.cursor.next();
                        }
                    }

                    case UNRECOGNIZED -> {
                        logger.error("unrecognized cursor operation requested");
                        responseObserver.onCompleted();
                        closeCursor();
                    }
                }

            }

            case REQUEST_NOT_SET -> {
                logger.error("unrecognized request case");
                responseObserver.onCompleted();
                closeCursor();
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("onError called with: {}", throwable.getMessage());
        responseObserver.onCompleted();
        closeCursor();
    }

    @Override
    public void onCompleted() {
        logger.trace("onCompleted");
        closeCursor();
    }

}
