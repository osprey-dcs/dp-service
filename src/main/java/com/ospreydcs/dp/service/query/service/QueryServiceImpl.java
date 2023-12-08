package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Date;

public class QueryServiceImpl extends DpQueryServiceGrpc.DpQueryServiceImplBase {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int TIMEOUT_STREAM_FINISH_MINUTES = 1;

    private QueryHandlerInterface handler;

    public boolean init(QueryHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            LOGGER.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            LOGGER.error("handler.start failed");
        }
        return true;
    }

    public void fini() {
        if (handler != null) {
            handler.stop();
            handler.fini();
            handler = null;
        }
    }

    public static QueryDataResponse queryResponseReject(QueryDataByTimeRequest request, String msg, RejectDetails.RejectReason reason) {
        RejectDetails rejectDetails = RejectDetails.newBuilder()
                .setRejectReason(reason)
                .setMessage(msg)
                .build();
        QueryDataResponse response = QueryDataResponse.newBuilder()
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setRejectDetails(rejectDetails)
                .build();
        return response;
    }

    public void queryDataByTime(QueryDataByTimeRequest request, StreamObserver<QueryDataResponse> responseObserver) {

        LOGGER.info("queryDataByTime columnName: {} startSeconds: {} endSeconds: {}",
                request.getColumnName(),
                request.getStartTime().getEpochSeconds(),
                request.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = handler.validateQueryDataByTimeRequest(request);
        boolean validationError = false;
        String validationMsg = "";

        if (validationResult.isError) {
            // send reject if request is invalid
            validationError = true;
            validationMsg = validationResult.msg;
            QueryDataResponse rejectResponse = queryResponseReject(
                    request, validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);
            responseObserver.onNext(rejectResponse);

        } else {
            // otherwise handle request
            HandlerQueryRequest handlerQueryRequest = new HandlerQueryRequest(request, responseObserver);
            handler.handleQueryRequest(handlerQueryRequest);
        }

        // close response stream
        responseObserver.onCompleted();
    }

}
