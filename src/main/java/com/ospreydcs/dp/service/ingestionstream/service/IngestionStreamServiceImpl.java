package com.ospreydcs.dp.service.ingestionstream.service;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.DpIngestionStreamServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IngestionStreamServiceImpl
        extends DpIngestionStreamServiceGrpc.DpIngestionStreamServiceImplBase
{
    private static final Logger logger = LogManager.getLogger();

    private IngestionStreamHandlerInterface handler;

    public boolean init(IngestionStreamHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            logger.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            logger.error("handler.start failed");
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

    private static SubscribeDataEventResponse subscribeDataEventResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final SubscribeDataEventResponse response = SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static SubscribeDataEventResponse subscribeDataEventResponseReject(String msg) {

        return subscribeDataEventResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    private static SubscribeDataEventResponse subscribeDataEventResponseError(String msg) {

        return subscribeDataEventResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    private static SubscribeDataEventResponse subscribeDataEventResponseAck(
    ) {
        final SubscribeDataEventResponse.AckResult result =
                SubscribeDataEventResponse.AckResult.newBuilder()
                        .build();

        final SubscribeDataEventResponse response = SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAckResult(result)
                .build();

        return response;
    }

    private static SubscribeDataEventResponse subscribeDataEventResponse(
            SubscribeDataEventResponse.SubscribeDataEventResult result
    ) {
        return SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSubscribeDataEventResult(result)
                .build();
    }

    private static SubscribeDataEventResponse subscribeDataEventResponseConditionEvent(
            String pvName,
            Timestamp dataTimestamp,
            DataValue dataValue
    ) {
        final SubscribeDataEventResponse.SubscribeDataEventResult.ConditionEvent conditionEvent =
                SubscribeDataEventResponse.SubscribeDataEventResult.ConditionEvent.newBuilder()
                        .setPvName(pvName)
                        .setTimestamp(dataTimestamp)
                        .setDataValue(dataValue)
                        .build();

        final SubscribeDataEventResponse.SubscribeDataEventResult result =
                SubscribeDataEventResponse.SubscribeDataEventResult.newBuilder()
                        .setConditionEvent(conditionEvent)
                        .build();

        return subscribeDataEventResponse(result);
    }

    public static void sendSubscribeDataEventResponseReject(
            String msg, StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response = subscribeDataEventResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendSubscribeDataEventResponseError(
            String msg, StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response = subscribeDataEventResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendSubscribeDataEventResponseAck(
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response = subscribeDataEventResponseAck();
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataEventResponseConditionEvent(
            String pvName,
            Timestamp dataTimestamp,
            DataValue dataValue,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response
                = subscribeDataEventResponseConditionEvent(pvName, dataTimestamp, dataValue);
        responseObserver.onNext(response);
    }

    public static ValidationResult validateSubscribeDataEventRequest(
            SubscribeDataEventRequest request
    ) {
        // validate common request fields
        if (request.getPvNamesList().isEmpty()) {
            return new ValidationResult(
                    true,
                    "SubscribeDataEventRequest.pvNames must be specified");
        }

        // validate contents for various types of eventDef payloads
        switch (request.getDataEventDefCase()) {

            case CONDITIONEVENTDEF -> {
                // validate request containing ConditionEventDef payload
                SubscribeDataEventRequest.ConditionEventDef eventDef = request.getConditionEventDef();
                if (eventDef.getOperator() ==
                        SubscribeDataEventRequest.ConditionEventDef.ConditionOperator.CONDITION_OPERATOR_UNSPECIFIED) {
                    return new ValidationResult(
                            true,
                            "SubscribeDataEventRequest.ConditionEventDef.operator must be specified");
                }
            }

            case DATAEVENTDEF_NOT_SET -> {
                return new ValidationResult(
                        true,
                        "SubscribeDataEventRequest.dataEventDef must be specified");
            }
        }

        return new ValidationResult(false, "");
    }

    @Override
    public void subscribeDataEvent(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        logger.info(
                "id: {} subscribeDataEvent request received, dataEventDef: {}",
                responseObserver.hashCode(),
                request.getDataEventDefCase().name());

        // validate request
        final ValidationResult validationResult = validateSubscribeDataEventRequest(request);
        if (validationResult.isError) {
            sendSubscribeDataEventResponseReject(validationResult.msg, responseObserver);
        }
        
        // add a handler to remove subscription when client closes method connection
        ServerCallStreamObserver<SubscribeDataEventResponse> serverCallStreamObserver =
                (ServerCallStreamObserver<SubscribeDataEventResponse>) responseObserver;
        serverCallStreamObserver.setOnCancelHandler(
                () -> {
                    logger.trace("onCancelHandler id: {}", responseObserver.hashCode());
                    if (handler != null) {
                        handler.cancelDataEventSubscriptions(responseObserver);
                    }
                }
        );
        
        // dispatch to handler
        handler.handleSubscribeDataEvent(request, responseObserver);
    }
}
