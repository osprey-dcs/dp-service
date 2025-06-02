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

import java.util.List;

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

//    private static SubscribeDataEventResponse subscribeDataEventResponse(
//            SubscribeDataEventResponse.SubscribeDataEventResult result
//    ) {
//        return SubscribeDataEventResponse.newBuilder()
//                .setResponseTime(TimestampUtility.getTimestampNow())
//                .setSubscribeDataEventResult(result)
//                .build();
//    }
//
//    private static SubscribeDataEventResponse subscribeDataEventResponseConditionEvent(
//            String pvName,
//            Timestamp dataTimestamp,
//            DataValue dataValue
//    ) {
//        final SubscribeDataEventResponse.SubscribeDataEventResult.ConditionEvent conditionEvent =
//                SubscribeDataEventResponse.SubscribeDataEventResult.ConditionEvent.newBuilder()
//                        .setPvName(pvName)
//                        .setTimestamp(dataTimestamp)
//                        .setDataValue(dataValue)
//                        .build();
//
//        final SubscribeDataEventResponse.SubscribeDataEventResult result =
//                SubscribeDataEventResponse.SubscribeDataEventResult.newBuilder()
//                        .setConditionEvent(conditionEvent)
//                        .build();
//
//        return subscribeDataEventResponse(result);
//    }
//
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

//    public static void sendSubscribeDataEventResponseConditionEvent(
//            String pvName,
//            Timestamp dataTimestamp,
//            DataValue dataValue,
//            StreamObserver<SubscribeDataEventResponse> responseObserver
//    ) {
//        final SubscribeDataEventResponse response
//                = subscribeDataEventResponseConditionEvent(pvName, dataTimestamp, dataValue);
//        responseObserver.onNext(response);
//    }

    public static ValidationResult validateSubscribeDataEventRequest(
            SubscribeDataEventRequest request
    ) {
        // validate common request fields
        if (request.getTriggersList().isEmpty()) {
            return new ValidationResult(
                    true,
                    "SubscribeDataEventRequest.triggers must be specified");
        }

        // validate each PvConditionTrigger
        for (SubscribeDataEventRequest.PvConditionTrigger pvConditionTrigger : request.getTriggersList()) {

            // check that name is specified
            if (pvConditionTrigger.getPvName().isBlank()) {
                return new ValidationResult(
                        true,
                        "SubscribeDataEventRequest PvConditionTrigger.pvName must be specified");
            }

            // check that condition is specified
            if (pvConditionTrigger.getCondition() ==
                    SubscribeDataEventRequest.PvConditionTrigger.PvCondition.PV_CONDITION_UNSPECIFIED
            ) {
                return new ValidationResult(
                        true,
                        "SubscribeDataEventRequest PvConditionTrigger.condition must be specified");
            }

            // TODO: how to validate that specified pvName is valid

            // TODO: how to validate that PvConditionTrigger value is appropriate for specified pvName and condition,
            // given that we don't know type of pvName
        }

        // validate operation
        {
            final SubscribeDataEventRequest.DataEventOperation dataEventOperation = request.getOperation();

            // validate targetPvs list not empty
            final List<String> targetPvs = dataEventOperation.getTargetPvsList();
            if (targetPvs.isEmpty()) {
                return new ValidationResult(
                        true,
                        "SubscribeDataEventRequest DataEventOperation.targetPvs list must not be empty");
            }

            // validate each targetPv
            for (String targetPv : targetPvs) {
                if (targetPv.isBlank()) {
                    return new ValidationResult(
                            true,
                            "SubscribeDataEventRequest DataEventOperation.targetPvs contains empty string");
                }

                // TODO: how to validate that specified targetPv name is valid?
            }

            // validate DataEventWindow
            {
                if (!dataEventOperation.hasWindow()) {
                    return new ValidationResult(
                            true,
                            "SubscribeDataEventRequest DataEventOperation.targetPvs contains empty string");
                }

                final SubscribeDataEventRequest.DataEventOperation.DataEventWindow dataEventWindow =
                        dataEventOperation.getWindow();

                // validate window type parameters
                switch (dataEventWindow.getTypeCase()) {

                    // validate DataEventWindow type TimeInterval
                    case TIMEINTERVAL -> {

                        final SubscribeDataEventRequest.DataEventOperation.DataEventWindow.TimeInterval timeInterval =
                                dataEventWindow.getTimeInterval();

                        // offset value of zero is allowed, it indicates to start data capture from trigger time

                        // validate that duration is greater than zero
                        if (timeInterval.getDuration() == 0L) {
                            return new ValidationResult(
                                    true,
                                    "SubscribeDataEventRequest.DataEventOperation.DataEventWindow TimeInterval.duration must be specified");
                        }

                        // TODO: do we need to check that duration value is "reasonable", whatever that means?
                    }

                    // validate DataEventWindow type SampleCount
                    case SAMPLECOUNT -> {

                        final SubscribeDataEventRequest.DataEventOperation.DataEventWindow.SampleCount sampleCount =
                                dataEventWindow.getSampleCount();

                        // offset value of zero is allowed, it indicates to start data capture from trigger time

                        // validate that size is greater than zero
                        if (sampleCount.getSize() == 0L) {
                            return new ValidationResult(
                                    true,
                                    "SubscribeDataEventRequest.DataEventOperation.DataEventWindow SampleCount.size must be specified");
                        }

                    }

                    default -> {
                        return new ValidationResult(
                                true,
                                "SubscribeDataEventRequest.DataEventOperation DataEventWindow.type must be specified");
                    }
                }
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
                "id: {} subscribeDataEvent request received", responseObserver.hashCode());

        // validate request
        final ValidationResult validationResult = validateSubscribeDataEventRequest(request);
        if (validationResult.isError) {
            sendSubscribeDataEventResponseReject(validationResult.msg, responseObserver);
            return;
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
