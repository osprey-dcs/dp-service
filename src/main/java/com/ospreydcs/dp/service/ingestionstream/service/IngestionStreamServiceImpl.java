package com.ospreydcs.dp.service.ingestionstream.service;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.*;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.service.request.SubscribeDataEventRequestObserver;
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

        return SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAck(result)
                .build();
    }

    private static SubscribeDataEventResponse subscribeDataEventResponseEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataValue dataValue
    ) {
        final SubscribeDataEventResponse.Event event =
                SubscribeDataEventResponse.Event.newBuilder()
                        .setEventTime(triggerTimestamp)
                        .setTrigger(trigger)
                        .setDataValue(dataValue)
                        .build();

        return SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setEvent(event)
                .build();
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

    public static void sendSubscribeDataEventResponseEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataValue dataValue,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response
                = subscribeDataEventResponseEvent(triggerTimestamp, trigger, dataValue);
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataEventResponseEventData(
            SubscribeDataEventResponse.EventData eventData,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventResponse response = SubscribeDataEventResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setEventData(eventData)
                .build();
        responseObserver.onNext(response);
    }

    @Override
    public StreamObserver<SubscribeDataEventRequest> subscribeDataEvent(
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        return new SubscribeDataEventRequestObserver(responseObserver, handler);
    }
}
