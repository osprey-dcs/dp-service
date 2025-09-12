package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.SubscribeDataEventApiResult;
import com.ospreydcs.dp.grpc.v1.ingestionstream.*;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IngestionStreamClient extends ServiceApiClientBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public IngestionStreamClient(ManagedChannel channel) {
        super(channel);
    }

    public static final class SubscribeDataEventRequestParams {
        private final List<PvConditionTrigger> triggers;
        private final List<String> targetPvs;
        private final Long offset;
        private final Long duration;
        public boolean noWindow = false;

        public SubscribeDataEventRequestParams(
                List<PvConditionTrigger> triggers,
                List<String> targetPvs,
                Long offset,
                Long duration
        ) {
            this.triggers = triggers;
            this.targetPvs = targetPvs;
            this.offset = offset;
            this.duration = duration;
        }

        public List<PvConditionTrigger> triggers() {
            return triggers;
        }

        public List<String> targetPvs() {
            return targetPvs;
        }

        public Long offset() {
            return offset;
        }

        public Long duration() {
            return duration;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (SubscribeDataEventRequestParams) obj;
            return Objects.equals(this.triggers, that.triggers) &&
                    Objects.equals(this.targetPvs, that.targetPvs) &&
                    Objects.equals(this.offset, that.offset) &&
                    Objects.equals(this.duration, that.duration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(triggers, targetPvs, offset, duration);
        }

        @Override
        public String toString() {
            return "SubscribeDataEventRequestParams[" +
                    "triggers=" + triggers + ", " +
                    "targetPvs=" + targetPvs + ", " +
                    "offset=" + offset + ", " +
                    "duration=" + duration + ']';
        }

        }

    public record SubscribeDataEventCall(
            StreamObserver<SubscribeDataEventRequest> requestObserver,
            SubscribeDataEventResponseObserver responseObserver
    ) {
    }
    
    public static class SubscribeDataEventResponseObserver implements StreamObserver<SubscribeDataEventResponse> {

        // instance variables
        CountDownLatch ackLatch = null;
        CountDownLatch closeLatch = null;
        private final int eventListSizeLimit;
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribeDataEventResponse.Event> eventList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public SubscribeDataEventResponseObserver(int eventListSizeLimit) {
            this.ackLatch = new CountDownLatch(1);
            this.closeLatch = new CountDownLatch(1);
            this.eventListSizeLimit = eventListSizeLimit;
        }

        public void awaitAckLatch() {
            try {
                final boolean await = ackLatch.await(1, TimeUnit.MINUTES);
                if ( ! await) {
                    final String errorMsg = "timed out waiting for ackLatch";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for ackLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public void awaitCloseLatch() {
            try {
                final boolean await = closeLatch.await(1, TimeUnit.MINUTES);
                if ( ! await) {
                    final String errorMsg = "timed out waiting for closeLatch";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for closeLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public List<SubscribeDataEventResponse.Event> getEventList() {
            return eventList;
        }

        public boolean isError() {
            return isError.get();
        }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        private void addEvent(SubscribeDataEventResponse.Event event) {
            eventList.add(event);
            while (eventList.size() > eventListSizeLimit) {
                eventList.remove(0);
            }
        }

        @Override
        public void onNext(SubscribeDataEventResponse response) {

            switch (response.getResultCase()) {

                case EXCEPTIONALRESULT -> {
                    final String errorMsg = response.getExceptionalResult().getMessage();
                    System.err.println("SubscribeDataEventResponseOberver received ExceptionalResult: " + errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    ackLatch.countDown();
                }

                case ACK -> {
                    // decrement ackLatch for ack response
                    System.err.println("SubscribeDataEventResponseOberver received ack");
                    ackLatch.countDown();
                }

                case EVENT -> {
                    addEvent(response.getEvent());
                }

                case EVENTDATA -> {
                    // ignore EVENTDATA messages
                }

                case RESULT_NOT_SET -> {
                    final String errorMsg = "SubscribeDataEventResponse result not set";
                    logger.error(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            final String errorMsg = "onError status: " + status;
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }

        @Override
        public void onCompleted() {
            System.out.println("SubscribeDataEventResponseObserver onCompleted");
            closeLatch.countDown();
        }

    }

    private static SubscribeDataEventRequest buildSubscribeDataEventRequest(
            SubscribeDataEventRequestParams requestParams
    ) {
        SubscribeDataEventRequest.NewSubscription.Builder newSubscriptionBuilder =
                SubscribeDataEventRequest.NewSubscription.newBuilder();

        // add triggers to request
        for (PvConditionTrigger trigger : requestParams.triggers) {
            newSubscriptionBuilder.addTriggers(trigger);
        }

        // add DataEventOperation to request
        if (requestParams.targetPvs() != null) {
            DataEventOperation dataEventOperation;

            if (requestParams.noWindow) {
                // causes request to be built without DataEventWindow for reject testing
                dataEventOperation = DataEventOperation.newBuilder()
                        .addAllTargetPvs(requestParams.targetPvs())
                        .build();

            } else {
                // regular request handling
                DataEventOperation.DataEventWindow.TimeInterval timeInterval =
                        DataEventOperation.DataEventWindow.TimeInterval.newBuilder()
                                .setOffset(requestParams.offset)
                                .setDuration(requestParams.duration)
                                .build();
                DataEventOperation.DataEventWindow dataEventWindow =
                        DataEventOperation.DataEventWindow.newBuilder()
                                .setTimeInterval(timeInterval)
                                .build();
                dataEventOperation = DataEventOperation.newBuilder()
                        .addAllTargetPvs(requestParams.targetPvs())
                        .setWindow(dataEventWindow)
                        .build();
            }
            newSubscriptionBuilder.setOperation(dataEventOperation);
        }

        newSubscriptionBuilder.build();

        return SubscribeDataEventRequest.newBuilder().setNewSubscription(newSubscriptionBuilder).build();
    }
    
    private static SubscribeDataEventRequest buildSubscribeDataEventCancelRequest() {

        final SubscribeDataEventRequest.CancelSubscription cancelSubscription =
                SubscribeDataEventRequest.CancelSubscription.newBuilder()
                .build();

        return SubscribeDataEventRequest.newBuilder()
                .setCancelSubscription(cancelSubscription)
                .build();
    }

    public SubscribeDataEventApiResult sendSubscribeDataEvent(
            SubscribeDataEventRequest request,
            int eventListSizeLimit
    ) {
        final DpIngestionStreamServiceGrpc.DpIngestionStreamServiceStub asyncStub =
                DpIngestionStreamServiceGrpc.newStub(channel);

        final SubscribeDataEventResponseObserver responseObserver =
                new SubscribeDataEventResponseObserver(eventListSizeLimit);

        // invoke subscribeDataEvent() API method, get handle to request stream
        StreamObserver<SubscribeDataEventRequest> requestObserver = asyncStub.subscribeDataEvent(responseObserver);

        // send request message in request stream
        new Thread(() -> {
            requestObserver.onNext(request);
        }).start();

        // wait for ack response
        responseObserver.awaitAckLatch();

        if (responseObserver.isError()) {
            return new SubscribeDataEventApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new SubscribeDataEventApiResult(new SubscribeDataEventCall(requestObserver, responseObserver));
        }
    }

    public SubscribeDataEventApiResult subscribeDataEvent(
            SubscribeDataEventRequestParams requestParams,
            int eventListSizeLimit
    ) {
        final SubscribeDataEventRequest request = buildSubscribeDataEventRequest(requestParams);
        return sendSubscribeDataEvent(request, eventListSizeLimit);
    }

    public void cancelSubscribeDataEventCall(SubscribeDataEventCall subscribeDataEventCall) {

        final SubscribeDataEventRequest request = buildSubscribeDataEventCancelRequest();

        // send NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataEventCall.requestObserver().onNext(request);
        }).start();

        // wait for response stream to close
        final SubscribeDataEventResponseObserver responseObserver = subscribeDataEventCall.responseObserver();
        responseObserver.awaitCloseLatch();

    }

    public void closeSubscribeDataEventCall(SubscribeDataEventCall subscribeDataEventCall) {

        // close the request stream
        new Thread(subscribeDataEventCall.requestObserver()::onCompleted).start();

        // wait for response stream to close
        final SubscribeDataEventResponseObserver responseObserver = subscribeDataEventCall.responseObserver();
        responseObserver.awaitCloseLatch();
    }

}
