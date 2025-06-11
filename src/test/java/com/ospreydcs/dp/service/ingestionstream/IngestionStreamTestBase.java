package com.ospreydcs.dp.service.ingestionstream;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

public class IngestionStreamTestBase {

    public record PvConditionTriggerParams(
            String pvName,
            PvConditionTrigger.PvCondition condition,
            DataValue value
    ) {
    }

    public record SubscribeDataEventRequestParams(
            List<PvConditionTriggerParams> triggerParamsList
    ) {
    }

    public record SubscribeDataEventCall(
            StreamObserver<SubscribeDataEventRequest> requestObserver,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
    }
    
    public static class SubscribeDataEventResponseObserver implements StreamObserver<SubscribeDataEventResponse> {

        // instance variables
        CountDownLatch ackLatch = null;
        CountDownLatch responseLatch = null;
        CountDownLatch closeLatch = null;
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribeDataEventResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public SubscribeDataEventResponseObserver(int expectedResponseCount) {
            this.ackLatch = new CountDownLatch(1);
            this.responseLatch = new CountDownLatch(expectedResponseCount);
            this.closeLatch = new CountDownLatch(1);
        }

        public void awaitAckLatch() {
            try {
                final boolean await = ackLatch.await(1, TimeUnit.MINUTES);
                if ( ! await) {
                    fail("timed out waiting for ack latch");
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for ackLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public void awaitResponseLatch() {
            try {
                final boolean await = responseLatch.await(1, TimeUnit.MINUTES);
                if ( ! await) {
                    fail("timed out waiting for response latch");
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for responseLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public List<SubscribeDataEventResponse> getResponseList() {
            return responseList;
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

        @Override
        public void onNext(SubscribeDataEventResponse response) {

            if (response.hasExceptionalResult()) {

                final String errorMsg = response.getExceptionalResult().getMessage();
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);

                if (ackLatch.getCount() > 0) {
                    // decrement ackLatch if initial response in stream
                    ackLatch.countDown();
                }

            } else if (response.hasAck()) {
                // decrement ackLatch for ack response
                ackLatch.countDown();

            } else {
                // decrement responseLatch for all other responses
                responseList.add(response);
                responseLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            final String errorMsg = "onError: " + status;
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }

        @Override
        public void onCompleted() {
            System.out.println("onCompleted");
            closeLatch.countDown();
        }

    }


    public static SubscribeDataEventRequest buildSubscribeDataEventRequest(
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams
    ) {
        SubscribeDataEventRequest.NewSubscription.Builder newSubscriptionBuilder =
                SubscribeDataEventRequest.NewSubscription.newBuilder();

        // add triggers to request
        for (PvConditionTriggerParams triggerParams : requestParams.triggerParamsList) {
            newSubscriptionBuilder.addTriggers(PvConditionTrigger.newBuilder()
                    .setPvName(triggerParams.pvName)
                    .setCondition(triggerParams.condition)
                    .setValue(triggerParams.value)
                    .build());
        }

        newSubscriptionBuilder.build();

        return SubscribeDataEventRequest.newBuilder().setNewSubscription(newSubscriptionBuilder).build();
    }

}
