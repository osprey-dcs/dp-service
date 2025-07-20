package com.ospreydcs.dp.service.ingestionstream.handler.model;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.EventMonitorSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorSubscribeDataResponseJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventMonitorSubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final String pvName;
    private final EventMonitorSubscriptionManager subscriptionManager;
    private final IngestionStreamHandler handler;
    private final CountDownLatch ackLatch = new CountDownLatch(1);
    private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean isError = new AtomicBoolean(false);


    public EventMonitorSubscribeDataResponseObserver(
            final String pvName,
            final EventMonitorSubscriptionManager subscriptionManager,
            final IngestionStreamHandler handler
    ) {
        this.pvName = pvName;
        this.subscriptionManager = subscriptionManager;
        this.handler = handler;
    }

    public boolean awaitAckLatch() {
        boolean await = true;
        try {
            await = ackLatch.await(1, TimeUnit.MINUTES);
            if (!await) {
                final String errorMsg = "timed out waiting for ackLatch";
                System.err.println(errorMsg);
            }
        } catch (InterruptedException e) {
            final String errorMsg = "InterruptedException waiting for ackLatch";
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }
        return await;
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
    public void onNext(
            SubscribeDataResponse subscribeDataResponse
    ) {
        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                isError.set(true);
                errorMessageList.add(subscribeDataResponse.getExceptionalResult().getMessage());
//                if (subscribeDataResponse.getExceptionalResult().getExceptionalResultStatus() == ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT) {
//                    isReject.set(true);
//                } else {
//                    isError.set(true);
//                }
            }
            case ACKRESULT -> {
            }
            case SUBSCRIBEDATARESULT -> {
            }
            case RESULT_NOT_SET -> {
            }
        }

        // decrement ackLatch for initial response
        ackLatch.countDown();

        // dispatch response to subscription manager for handling
        final EventMonitorSubscribeDataResponseJob job = new EventMonitorSubscribeDataResponseJob(
                pvName, subscriptionManager, subscribeDataResponse);
        handler.addJob(job);
    }

    @Override
    public void onError(Throwable throwable) {
        // TODO - notify subscriptionManager of problem so it can clean up,
        // by either calling subscribeData() again or cleaning up subscription manager data structures
    }

    @Override
    public void onCompleted() {
        // TODO - notify subscriptionManager of problem so it can clean up,
        // by either calling subscribeData() again or cleaning up subscription manager data structures
    }
}
