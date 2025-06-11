package com.ospreydcs.dp.service.ingestionstream.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.EventMonitorSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EventMonitorSubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final String pvName;
    private final EventMonitorSubscriptionManager subscriptionManager;
    private final IngestionStreamHandler handler;
    private final CountDownLatch ackLatch = new CountDownLatch(1);

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
//            isError.set(true);
//            errorMessageList.add(errorMsg);
        }
        return await;
    }

    @Override
    public void onNext(
            SubscribeDataResponse subscribeDataResponse
    ) {
        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                logger.trace("received exceptional result for pv: {}", pvName);
                // TODO: nothing else to do because stream should have also been closed? confirm
            }
            case ACKRESULT -> {
                logger.trace("received ack result for pv: {}", pvName);
                ackLatch.countDown();
            }
            case SUBSCRIBEDATARESULT -> {
                logger.trace("received subscribeData result for pv: {}", pvName);
                final EventMonitorJob job = new EventMonitorJob(
                        pvName, subscriptionManager, subscribeDataResponse.getSubscribeDataResult());
                handler.addJob(job);
            }
            case RESULT_NOT_SET -> {
                logger.trace("received result not set for pv: {}", pvName);
                // TODO: maybe we should do the same as onError / onCompleted here to clean up?
                // this is not expected but not sure what subsequent messaging would be if anything
            }
        }
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
