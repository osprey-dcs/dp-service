package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryBenchmarkResponseObserver implements StreamObserver<QueryResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    final private int streamNumber;
    final private QueryBenchmarkBase.QueryTaskParams params;
    final public CountDownLatch finishLatch;
    final private QueryBenchmarkBase.QueryTask task;
    protected AtomicBoolean isError = new AtomicBoolean(false);
    protected AtomicInteger dataValuesReceived = new AtomicInteger(0);
    protected AtomicInteger dataBytesReceived = new AtomicInteger(0);
    protected AtomicInteger grpcBytesReceived = new AtomicInteger(0);
    private AtomicInteger numResponsesReceived = new AtomicInteger(0);
    private AtomicInteger numBucketsReceived = new AtomicInteger(0);

    public QueryBenchmarkResponseObserver(
            int streamNumber,
            QueryBenchmarkBase.QueryTaskParams params,
            CountDownLatch finishLatch,
            QueryBenchmarkBase.QueryTask task
    ) {
        this.streamNumber = streamNumber;
        this.params = params;
        this.finishLatch = finishLatch;
        this.task = task;
    }

    protected void verifyResponse(QueryResponse response) {
        task.onResponse(response);
    }

    protected void onAssertionError(AssertionError assertionError) {
        // empty override
    }

    protected void onAdditionalBuckets() {
        // empty override
    }

    @Override
    public void onNext(QueryResponse response) {

        if (finishLatch.getCount() == 0) {
            return;
        }

        final String responseType = response.getResponseType().name();
        logger.trace("stream: {} received response type: {}", streamNumber, responseType);

        boolean success = true;
        String msg = "";

        if (response.hasQueryReject()) {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber
                    + " received reject with message: " + response.getQueryReject().getMessage();
            logger.error(msg);

        } else if (response.hasQueryReport()) {

            QueryResponse.QueryReport report = response.getQueryReport();

            if (report.hasBucketData()) {

                grpcBytesReceived.getAndAdd(response.getSerializedSize());
                numResponsesReceived.incrementAndGet();

                QueryResponse.QueryReport.BucketData queryData = report.getBucketData();
                int numResultBuckets = queryData.getDataBucketsCount();
                logger.trace("stream: {} received data result numBuckets: {}", streamNumber, numResultBuckets);

                for (QueryResponse.QueryReport.BucketData.DataBucket bucket : queryData.getDataBucketsList()) {
                    int dataValuesCount = bucket.getDataColumn().getDataValuesCount();
//                        LOGGER.trace(
//                                "stream: {} bucket column: {} startTime: {} numValues: {}",
//                                streamNumber,
//                                bucket.getDataColumn().getName(),
//                                GrpcUtility.dateFromTimestamp(bucket.getSamplingInterval().getStartTime()),
//                                dataValuesCount);

                    dataValuesReceived.addAndGet(dataValuesCount);
                    dataBytesReceived.addAndGet(dataValuesCount * Double.BYTES);
                    numBucketsReceived.incrementAndGet();
                }

                // call hook for subclasses to add validation
                try {
                    verifyResponse(response);
                } catch (AssertionError assertionError) {
                    if (finishLatch.getCount() > 0) {
                        System.err.println("stream: " + streamNumber + " assertion error");
                        assertionError.printStackTrace(System.err);
                        isError.set(true);
                        finishLatch.countDown();
                        onAssertionError(assertionError);

                    }
                    return;
                }

            } else if (report.hasQueryStatus()) {
                final QueryResponse.QueryReport.QueryStatus status = report.getQueryStatus();

                if (status.getQueryStatusType()
                        == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR) {
                    isError.set(true);
                    success = false;
                    final String errorMsg = status.getStatusMessage();
                    msg = "stream: " + streamNumber + " received error response: " + errorMsg;
                    logger.error(msg);

                } else if (status.getQueryStatusType()
                        == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY) {
                    isError.set(true);
                    success = false;
                    msg = "stream: " + streamNumber + " query returned no data";
                    logger.error(msg);
                }

            } else {
                isError.set(true);
                success = false;
                msg = "stream: " + streamNumber + " received QueryReport with unexpected content";
                logger.error(msg);
            }

        } else {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber + " received unexpected response";
            logger.error(msg);
        }

        if (success) {

            final int numBucketsExpected = params.columnNames.size() * 60;
            final int numBucketsReceivedValue = numBucketsReceived.get();

            if  ( numBucketsReceivedValue < numBucketsExpected) {
                onAdditionalBuckets();

            } else {
                // otherwise signal that we are done
                logger.trace("stream: {} onNext received expected number of buckets", streamNumber);
                finishLatch.countDown();
            }

        } else {
            // something went wrong, signal that we are done
            isError.set(true);
            logger.error("stream: {} onNext unexpected error", streamNumber);
            finishLatch.countDown();
        }

    }

    @Override
    public void onError(Throwable t) {
        logger.error("stream: {} responseObserver.onError with msg: {}", streamNumber, t.getMessage());
        isError.set(true);
        if (finishLatch.getCount() > 0) {
            finishLatch.countDown();
        }
    }

    @Override
    public void onCompleted() {
        logger.trace("stream: {} responseObserver.onCompleted", streamNumber);
    }

}
