package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTableResponseObserver implements StreamObserver<QueryTableResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    final private int streamNumber;
    final private QueryBenchmarkBase.QueryDataRequestTaskParams params;
    final public CountDownLatch finishLatch;
    final private BenchmarkQueryDataTable.QueryResponseTableTask task;
    protected AtomicBoolean isError = new AtomicBoolean(false);

    // stats variables
    protected AtomicInteger dataValuesReceived = new AtomicInteger(0);
    protected AtomicInteger dataBytesReceived = new AtomicInteger(0);
    protected AtomicInteger grpcBytesReceived = new AtomicInteger(0);
    private AtomicInteger numResponsesReceived = new AtomicInteger(0);
    private AtomicInteger numColumnsReceived = new AtomicInteger(0);


    public QueryTableResponseObserver(
            int streamNumber,
            QueryBenchmarkBase.QueryDataRequestTaskParams params,
            CountDownLatch finishLatch,
            BenchmarkQueryDataTable.QueryResponseTableTask task
    ) {
        this.streamNumber = streamNumber;
        this.params = params;
        this.finishLatch = finishLatch;
        this.task = task;
    }

    protected void verifyResponse(QueryTableResponse response) {
        task.onResponse(response);
    }

    public void onNext(QueryTableResponse response) {

        if (finishLatch.getCount() == 0) {
            return;
        }

        logger.trace("stream: {} received response type: {}", streamNumber, response.getResultCase().toString());

        boolean success = true;
        String msg = "";

        if (response.hasExceptionalResult()) {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber
                    + " received exception with message: " + response.getExceptionalResult().getStatusMessage();
            logger.error(msg);

        } else if (response.hasTableResult()) {

            grpcBytesReceived.getAndAdd(response.getSerializedSize());
            numResponsesReceived.incrementAndGet();

            QueryTableResponse.TableResult dataTable = response.getTableResult();
            int numResultColumns = dataTable.getDataColumnsCount();
            logger.trace("stream: {} received DataTable numColumns: {}", streamNumber, numResultColumns);

            for (DataColumn column : dataTable.getDataColumnsList()) {
                int dataValuesCount = column.getDataValuesCount();
                dataValuesReceived.addAndGet(dataValuesCount);
                dataBytesReceived.addAndGet(dataValuesCount * Double.BYTES);
                numColumnsReceived.incrementAndGet();
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

                }
                return;
            }

        } else {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber + " received unexpected response";
            logger.error(msg);
        }

        if (success) {

            final int numColumnsExpected = params.columnNames.size();
            final int numColumnsReceivedValue = numColumnsReceived.get();

            if (numColumnsReceivedValue < numColumnsExpected) {
                isError.set(true);
                logger.error(
                        "stream: {} onNext number of columns received: {} less than expected: {}",
                        streamNumber, numColumnsReceivedValue, numColumnsExpected);

            } else {

                final int numValuesExpected = params.columnNames.size() * 1000 * 60;
                final int numValuesReceivedValue = dataValuesReceived.get();

                if (numValuesReceivedValue < numValuesExpected) {
                    isError.set(true);
                    logger.error("stream: {} onNext number of values received: {} less than expected: {}",
                            streamNumber, numColumnsReceivedValue, numColumnsExpected);

                } else {
                    // otherwise signal that we are done
                    logger.trace("stream: {} onNext received expected number of buckets", streamNumber);
                    finishLatch.countDown();
                }
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
