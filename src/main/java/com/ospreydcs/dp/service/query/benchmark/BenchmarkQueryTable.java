package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Benchmark client for {@code queryTable} in column format (issue #275). The whole result must fit
 * the outgoing message budget, so the default scenario keeps requests small; the cost it exercises
 * is table assembly (#274 D9).
 */
public class BenchmarkQueryTable extends QueryBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    public static class QueryTableTask extends QueryTask {

        public QueryTableTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }

        @Override
        public QueryTaskResult call() {
            final DpQueryServiceGrpc.DpQueryServiceBlockingStub stub = DpQueryServiceGrpc.newBlockingStub(channel);
            try {
                final QueryTableResponse response = stub.queryTable(QueryV2BenchmarkSupport.tableRequest(params));
                if (response.hasExceptionalResult()) {
                    logger.error("stream: {} queryTable failed: {}", params.streamNumber(),
                            response.getExceptionalResult().getMessage());
                    return new QueryTaskResult(false, 0, 0, 0);
                }
                long values = 0;
                for (DataColumn column : response.getTableResult().getColumnTable().getDataColumnsList()) {
                    values += column.getDataValuesCount();
                }
                logger.trace("stream: {} queryTable values: {}", params.streamNumber(), values);
                return resultRequiringData(
                        "queryTable", params.streamNumber(), values, response.getSerializedSize());
            } catch (RuntimeException ex) {
                logger.error("stream: {} queryTable exception: {}", params.streamNumber(), ex.getMessage(), ex);
                return new QueryTaskResult(false, 0, 0, 0);
            }
        }
    }

    @Override
    protected QueryTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QueryTableTask(channel, params);
    }

    public static void main(final String[] args) {
        // small requests: a queryTable result must fit one message (the default fixture is 1,000
        // samples/s per PV, so one PV over the 60 s window is 480 KB of values)
        final int[] totalNumPvsArray = {100};
        final int[] numPvsPerRequestArray = {1, 5};
        final int[] numThreadsArray = {5};

        BenchmarkQueryTable benchmark = new BenchmarkQueryTable();
        runBenchmark(benchmark, args, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }
}
