package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;

/** Benchmark client for {@code querySamplesStream} (issue #275): one task consumes one stream to completion. */
public class BenchmarkQuerySamplesStream extends QueryBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    public static class QuerySamplesStreamTask extends QueryTask {

        public QuerySamplesStreamTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }

        @Override
        public QueryTaskResult call() {
            final DpQueryServiceGrpc.DpQueryServiceBlockingStub stub = DpQueryServiceGrpc.newBlockingStub(channel);
            long values = 0;
            long grpcBytes = 0;
            int messages = 0;
            try {
                final Iterator<QuerySamplesResponse> responses =
                        stub.querySamplesStream(QueryV2BenchmarkSupport.samplesRequest(params, null));
                while (responses.hasNext()) {
                    final QuerySamplesResponse response = responses.next();
                    if (response.hasExceptionalResult()) {
                        logger.error("stream: {} querySamplesStream failed: {}", params.streamNumber(),
                                response.getExceptionalResult().getMessage());
                        return new QueryTaskResult(false, 0, 0, 0);
                    }
                    grpcBytes += response.getSerializedSize();
                    values += QueryV2BenchmarkSupport.countValues(response.getSampleQueryResult().getColumnTable());
                    messages++;
                }
            } catch (RuntimeException ex) {
                logger.error("stream: {} querySamplesStream exception: {}", params.streamNumber(), ex.getMessage(), ex);
                return new QueryTaskResult(false, 0, 0, 0);
            }
            logger.trace("stream: {} querySamplesStream messages: {} values: {}", params.streamNumber(), messages, values);
            return resultRequiringData("querySamplesStream", params.streamNumber(), values, grpcBytes);
        }
    }

    @Override
    protected QueryTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QuerySamplesStreamTask(channel, params);
    }

    public static void main(final String[] args) {
        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10, 100};
        final int[] numThreadsArray = {5};

        BenchmarkQuerySamplesStream benchmark = new BenchmarkQuerySamplesStream();
        runBenchmark(benchmark, args, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }
}
