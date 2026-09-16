package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;

/**
 * Benchmark client for {@code queryBucketsStream} (issue #275). Its default scenario matches
 * {@link BenchmarkQueryDataStream}'s, so the two numbers are a direct comparison of the V2 and V1
 * bucket streams over the same fixture -- a regression check for the shared retrieval path.
 */
public class BenchmarkQueryBucketsStream extends QueryBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    public static class QueryBucketsStreamTask extends QueryTask {

        public QueryBucketsStreamTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }

        @Override
        public QueryTaskResult call() {
            final DpQueryServiceGrpc.DpQueryServiceBlockingStub stub = DpQueryServiceGrpc.newBlockingStub(channel);
            long values = 0;
            long grpcBytes = 0;
            int messages = 0;
            try {
                final Iterator<QueryBucketsResponse> responses =
                        stub.queryBucketsStream(QueryV2BenchmarkSupport.bucketsRequest(params, null));
                while (responses.hasNext()) {
                    final QueryBucketsResponse response = responses.next();
                    if (response.hasExceptionalResult()) {
                        logger.error("stream: {} queryBucketsStream failed: {}", params.streamNumber(),
                                response.getExceptionalResult().getMessage());
                        return new QueryTaskResult(false, 0, 0, 0);
                    }
                    grpcBytes += response.getSerializedSize();
                    for (DataBucket bucket : response.getBucketQueryResult().getDataBucketsList()) {
                        values += QueryV2BenchmarkSupport.countValues(bucket);
                    }
                    messages++;
                }
            } catch (RuntimeException ex) {
                logger.error("stream: {} queryBucketsStream exception: {}", params.streamNumber(), ex.getMessage(), ex);
                return new QueryTaskResult(false, 0, 0, 0);
            }
            logger.trace("stream: {} queryBucketsStream messages: {} values: {}", params.streamNumber(), messages, values);
            return new QueryTaskResult(true, values, values * Double.BYTES, grpcBytes);
        }
    }

    @Override
    protected QueryTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QueryBucketsStreamTask(channel, params);
    }

    public static void main(final String[] args) {
        // same scenario as BenchmarkQueryDataStream, for a direct V2-vs-V1 comparison
        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10};
        final int[] numThreadsArray = {5};

        BenchmarkQueryBucketsStream benchmark = new BenchmarkQueryBucketsStream();
        runBenchmark(benchmark, args, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }
}
