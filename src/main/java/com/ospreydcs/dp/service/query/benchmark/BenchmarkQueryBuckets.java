package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Benchmark client for unary {@code queryBuckets} (issue #275): each task pages one query to completion. */
public class BenchmarkQueryBuckets extends QueryBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    public static class QueryBucketsTask extends QueryTask {

        public QueryBucketsTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }

        @Override
        public QueryTaskResult call() {
            final DpQueryServiceGrpc.DpQueryServiceBlockingStub stub = DpQueryServiceGrpc.newBlockingStub(channel);
            long values = 0;
            long grpcBytes = 0;
            int pages = 0;
            String pageToken = null;
            try {
                do {
                    final QueryBucketsResponse response = stub.queryBuckets(
                            QueryV2BenchmarkSupport.bucketsRequest(params, pageToken));
                    if (response.hasExceptionalResult()) {
                        logger.error("stream: {} queryBuckets page {} failed: {}", params.streamNumber(), pages,
                                response.getExceptionalResult().getMessage());
                        return new QueryTaskResult(false, 0, 0, 0);
                    }
                    grpcBytes += response.getSerializedSize();
                    for (DataBucket bucket : response.getBucketQueryResult().getDataBucketsList()) {
                        values += QueryV2BenchmarkSupport.countValues(bucket);
                    }
                    pageToken = response.getBucketQueryResult().getNextPageToken();
                    pages++;
                } while (!pageToken.isEmpty());
            } catch (RuntimeException ex) {
                logger.error("stream: {} queryBuckets exception: {}", params.streamNumber(), ex.getMessage(), ex);
                return new QueryTaskResult(false, 0, 0, 0);
            }
            logger.trace("stream: {} queryBuckets pages: {} values: {}", params.streamNumber(), pages, values);
            return new QueryTaskResult(true, values, values * Double.BYTES, grpcBytes);
        }
    }

    @Override
    protected QueryTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QueryBucketsTask(channel, params);
    }

    public static void main(final String[] args) {
        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10};
        final int[] numThreadsArray = {5};

        BenchmarkQueryBuckets benchmark = new BenchmarkQueryBuckets();
        runBenchmark(benchmark, args, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }
}
