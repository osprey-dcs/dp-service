package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Benchmark client for unary {@code querySamples} (issue #275): each task pages one logical query
 * to completion, following {@code nextPageToken}, so a task measures a whole result rather than one
 * page. The cost it exercises is server-side assembly (per-sample table insertion, #274 D9) and the
 * time-sliced page retrieval (#274 D1–D3), which is why the default scenario also runs a wide
 * request: assembly cost is per column.
 */
public class BenchmarkQuerySamples extends QueryBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    public static class QuerySamplesTask extends QueryTask {

        public QuerySamplesTask(Channel channel, QueryTaskParams params) {
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
                    final QuerySamplesResponse response = stub.querySamples(
                            QueryV2BenchmarkSupport.samplesRequest(params, pageToken));
                    if (response.hasExceptionalResult()) {
                        logger.error("stream: {} querySamples page {} failed: {}", params.streamNumber(), pages,
                                response.getExceptionalResult().getMessage());
                        return new QueryTaskResult(false, 0, 0, 0);
                    }
                    grpcBytes += response.getSerializedSize();
                    values += QueryV2BenchmarkSupport.countValues(response.getSampleQueryResult().getColumnTable());
                    final String nextToken = response.getSampleQueryResult().getNextPageToken();
                    checkPagingProgress("querySamples", params.streamNumber(),
                            pageToken == null ? "" : pageToken, nextToken, pages);
                    pageToken = nextToken;
                    pages++;
                } while (!pageToken.isEmpty());
            } catch (RuntimeException ex) {
                logger.error("stream: {} querySamples exception: {}", params.streamNumber(), ex.getMessage(), ex);
                return new QueryTaskResult(false, 0, 0, 0);
            }
            logger.trace("stream: {} querySamples pages: {} values: {}", params.streamNumber(), pages, values);
            return resultRequiringData("querySamples", params.streamNumber(), values, grpcBytes);
        }
    }

    @Override
    protected QueryTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QuerySamplesTask(channel, params);
    }

    public static void main(final String[] args) {
        // the standard scenario plus a wide one: querySamples assembly cost is per column
        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10, 100};
        final int[] numThreadsArray = {5};

        BenchmarkQuerySamples benchmark = new BenchmarkQuerySamples();
        runBenchmark(benchmark, args, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }
}
