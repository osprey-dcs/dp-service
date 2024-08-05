package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.service.common.benchmark.BenchmarkMongoClient;
import com.ospreydcs.dp.service.query.server.QueryGrpcServer;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class BenchmarkQueryGrpcServer extends QueryGrpcServer {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    public static final int QUERY_BENCHMARK_PORT = 60052;

    public BenchmarkQueryGrpcServer(QueryServiceImpl serviceImpl) {
        super(serviceImpl);
    }

    @Override
    protected int getPort_() {
        return QUERY_BENCHMARK_PORT;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BenchmarkMongoClient.prepareBenchmarkDatabase();
        QueryServiceImpl serviceImpl = new QueryServiceImpl();
        final BenchmarkQueryGrpcServer server = new BenchmarkQueryGrpcServer(serviceImpl);
        server.start();
        server.blockUntilShutdown();
    }

}
