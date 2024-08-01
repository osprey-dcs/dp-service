package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.service.common.benchmark.BenchmarkMongoClient;
import com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class BenchmarkIngestionGrpcServer extends IngestionGrpcServer {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    public static final int INGESTION_BENCHMARK_PORT = 60051;

    public BenchmarkIngestionGrpcServer(IngestionServiceImpl serviceImpl) {
        super(serviceImpl);
    }

    @Override
    protected int getPort_() {
        return INGESTION_BENCHMARK_PORT;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BenchmarkMongoClient.prepareBenchmarkDatabase();
        IngestionServiceImpl serviceImpl = new IngestionServiceImpl();
        final BenchmarkIngestionGrpcServer server = new BenchmarkIngestionGrpcServer(serviceImpl);
        server.start();
        server.blockUntilShutdown();
    }

}
