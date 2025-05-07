package com.ospreydcs.dp.service.ingest.benchmark;

public class BenchmarkIngestDataStreamBytes extends BenchmarkUniStreamingIngestion {

    public static void main(final String[] args) {
        BenchmarkIngestDataStreamBytes benchmark = new BenchmarkIngestDataStreamBytes();
        runBenchmark(benchmark, true);
    }

}
