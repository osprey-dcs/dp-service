package com.ospreydcs.dp.service.query.benchmark;

public class BenchmarkQueryDataStreamBytes extends BenchmarkQueryDataStream {

    public static void main(final String[] args) {

//        final int[] totalNumPvsArray = {100, 500, 1000};
//        final int[] numPvsPerRequestArray = {1, 10, 25, 50};
//        final int[] numThreadsArray = {1, 3, 5, 7};

        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10};
        final int[] numThreadsArray = {5};

        BenchmarkQueryDataStreamBytes benchmark = new BenchmarkQueryDataStreamBytes();
        runBenchmark(benchmark, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray, true);
    }

}
