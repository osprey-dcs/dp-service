package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvNameList;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.service.common.benchmark.BenchmarkMongoClient;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Long-range {@code querySamplesStream} benchmark shaped like the request behind issue #314: a
 * few concurrent streams, each over the whole history of one PV with a small chunk limit, the
 * requested range beginning long before the PV's first sample.
 *
 * <p>{@link BenchmarkQuerySamplesStream} cannot express this: it queries the last minute of the
 * fixture, a different set of PVs per stream. What that request stressed was the opposite: how
 * much the service reads and holds before it sends anything on a long range, and whether several
 * such streams fit in the heap together. Run the benchmark server with a small heap
 * ({@code -Xmx512m}) to see the second.
 *
 * <p>Start {@link BenchmarkQueryGrpcServer} first; it drops the benchmark database, and this
 * client then loads the fixture and queries it. The client reports, per stream, the time to the
 * first message and to completion, and the values received. Server heap is measured from outside
 * (for example {@code jstat -gc}); the client cannot see it.
 */
public class BenchmarkQuerySamplesStreamLongRange {

    private static final String USAGE = String.join("\n",
            "long-range querySamplesStream benchmark options (all optional):",
            "  --buckets=N              buckets to load for the PV (default 40000)",
            "  --seconds-per-bucket=N   seconds per bucket (default 100)",
            "  --samples-per-second=N   samples per second (default 1)",
            "  --lead-seconds=N         empty range requested before the first sample",
            "                           (default 63072000, two years)",
            "  --streams=N              concurrent streams over the same PV and range (default 4)",
            "  --limit=N                rows per streamed message (default 100)",
            "  --skip-load              query the fixture already loaded with the same options",
            "  --help                   print this and exit");

    private record Options(
            int buckets, int secondsPerBucket, int samplesPerSecond, long leadSeconds,
            int streams, int limit, boolean skipLoad) {
    }

    private record StreamResult(boolean success, long values, int messages, double firstMessageSeconds, double totalSeconds) {
    }

    private static Options parseArgs(String[] args) {
        int buckets = 40_000;
        int secondsPerBucket = 100;
        int samplesPerSecond = 1;
        long leadSeconds = 2L * 365 * 24 * 3600;
        int streams = 4;
        int limit = 100;
        boolean skipLoad = false;
        for (String arg : args) {
            if (arg.equals("--help")) {
                System.out.println(USAGE);
                System.exit(0);
            } else if (arg.equals("--skip-load")) {
                skipLoad = true;
            } else if (arg.startsWith("--") && arg.contains("=")) {
                final String key = arg.substring(2, arg.indexOf('='));
                final long value;
                try {
                    value = Long.parseLong(arg.substring(arg.indexOf('=') + 1));
                } catch (NumberFormatException ex) {
                    System.err.println("not a number: " + arg + "\n" + USAGE);
                    System.exit(2);
                    return null;
                }
                switch (key) {
                    case "buckets" -> buckets = (int) value;
                    case "seconds-per-bucket" -> secondsPerBucket = (int) value;
                    case "samples-per-second" -> samplesPerSecond = (int) value;
                    case "lead-seconds" -> leadSeconds = value;
                    case "streams" -> streams = (int) value;
                    case "limit" -> limit = (int) value;
                    default -> {
                        System.err.println("unknown option: " + arg + "\n" + USAGE);
                        System.exit(2);
                    }
                }
            } else {
                System.err.println("unknown argument: " + arg + "\n" + USAGE);
                System.exit(2);
            }
        }
        return new Options(buckets, secondsPerBucket, samplesPerSecond, leadSeconds, streams, limit, skipLoad);
    }

    private static StreamResult runStream(ManagedChannel channel, QuerySamplesRequest request, int streamNumber) {
        final long startNanos = System.nanoTime();
        long firstMessageNanos = -1;
        long values = 0;
        int messages = 0;
        try {
            final Iterator<QuerySamplesResponse> responses =
                    DpQueryServiceGrpc.newBlockingStub(channel).querySamplesStream(request);
            while (responses.hasNext()) {
                final QuerySamplesResponse response = responses.next();
                if (firstMessageNanos < 0) {
                    firstMessageNanos = System.nanoTime() - startNanos;
                }
                if (response.hasExceptionalResult()) {
                    System.err.println("stream " + streamNumber + " failed: "
                            + response.getExceptionalResult().getMessage());
                    return new StreamResult(false, values, messages, seconds(firstMessageNanos), seconds(System.nanoTime() - startNanos));
                }
                values += QueryV2BenchmarkSupport.countValues(response.getSampleQueryResult().getColumnTable());
                messages++;
            }
        } catch (RuntimeException ex) {
            System.err.println("stream " + streamNumber + " exception after " + messages + " messages: " + ex);
            return new StreamResult(false, values, messages, seconds(firstMessageNanos), seconds(System.nanoTime() - startNanos));
        }
        return new StreamResult(true, values, messages, seconds(firstMessageNanos), seconds(System.nanoTime() - startNanos));
    }

    private static double seconds(long nanos) {
        return nanos < 0 ? Double.NaN : nanos / 1e9;
    }

    public static void main(final String[] args) throws Exception {
        final Options options = parseArgs(args);
        final long historySeconds = (long) options.buckets() * options.secondsPerBucket();
        final long expectedValues = historySeconds * options.samplesPerSecond();

        // The history ends now, unless a previous load is being reused; then its marker says where.
        final long loadStartSeconds;
        if (options.skipLoad()) {
            final QueryBenchmarkBase.LoadMarker marker = QueryBenchmarkBase.readLoadMarker();
            if (marker == null || marker.historySeconds() != historySeconds) {
                System.err.println("--skip-load: no fixture with a history of " + historySeconds + " s is loaded");
                System.exit(1);
                return;
            }
            loadStartSeconds = marker.loadStartSeconds();
        } else {
            loadStartSeconds = Instant.now().getEpochSecond() - historySeconds;
            // selects the benchmark database (dp-benchmark) and drops it; without this the loader
            // would write to the default database
            BenchmarkMongoClient.prepareBenchmarkDatabase();
            QueryBenchmarkBase.loadBucketData(
                    new QueryBenchmarkBase.LoadParams(
                            1, options.samplesPerSecond(), options.secondsPerBucket(), historySeconds, 0, 0, false, false),
                    loadStartSeconds);
        }

        final QuerySamplesRequest request = QuerySamplesRequest.newBuilder()
                .setQuerySpec(QuerySpec.newBuilder()
                        .setTimeRange(TimeRange.newBuilder()
                                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(loadStartSeconds - options.leadSeconds()))
                                .setEndTime(Timestamp.newBuilder().setEpochSeconds(loadStartSeconds + historySeconds)))
                        .setPvSelector(PvSelector.newBuilder().setPvNameList(
                                PvNameList.newBuilder().addPvNames(IngestionBenchmarkBase.NAME_COLUMN_BASE + 1))))
                .setExecutionOptions(ExecutionOptions.newBuilder().setLimit(options.limit()))
                .build();

        final ManagedChannel channel = Grpc.newChannelBuilder(
                QueryBenchmarkBase.DEFAULT_GRPC_CONNECT_STRING, InsecureChannelCredentials.create()).build();
        final ExecutorService executor = Executors.newFixedThreadPool(options.streams());
        final long startNanos = System.nanoTime();
        final List<Future<StreamResult>> futures = new ArrayList<>();
        for (int stream = 1; stream <= options.streams(); stream++) {
            final int streamNumber = stream;
            futures.add(executor.submit(() -> runStream(channel, request, streamNumber)));
        }

        boolean allSucceeded = true;
        System.out.println(String.format(Locale.ROOT,
                "long-range querySamplesStream: %d streams, %d buckets x %d s, limit %d, lead %d s",
                options.streams(), options.buckets(), options.secondsPerBucket(), options.limit(), options.leadSeconds()));
        for (int i = 0; i < futures.size(); i++) {
            final StreamResult result = futures.get(i).get();
            final boolean complete = result.success() && result.values() == expectedValues;
            allSucceeded &= complete;
            System.out.println(String.format(Locale.ROOT,
                    "stream %d: %s  first message %.3f s  total %.3f s  messages %d  values %d of %d",
                    i + 1, complete ? "ok" : "FAILED", result.firstMessageSeconds(), result.totalSeconds(),
                    result.messages(), result.values(), expectedValues));
        }
        System.out.println(String.format(Locale.ROOT, "wall time %.3f s", seconds(System.nanoTime() - startNanos)));

        executor.shutdown();
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        // A run that returned less than the fixture measured something else; fail it (#275).
        System.exit(allSucceeded ? 0 : 1);
    }
}
