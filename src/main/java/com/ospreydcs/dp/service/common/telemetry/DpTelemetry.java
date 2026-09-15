package com.ospreydcs.dp.service.common.telemetry;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;
import io.grpc.ServerBuilder;
import io.grpc.opentelemetry.GrpcOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Owns this process's OpenTelemetry SDK and hands out the {@link Meter} every dp instrument is
 * built from (issue #212).
 *
 * <p>Initialization ordering is load-bearing: {@code init()} must run <em>before</em>
 * {@code initService_()}, because the Mongo client and the request handlers create their
 * instruments during their own init and must find a real meter rather than a no-op one. The
 * instruments in {@link DpMetrics} are created lazily against whatever this class holds at first
 * use, so an instrument built before {@code init()} would silently record nothing for the life of
 * the process.
 *
 * <p>This class never touches {@code GlobalOpenTelemetry}. That global is settable only once per
 * JVM and warns on every subsequent attempt, while the integration tests build many servers in a
 * single JVM; routing every lookup through {@link #meter()} is what lets a test install its own SDK
 * with an in-memory reader via {@link #initForTest} and tear it down again. The autoconfigured
 * builder does not register its result globally unless asked, so the rule costs nothing to keep.
 *
 * <p>The SDK is configured through {@code AutoConfiguredOpenTelemetrySdk} with dp-supplied
 * defaults, which the standard {@code OTEL_*} environment variables override: pointing a
 * deployment at a collector instead of the Prometheus endpoint is
 * {@code OTEL_METRICS_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=...} with no rebuild.
 */
public class DpTelemetry {

    // constants
    public static final String CFG_KEY_TELEMETRY_ENABLED = "Telemetry.enabled";
    public static final boolean DEFAULT_TELEMETRY_ENABLED = true;
    public static final String CFG_KEY_PROMETHEUS_HOST = "Telemetry.prometheusHost";
    public static final String DEFAULT_PROMETHEUS_HOST = "0.0.0.0";

    /** Instrumentation scope name, which the exporters carry as {@code otel_scope_name}. */
    public static final String INSTRUMENTATION_SCOPE_NAME = "com.ospreydcs.dp.service";

    private static final String OTEL_PROPERTY_SERVICE_NAME = "otel.service.name";
    private static final String OTEL_PROPERTY_METRICS_EXPORTER = "otel.metrics.exporter";
    private static final String OTEL_PROPERTY_TRACES_EXPORTER = "otel.traces.exporter";
    private static final String OTEL_PROPERTY_LOGS_EXPORTER = "otel.logs.exporter";
    private static final String OTEL_PROPERTY_PROMETHEUS_HOST = "otel.exporter.prometheus.host";
    private static final String OTEL_PROPERTY_PROMETHEUS_PORT = "otel.exporter.prometheus.port";

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * The OpenTelemetry instance every meter comes from. Never null: until {@code init()} runs it
     * is the no-op implementation, so instrumentation added to a code path that runs before
     * bootstrap (or in a unit test that never bootstraps) records nothing instead of throwing.
     */
    private static volatile OpenTelemetry openTelemetry = OpenTelemetry.noop();

    /** Non-null only when this class built the SDK itself, i.e. what {@link #shutdown} owns. */
    private static volatile OpenTelemetrySdk openTelemetrySdk = null;

    /** JVM runtime metrics registration, held only so that it can be closed on shutdown. */
    private static volatile AutoCloseable runtimeTelemetry = null;

    private static volatile boolean initialized = false;

    private DpTelemetry() {
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Builds the SDK for this process, exporting on the given Prometheus port.
     *
     * <p>A second call is a no-op with a warning rather than a replacement: the benchmark programs
     * start a server and a client in the same JVM, and rebuilding the SDK underneath instruments
     * that already hold a reference to the old meter would leave those instruments recording into
     * a shut-down provider.
     *
     * @param serviceName    value for {@code otel.service.name}, e.g. "ingestion" or "query"
     * @param prometheusPort port for the Prometheus scrape endpoint (D7 assigns one per service)
     * @throws DpRuntimeException if telemetry is enabled but the SDK cannot be built — most
     *                            commonly because the Prometheus port is already bound
     */
    public static synchronized void init(String serviceName, int prometheusPort) {

        if (initialized) {
            LOGGER.warn(
                    "DpTelemetry.init() ignored for service: {} because telemetry is already "
                            + "initialized in this JVM",
                    serviceName);
            return;
        }

        if (!configMgr().getConfigBoolean(CFG_KEY_TELEMETRY_ENABLED, DEFAULT_TELEMETRY_ENABLED)) {
            LOGGER.info(
                    "telemetry disabled by config key: {} so metrics are not being collected",
                    CFG_KEY_TELEMETRY_ENABLED);
            openTelemetry = OpenTelemetry.noop();
            initialized = true;
            return;
        }

        final String prometheusHost =
                configMgr().getConfigString(CFG_KEY_PROMETHEUS_HOST, DEFAULT_PROMETHEUS_HOST);

        final Map<String, String> properties = new HashMap<>();
        properties.put(OTEL_PROPERTY_SERVICE_NAME, serviceName);
        properties.put(OTEL_PROPERTY_METRICS_EXPORTER, "prometheus");
        // Traces and logs are explicitly off rather than left at their defaults: the autoconfigure
        // module defaults both to otlp, so without these the service would try to reach a
        // collector on localhost:4317 that a metrics-only deployment has no reason to run, and log
        // an export failure on every interval.
        properties.put(OTEL_PROPERTY_TRACES_EXPORTER, "none");
        properties.put(OTEL_PROPERTY_LOGS_EXPORTER, "none");
        properties.put(OTEL_PROPERTY_PROMETHEUS_HOST, prometheusHost);
        properties.put(OTEL_PROPERTY_PROMETHEUS_PORT, String.valueOf(prometheusPort));

        final OpenTelemetrySdk sdk;
        try {
            // addPropertiesSupplier() supplies defaults, so a value set through an OTEL_*
            // environment variable or a -Dotel.* system property still wins.
            //
            // setResultAsGlobal() is deliberately not called: it opts into registering this SDK as
            // GlobalOpenTelemetry, which the class javadoc explains must not happen. Nor is
            // disableShutdownHook() — the SDK's own JVM hook flushing a final export is wanted, and
            // is harmless alongside the explicit shutdown() from stopServer().
            sdk = AutoConfiguredOpenTelemetrySdk.builder()
                    .addPropertiesSupplier(() -> properties)
                    .build()
                    .getOpenTelemetrySdk();
        } catch (Exception e) {
            // Most likely the Prometheus port is already bound — by another dp service on the same
            // host (each has its own default port, see doc/metrics.md) or by a previous instance
            // still shutting down. Fail startup rather than continue: per the #254 rule a service
            // that cannot complete initialization must not serve requests, and the alternative
            // here is a service that runs with silently absent metrics, which an operator
            // discovers only when they go looking for a number that is not there.
            // The root cause is reported rather than e.getMessage(): the autoconfigure module
            // wraps a bind failure three deep and its own message is the useless "Unexpected
            // configuration error". Since this exception is what stops startup, its message is
            // the line an operator reads first, and "Address already in use" is the whole
            // diagnosis.
            throw new DpRuntimeException(
                    "error initializing telemetry for service: " + serviceName
                            + " with prometheus endpoint: " + prometheusHost + ":" + prometheusPort
                            + " exception: " + rootCauseMessage(e),
                    e);
        }

        openTelemetrySdk = sdk;
        openTelemetry = sdk;
        initialized = true;

        // JVM metrics (heap, GC, threads, class loading) from the instrumentation library, so that
        // a query that slowed down because the process is in GC trouble is diagnosable from the
        // same endpoint as the query metrics themselves.
        runtimeTelemetry = RuntimeTelemetry.create(sdk);

        LOGGER.info(
                "telemetry initialized for service: {} prometheus endpoint: {}:{}",
                serviceName, prometheusHost, prometheusPort);
    }

    /** Message of the deepest cause, which is where the actionable detail is. */
    private static String rootCauseMessage(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /**
     * Shuts the SDK down, flushing a final export. Called from {@code stopServer()} after the gRPC
     * server has terminated so that the last requests' measurements are included.
     */
    public static synchronized void shutdown() {

        if (runtimeTelemetry != null) {
            try {
                runtimeTelemetry.close();
            } catch (Exception e) {
                LOGGER.warn("error closing runtime telemetry: {}", e.getMessage(), e);
            }
            runtimeTelemetry = null;
        }

        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
            openTelemetrySdk = null;
        }

        openTelemetry = OpenTelemetry.noop();
        initialized = false;
    }

    /** The OpenTelemetry instance for this process; the no-op instance until {@code init()}. */
    public static OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    /** The meter every dp instrument is built from. */
    public static Meter meter() {
        return openTelemetry.getMeter(INSTRUMENTATION_SCOPE_NAME);
    }

    /**
     * Applies gRPC's own server-side metrics ({@code grpc.server.call.duration} and friends,
     * attributed by {@code grpc.method} and {@code grpc.status}) to a server builder.
     *
     * <p>Both {@code GrpcServerBase.start()} and the integration-test server wrapper call this, so
     * that what the ITs exercise is the same configuration production runs.
     */
    public static void configureServerBuilder(ServerBuilder<?> serverBuilder) {
        GrpcOpenTelemetry.newBuilder()
                .sdk(openTelemetry)
                .build()
                .configureServerBuilder(serverBuilder);
    }

    /**
     * Installs a caller-supplied SDK, for tests that assert on recorded measurements through an
     * {@code InMemoryMetricReader}. Pair with {@link #resetForTest()}.
     */
    public static synchronized void initForTest(OpenTelemetrySdk sdk) {
        // Deliberately does not take ownership: shutdown() closes only an SDK this class built, and
        // a test that supplied its own reader is responsible for its lifecycle.
        openTelemetry = sdk;
        initialized = true;
    }

    /**
     * Restores the uninitialized state so the next test starts from the no-op instance.
     *
     * <p>Instruments in {@link DpMetrics} are discarded as well, since they hold references to the
     * meter of the SDK being removed.
     */
    public static synchronized void resetForTest() {
        runtimeTelemetry = null;
        openTelemetrySdk = null;
        openTelemetry = OpenTelemetry.noop();
        initialized = false;
        DpMetrics.resetForTest();
    }

    /** True once {@code init()} or {@code initForTest()} has run. */
    public static boolean isInitialized() {
        return initialized;
    }

}
