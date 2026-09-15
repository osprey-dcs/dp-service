package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import com.ospreydcs.dp.service.integration.annotation.GrpcIntegrationAnnotationServiceWrapper;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.integration.ingestionstream.GrpcIntegrationIngestionStreamServiceWrapper;
import com.ospreydcs.dp.service.integration.query.GrpcIntegrationQueryServiceWrapper;
import io.grpc.ManagedChannel;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.mockito.Mockito.mock;

public abstract class GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    // instance variables
    protected MongoTestClient mongoClient;

    /**
     * Collects the metrics recorded during one test, for the metric-asserting ITs (issue #212).
     * Every IT installs one whether or not it asserts on metrics: the instrumentation runs on the
     * paths they all exercise, and giving it a real SDK here means the ITs cover the recording
     * code rather than a no-op meter.
     */
    protected InMemoryMetricReader metricReader;

    private OpenTelemetrySdk telemetrySdk;

    protected GrpcIntegrationIngestionServiceWrapper ingestionServiceWrapper =
            new GrpcIntegrationIngestionServiceWrapper();
    protected GrpcIntegrationQueryServiceWrapper queryServiceWrapper =
            new GrpcIntegrationQueryServiceWrapper();
    protected GrpcIntegrationAnnotationServiceWrapper annotationServiceWrapper =
            new GrpcIntegrationAnnotationServiceWrapper();
    protected GrpcIntegrationIngestionStreamServiceWrapper ingestionStreamServiceWrapper =
            new GrpcIntegrationIngestionStreamServiceWrapper();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public void setUp() throws Exception {

        // Install a test SDK before anything else is created. The handlers and the Mongo client
        // build their instruments during init, and DpMetrics binds each instrument to whatever
        // meter DpTelemetry holds at first use -- installed after the wrappers, the instruments
        // would be bound to the no-op meter and nothing would reach this reader.
        //
        // initForTest() rather than init(): this JVM runs a server per wrapper per test, and
        // production init() binds a Prometheus port, which the second test in a class would fail
        // to bind. It also deliberately does not take ownership of the SDK, so tearDown() closes
        // it here.
        metricReader = InMemoryMetricReader.create();
        telemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(
                        SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        DpTelemetry.initForTest(telemetrySdk);

        // init the mongo client interface for db verification, globally changes database name to dp-test
        mongoClient = new MongoTestClient();
        mongoClient.init();

        // init ingestion service
        ingestionServiceWrapper.init(mongoClient);
        ManagedChannel ingestionChannel = ingestionServiceWrapper.getIngestionChannel();

        // init query service
        queryServiceWrapper.init(mongoClient);

        // init annotation service
        annotationServiceWrapper.init(mongoClient);

        // init ingestion stream service
        ingestionStreamServiceWrapper.init(mongoClient, ingestionChannel);
    }

    public void tearDown() {

        logger.debug("GrpcIntegrationTestBase tearDown");

        ingestionStreamServiceWrapper.fini();
        annotationServiceWrapper.fini();
        queryServiceWrapper.fini();
        ingestionServiceWrapper.fini();

        mongoClient.fini();
        mongoClient = null;

        // After every wrapper's fini(), so that a handler shutting down can still close its
        // observable-gauge registration against a live meter provider. resetForTest() also
        // discards the cached DpMetrics instruments, which hold references to the provider being
        // closed here -- without that, the next test's instruments would record into this test's
        // reader.
        DpTelemetry.resetForTest();
        if (telemetrySdk != null) {
            telemetrySdk.close();
            telemetrySdk = null;
        }
        metricReader = null;
    }

}
