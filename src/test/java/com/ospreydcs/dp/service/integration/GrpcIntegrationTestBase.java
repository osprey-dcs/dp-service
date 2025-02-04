package com.ospreydcs.dp.service.integration;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.grpc.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import com.ospreydcs.dp.service.ingest.utility.RegisterProviderUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.TableResponseDispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public abstract class GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    protected static MongoTestClient mongoClient;

    // ingestion service instance variables
    private static IngestionServiceImpl ingestionService;
    private static IngestionServiceImpl ingestionServiceMock;
    protected static ManagedChannel ingestionChannel;

    // query service instance variables
    private static QueryServiceImpl queryService;
    private static QueryServiceImpl queryServiceMock;
    protected static ManagedChannel queryChannel;

    // annotation service instance variables
    private static AnnotationServiceImpl annotationService;
    private static AnnotationServiceImpl annotationServiceMock;
    protected static ManagedChannel annotationChannel;

    // validation instance variables
    protected static Map<String, AnnotationTestBase.CreateDataSetParams> createDataSetIdParamsMap = new TreeMap<>();
    protected static Map<AnnotationTestBase.CreateDataSetParams, String> createDataSetParamsIdMap = new HashMap<>();
    protected static Map<AnnotationTestBase.CreateAnnotationRequestParams, String> createAnnotationParamsIdMap = new HashMap<>();

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected static class IngestionColumnInfo {

        // instance variables
        public final String columnName;
        public final String requestIdBase;
        public final long measurementInterval;
        public final int numBuckets;
        public final int numSecondsPerBucket;
        public final boolean useExplicitTimestampList;

        public IngestionColumnInfo(
                String columnName,
                String requestIdBase,
                long measurementInterval,
                int numBuckets,
                int numSecondsPerBucket,
                boolean useExplicitTimestampList
        ) {
            this.columnName = columnName;
            this.requestIdBase = requestIdBase;
            this.measurementInterval = measurementInterval;
            this.numBuckets = numBuckets;
            this.numSecondsPerBucket = numSecondsPerBucket;
            this.useExplicitTimestampList = useExplicitTimestampList;
        }
    }

    protected static class IngestionBucketInfo {

        // instance variables
        public final String providerId;
        public final String requestId;
        public final long startSeconds;
        public final long startNanos;
        public final long endSeconds;
        public final long endNanos;
        public final int numValues;
        public final long intervalNanos;
        public final List<Object> dataValues;
        public final List<Long> timestampSecondsList;
        public final List<Long> timestampNanosList;

        public IngestionBucketInfo(
                String providerId,
                String requestId,
                long startSeconds,
                long startNanos,
                long endSeconds,
                long endNanos,
                int numValues,
                long intervalNanos,
                List<Object> dataValues,
                List<Long> timestampSecondsList,
                List<Long> timestampNanosList
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
            this.startSeconds = startSeconds;
            this.startNanos = startNanos;
            this.endSeconds = endSeconds;
            this.endNanos = endNanos;
            this.numValues = numValues;
            this.intervalNanos = intervalNanos;
            this.dataValues = dataValues;
            this.timestampSecondsList = timestampSecondsList;
            this.timestampNanosList = timestampNanosList;
        }
    }

    protected static class IngestionStreamInfo {

        // instance variables
        final public TimestampMap<IngestionBucketInfo> bucketInfoMap;
        final public TimestampMap<Double> valueMap;
        final List<IngestionTestBase.IngestionRequestParams> paramsList;
        final List<IngestDataRequest> requestList;
        final List<IngestDataResponse> responseList;

        public IngestionStreamInfo(
                TimestampMap<IngestionBucketInfo> bucketInfoMap,
                TimestampMap<Double> valueMap,
                List<IngestionTestBase.IngestionRequestParams> paramsList,
                List<IngestDataRequest> requestList,
                List<IngestDataResponse> responseList
        ) {
            this.bucketInfoMap = bucketInfoMap;
            this.valueMap = valueMap;
            this.paramsList = paramsList;
            this.requestList = requestList;
            this.responseList = responseList;
        }
    }

    public static void setUp() throws Exception {

        // init the mongo client interface for db verification, globally changes database name to dp-test
        mongoClient = new MongoTestClient();
        mongoClient.init();

        // init ingestion service
        IngestionHandlerInterface ingestionHandler = MongoIngestionHandler.newMongoSyncIngestionHandler();
        ingestionService = new IngestionServiceImpl();
        if (!ingestionService.init(ingestionHandler)) {
            fail("IngestionServiceImpl.init failed");
        }
        ingestionServiceMock = mock(IngestionServiceImpl.class, delegatesTo(ingestionService));
        // Generate a unique in-process server name.
        String ingestionServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(ingestionServerName).directExecutor().addService(ingestionServiceMock).build().start());
        // Create a client channel and register for automatic graceful shutdown.
        ingestionChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(ingestionServerName).directExecutor().build());

        // init query service
        QueryHandlerInterface queryHandler = MongoQueryHandler.newMongoSyncQueryHandler();
        queryService = new QueryServiceImpl();
        if (!queryService.init(queryHandler)) {
            fail("QueryServiceImpl.init failed");
        }
        queryServiceMock = mock(QueryServiceImpl.class, delegatesTo(queryService));
        // Generate a unique in-process server name.
        String queryServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(queryServerName).directExecutor().addService(queryServiceMock).build().start());
        // Create a client channel and register for automatic graceful shutdown.
        queryChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(queryServerName).directExecutor().build());

        // init annotation service
        AnnotationHandlerInterface annotationHandler = MongoAnnotationHandler.newMongoSyncAnnotationHandler();
        annotationService = new AnnotationServiceImpl();
        if (!annotationService.init(annotationHandler)) {
            fail("AnnotationServiceImpl.init failed");
        }
        annotationServiceMock = mock(AnnotationServiceImpl.class, delegatesTo(annotationService));
        // Generate a unique in-process server name.
        String annotationServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(annotationServerName).directExecutor().addService(annotationServiceMock).build().start());
        // Create a client channel and register for automatic graceful shutdown.
        annotationChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(annotationServerName).directExecutor().build());
    }

    public static void tearDown() {
        annotationService.fini();
        queryService.fini();
        ingestionService.fini();
        mongoClient.fini();
        mongoClient = null;
        ingestionServiceMock = null;
    }

    protected RegisterProviderResponse sendRegsiterProvider(
            RegisterProviderRequest request
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final RegisterProviderUtility.RegisterProviderResponseObserver responseObserver =
                new RegisterProviderUtility.RegisterProviderResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.registerProvider(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            fail("responseObserver error: " + responseObserver.getErrorMessage());
        }

        return responseObserver.getResponseList().get(0);
    }

    protected String sendAndVerifyRegisterProvider(
            RegisterProviderUtility.RegisterProviderRequestParams params,
            boolean expectExceptionalResponse,
            ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus,
            String expectedExceptionMessage,
            boolean expectedIsNew,
            String expectedProviderId
    ) {
        // build request
        final RegisterProviderRequest request = RegisterProviderUtility.buildRegisterProviderRequest(params);

        // send API request
        final RegisterProviderResponse response = sendRegsiterProvider(request);

        // verify exceptional response
        if (expectExceptionalResponse) {
            assertTrue(response.hasExceptionalResult());
            final ExceptionalResult exceptionalResult = response.getExceptionalResult();
            assertEquals(expectedExceptionStatus, exceptionalResult.getExceptionalResultStatus());
            assertTrue(exceptionalResult.getMessage().contains(expectedExceptionMessage));
            return null;
        }

        // verify registration result
        assertTrue(response.hasRegistrationResult());
        final RegisterProviderResponse.RegistrationResult registrationResult = response.getRegistrationResult();
        assertEquals(params.name, registrationResult.getProviderName());
        assertEquals(expectedIsNew, registrationResult.getIsNewProvider());
        final String providerId = registrationResult.getProviderId();

        // verify ProviderDocument from database
        final ProviderDocument providerDocument = mongoClient.findProvider(providerId);
        assertEquals(params.name, providerDocument.getName());
        if (params.attributes != null) {
            assertEquals(params.attributes, providerDocument.getAttributeMap());
        }

        // return id of ProviderDocument
        return providerId;
    }

    protected String registerProvider(String providerName, Map<String, String> attributeMap) {

        String providerId = null;

        // create register provider params
        final RegisterProviderUtility.RegisterProviderRequestParams params
                = new RegisterProviderUtility.RegisterProviderRequestParams(providerName, attributeMap);

        // send and verify register provider API request
        final boolean expectExceptionalResponse = false;
        final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
        final String expectedExceptionMessage = null;
        boolean expectedIsNew = true;
        final String expectedProviderId = null;
        providerId = sendAndVerifyRegisterProvider(
                params,
                expectExceptionalResponse,
                expectedExceptionStatus,
                expectedExceptionMessage,
                expectedIsNew,
                expectedProviderId);
        Objects.requireNonNull(providerId);

        return providerId;
    }

    protected IngestDataResponse sendIngestData(IngestDataRequest request) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(1);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.ingestData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            return responseObserver.getResponseList().get(0);
        }
    }

    protected IngestDataStreamResponse sendIngestDataStream(
            List<IngestDataRequest> requestList
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestDataStreamResponseObserver responseObserver =
                new IngestionTestBase.IngestDataStreamResponseObserver();

        StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataStream(responseObserver);

        for (IngestDataRequest request : requestList) {
            requestObserver.onNext(request); // don't create a thread to send request because it will be a race condition with call to onCompleted()
        }

        requestObserver.onCompleted();
        responseObserver.await();
        return responseObserver.getResponse();
    }

    protected List<IngestDataResponse> sendIngestDataBidiStream(List<IngestDataRequest> requestList) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(requestList.size());

        StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataBidiStream(responseObserver);

        for (IngestDataRequest request : requestList) {
            // send request in separate thread to better simulate out of process grpc,
            // otherwise service handles request in this thread
            new Thread(() -> {
                requestObserver.onNext(request);
            }).start();
        }

        responseObserver.await();
        requestObserver.onCompleted();

        if (responseObserver.isError()) {
            return new ArrayList<>();
        } else {
            return responseObserver.getResponseList();
        }
    }

    protected List<BucketDocument> verifyIngestionHandling(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            IngestDataStreamResponse response,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        if (expectReject) {
            assertTrue(response.hasExceptionalResult());
            assertEquals(expectedRejectMessage, response.getExceptionalResult().getMessage());
            assertEquals(
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                    response.getExceptionalResult().getExceptionalResultStatus());
        } else {
            assertTrue(response.hasIngestDataStreamResult());
            IngestDataStreamResponse.IngestDataStreamResult result = response.getIngestDataStreamResult();
            assertEquals(paramsList.size(), requestList.size());
            assertEquals(requestList.size(), result.getNumRequests());

            // verify handling for each params / request / response
            for (int listIndex = 0; listIndex < requestList.size(); ++listIndex) {

                final IngestionTestBase.IngestionRequestParams params = paramsList.get(listIndex);
                final IngestDataRequest request = requestList.get(listIndex);

                // verify database contents (request status and corresponding bucket documents)
                bucketDocumentList.addAll(verifyIngestionHandling(params, request));
            }
        }

        return bucketDocumentList;
    }

    protected List<BucketDocument> verifyIngestionHandling(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            List<IngestDataResponse> responseList
    ) {
        // check that parameter list sizes match
        assertEquals(paramsList.size(), requestList.size());
        assertEquals(requestList.size(), responseList.size());

        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        // verify handling for each params / request / response
        for (int listIndex = 0 ; listIndex < requestList.size() ; ++listIndex) {

            final IngestionTestBase.IngestionRequestParams params = paramsList.get(listIndex);
            final IngestDataRequest request = requestList.get(listIndex);
            final IngestDataResponse response = responseList.get(listIndex);

            // verify API response
            final int numPvs = params.columnNames.size();
            assertTrue(response.hasAckResult());
            final IngestDataResponse.AckResult ackResult = response.getAckResult();
            assertEquals(numPvs, ackResult.getNumColumns());
            assertEquals((int) params.samplingClockCount, ackResult.getNumRows());

            // verify database contents (request status and corresponding bucket documents)
            bucketDocumentList.addAll(verifyIngestionHandling(params, request));
        }

        return bucketDocumentList;
    }

    protected List<BucketDocument> verifyIngestionHandling(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest request
    ) {
        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        // validate database RequestStatusDocument
        final int numPvs = params.columnNames.size();
        final RequestStatusDocument statusDocument =
                mongoClient.findRequestStatus(params.providerId, params.requestId);
        assertEquals(
                IngestionRequestStatus.INGESTION_REQUEST_STATUS_SUCCESS_VALUE,
                statusDocument.getRequestStatusCase());
        assertEquals(numPvs, statusDocument.getIdsCreated().size());
        final List<String> expectedBucketIds = new ArrayList<>();
        for (String pvName : params.columnNames) {
            final String expectedBucketId =
                    pvName + "-" + params.samplingClockStartSeconds + "-" + params.samplingClockStartNanos;
            assertTrue(expectedBucketId, statusDocument.getIdsCreated().contains(expectedBucketId));
            expectedBucketIds.add(expectedBucketId);
        }

        // validate database BucketDocument for each column
        int pvIndex = 0;
        for (String expectedBucketId : expectedBucketIds) {

            final BucketDocument bucketDocument = mongoClient.findBucket(expectedBucketId);
            bucketDocumentList.add(bucketDocument);

            assertNotNull(bucketDocument);
            final String pvName = params.columnNames.get(pvIndex);
            assertEquals(pvName, bucketDocument.getPvName());
            assertEquals(expectedBucketId, bucketDocument.getId());

            // check bucket start times
            assertEquals((long) params.samplingClockStartSeconds, bucketDocument.getFirstSeconds());
            assertEquals((long) params.samplingClockStartNanos, bucketDocument.getFirstNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(
                            params.samplingClockStartSeconds, params.samplingClockStartNanos)),
                    bucketDocument.getFirstTime());

            // check sample count params
            assertEquals((int) params.samplingClockCount, bucketDocument.getSampleCount());
            assertEquals(
                    (int) params.samplingClockCount,
                    bucketDocument.readDataColumnContent().getDataValuesList().size());

            // check DataTimestamps (TimestampsList or SamplingClock depending on request)
            final DataTimestamps bucketDataTimestamps = bucketDocument.readDataTimestampsContent();
            DataTimestampsUtility.DataTimestampsModel requestDataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(
                            request.getIngestionDataFrame().getDataTimestamps());
            final long endSeconds = requestDataTimestampsModel.getLastTimestamp().getEpochSeconds();
            final long endNanos = requestDataTimestampsModel.getLastTimestamp().getNanoseconds();
            assertEquals(requestDataTimestampsModel.getSamplePeriodNanos(), bucketDocument.getSamplePeriod());

            if (params.timestampsSecondsList != null && params.timestampsSecondsList.size() > 0) {
                // check explicit TimestampsList
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.getNumber(),
                        bucketDocument.getDataTimestampsCase());
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.name(),
                        bucketDocument.getDataTimestampsType());

                // compare list of timestamps in bucket vs. params
                assertTrue(bucketDataTimestamps.hasTimestampList());
                final List<Timestamp> bucketTimestampList =
                        bucketDataTimestamps.getTimestampList().getTimestampsList();
                assertEquals(params.timestampsSecondsList.size(), bucketTimestampList.size());
                assertEquals(params.timestampNanosList.size(), bucketTimestampList.size());
                for (int timestampIndex = 0; timestampIndex < bucketTimestampList.size(); ++timestampIndex) {
                    final Timestamp bucketTimestamp = bucketTimestampList.get(timestampIndex);
                    final long requestSeconds = params.timestampsSecondsList.get(timestampIndex);
                    final long requestNanos = params.timestampNanosList.get(timestampIndex);
                    assertEquals(requestSeconds, bucketTimestamp.getEpochSeconds());
                    assertEquals(requestNanos, bucketTimestamp.getNanoseconds());
                }

            } else {
                // check SamplingClock parameters
                assertEquals(
                        DataTimestamps.ValueCase.SAMPLINGCLOCK.getNumber(),
                        bucketDocument.getDataTimestampsCase());
                assertEquals(
                        DataTimestamps.ValueCase.SAMPLINGCLOCK.name(),
                        bucketDocument.getDataTimestampsType());

            }

            // check bucket end times against expected values determined above
            assertEquals(endSeconds, bucketDocument.getLastSeconds());
            assertEquals(endNanos, bucketDocument.getLastNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(endSeconds, endNanos)),
                    bucketDocument.getLastTime());

            // compare data value vectors
            final List<DataColumn> dataColumnList = request.getIngestionDataFrame().getDataColumnsList();
            final DataColumn bucketDataColumn = bucketDocument.readDataColumnContent();
            final DataColumn requestDataColumn = dataColumnList.get(pvIndex);

            // this compares each DataValue including ValueStatus, confirmed in debugger
            assertEquals(requestDataColumn, bucketDataColumn);

            pvIndex = pvIndex + 1;
        }

        return bucketDocumentList;
    }

    protected List<BucketDocument> sendAndVerifyIngestData(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest
    ) {
        final IngestDataResponse response = sendIngestData(ingestionRequest);
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = Arrays.asList(response);
        return verifyIngestionHandling(paramsList, requestList, responseList);
    }

    protected List<BucketDocument> sendAndVerifyIngestDataStream(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            boolean expectReject,
            String expectedRejectMessage
    ) {

        // send request
        final IngestDataStreamResponse response = sendIngestDataStream(requestList);
        return verifyIngestionHandling(paramsList, requestList, response, expectReject, expectedRejectMessage);
    }

    protected List<BucketDocument> sendAndVerifyIngestDataBidiStream(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest
    ) {

        // send request
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = sendIngestDataBidiStream(requestList);
        return verifyIngestionHandling(paramsList, requestList, responseList);
    }

    protected IngestionStreamInfo ingestDataBidiStream(
            long startSeconds,
            long startNanos,
            String providerId,
            IngestionColumnInfo columnInfo
    ) {
        final String requestIdBase = columnInfo.requestIdBase;
        long measurementInterval = columnInfo.measurementInterval;
        final String columnName = columnInfo.columnName;
        final int numBuckets = columnInfo.numBuckets;
        final int numSecondsPerBucket = columnInfo.numSecondsPerBucket;

        final int numSamplesPerSecond = ((int) (1_000_000_000 / measurementInterval));
        final int numSamplesPerBucket = numSamplesPerSecond * numSecondsPerBucket;

        // create data structures for later validation
        final TimestampMap<Double> valueMap = new TimestampMap<>();
        final TimestampMap<IngestionBucketInfo> bucketInfoMap = new TimestampMap<>();

        // create requests
        final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
        final List<IngestDataRequest> requestList = new ArrayList<>();
        long currentSeconds = startSeconds;
        int secondsCount = 0;
        for (int bucketIndex = 0; bucketIndex < numBuckets; ++bucketIndex) {

            final String requestId = requestIdBase + bucketIndex;

            // create list of column data values for request
            final List<List<Object>> columnValues = new ArrayList<>();
            final List<Object> dataValuesList = new ArrayList<>();
            List<Long> timestampSecondsList = null;
            List<Long> timestampNanosList = null;
            if (columnInfo.useExplicitTimestampList) {
                timestampSecondsList = new ArrayList<>();
                timestampNanosList = new ArrayList<>();
            }
            for (int secondIndex = 0; secondIndex < numSecondsPerBucket; ++secondIndex) {
                long currentNanos = 0;

                for (int sampleIndex = 0; sampleIndex < numSamplesPerSecond; ++sampleIndex) {
                    final double dataValue =
                            secondsCount + (double) sampleIndex / numSamplesPerSecond;
                    dataValuesList.add(dataValue);
                    valueMap.put(currentSeconds + secondIndex, currentNanos, dataValue);
                    if (columnInfo.useExplicitTimestampList) {
                        timestampSecondsList.add(currentSeconds + secondIndex);
                        timestampNanosList.add(currentNanos);
                    }
                    currentNanos = currentNanos + measurementInterval;
                }

                secondsCount = secondsCount + 1;
            }
            columnValues.add(dataValuesList);

            // create request parameters
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            timestampSecondsList, // if not null, request will use explicit TimestampsList in DataTimestamps
                            timestampNanosList,
                            currentSeconds,
                            startNanos,
                            measurementInterval,
                            numSamplesPerBucket,
                            List.of(columnName),
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            columnValues, null);
            paramsList.add(params);

            final Instant startTimeInstant = Instant.ofEpochSecond(currentSeconds, startNanos);
            final Instant endTimeInstant =
                    startTimeInstant.plusNanos(measurementInterval * (numSamplesPerBucket - 1));

            // capture data for later validation
            final long bucketInfoSamplePeriod = (columnInfo.useExplicitTimestampList) ? 0 : measurementInterval;
            final IngestionBucketInfo bucketInfo =
                    new IngestionBucketInfo(
                            providerId,
                            requestId,
                            currentSeconds,
                            startNanos,
                            endTimeInstant.getEpochSecond(),
                            endTimeInstant.getNano(),
                            numSamplesPerBucket,
                            bucketInfoSamplePeriod,
                            dataValuesList,
                            timestampSecondsList,
                            timestampNanosList);
            bucketInfoMap.put(currentSeconds, startNanos, bucketInfo);

            // build request
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            requestList.add(request);

            currentSeconds = currentSeconds + numSecondsPerBucket;
        }

        // send requests
        final List<IngestDataResponse> responseList = sendIngestDataBidiStream(requestList);
        assertEquals(requestList.size(), responseList.size());
        for (IngestDataResponse response : responseList) {
            assertTrue(response.hasAckResult());
            final IngestDataResponse.AckResult ackResult = response.getAckResult();
            assertEquals(1, ackResult.getNumColumns());
            assertEquals(numSamplesPerBucket, ackResult.getNumRows());
        }

        return new IngestionStreamInfo(bucketInfoMap, valueMap, paramsList, requestList, responseList);
    }

    protected Map<String, IngestionStreamInfo> ingestDataBidiStreamFromColumn(
            List<IngestionColumnInfo> columnInfoList,
            long startSeconds,
            long startNanos,
            String providerId
    ) {
        // create data structure for validating query result
        Map<String, IngestionStreamInfo> validationMap = new TreeMap<>();

        for (IngestionColumnInfo columnInfo : columnInfoList) {
            final IngestionStreamInfo streamInfo =
                    ingestDataBidiStream(
                            startSeconds,
                            startNanos,
                            providerId,
                            columnInfo);
            verifyIngestionHandling(streamInfo.paramsList, streamInfo.requestList, streamInfo.responseList);
            validationMap.put(columnInfo.columnName, streamInfo);
        }

        return validationMap;
    }

    protected Map<String, IngestionStreamInfo> simpleIngestionScenario() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

        // register provider used by scenario
        final String providerName = String.valueOf(INGESTION_PROVIDER_ID);
        final String providerId = registerProvider(providerName, null);

        List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();

        // create data for 10 sectors, each containing 3 gauges and 3 bpms
        for (int sectorIndex = 1 ; sectorIndex <= 10 ; ++sectorIndex) {
            final String sectorName = String.format("S%02d", sectorIndex);

            // create columns for 3 gccs in each sector
            for (int gccIndex = 1 ; gccIndex <= 3 ; ++ gccIndex) {
                final String gccName = sectorName + "-" + String.format("GCC%02d", gccIndex);
                final String requestIdBase = gccName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                gccName,
                                requestIdBase,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false);
                ingestionColumnInfoList.add(columnInfoTenths);
            }

            // create columns for 3 bpms in each sector
            for (int bpmIndex = 1 ; bpmIndex <= 3 ; ++ bpmIndex) {
                final String bpmName = sectorName + "-" + String.format("BPM%02d", bpmIndex);
                final String requestIdBase = bpmName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                bpmName,
                                requestIdBase,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false);
                ingestionColumnInfoList.add(columnInfoTenths);
            }
        }

        Map<String, IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestDataBidiStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, providerId);
        }

        return validationMap;
    }

    private static class QueryRequestStatusResult {
        final public List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> statusList;
        final boolean noData;
        public QueryRequestStatusResult(
                List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> statusList,
                boolean noData
        ) {
            this.statusList = statusList;
            this.noData = noData;
        }
    }

    private QueryRequestStatusResult sendQueryRequestStatus(
            QueryRequestStatusRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub
                = DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.QueryRequestStatusResponseObserver responseObserver =
                new IngestionTestBase.QueryRequestStatusResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryRequestStatus(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            if (responseObserver.isError()
                    && responseObserver.getErrorMessage().equals(
                            "onNext received exception response: query returned no data")
            ) {
                return new QueryRequestStatusResult(responseObserver.getRequestStatusList(), true);
            } else {
                assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
            }
        }

        return new QueryRequestStatusResult(responseObserver.getRequestStatusList(), false);
    }

    protected void sendAndVerifyQueryRequestStatus(
            IngestionTestBase.QueryRequestStatusParams params,
            IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        // NOTE
        //
        // This method includes retry logic for a strange behavior that I encountered, but determined that retry didn't
        // solve (e.g., the start times for ingestion and query were not correct).  I left the retry logic here because
        // we might decide we need it at some point.

        final QueryRequestStatusRequest request = IngestionTestBase.buildQueryRequestStatusRequest(params);
        List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> requestStatusList = new ArrayList<>();
        for (int retryCount = 0 ; retryCount < MongoTestClient.MONGO_FIND_RETRY_COUNT ; ++retryCount) {
            QueryRequestStatusResult result = sendQueryRequestStatus(request, expectReject, expectedRejectMessage);
            if (result.noData && expectedResponseMap.size() > 0) {
                logger.info("sendAndVerifyQueryRequestStatus no data found, retrying");
                try {
                    Thread.sleep(MongoTestClient.MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and retry
                }
            } else {
                requestStatusList = result.statusList;
                break;
            }
        }

        // verify API response against expectedResponseMap
        assertEquals(expectedResponseMap.size(), requestStatusList.size());
        for (QueryRequestStatusResponse.RequestStatusResult.RequestStatus responseStatus : requestStatusList) {
            IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponseStatus =
                    expectedResponseMap.get(responseStatus.getProviderId(), responseStatus.getRequestId());
            assertEquals(expectedResponseStatus.providerId, responseStatus.getProviderId());
            assertEquals(expectedResponseStatus.providerName, responseStatus.getProviderName());
            assertEquals(expectedResponseStatus.requestId, responseStatus.getRequestId());
            assertEquals(expectedResponseStatus.status, responseStatus.getIngestionRequestStatus());
//            assertEquals(responseStatus.getStatusMessage(), expectedResponseStatus.statusMessage);
            assertEquals(expectedResponseStatus.idsCreated, responseStatus.getIdsCreatedList());
        }
    }

    private SubscribeDataUtility.SubscribeDataCall sendSubscribeData(
            SubscribeDataRequest request,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                new IngestionTestBase.SubscribeDataResponseObserver(expectedResponseCount);

        // invoke subscribeData() API method, get handle to request stream
        StreamObserver<SubscribeDataRequest> requestObserver = asyncStub.subscribeData(responseObserver);

        // send NewSubscription message in request stream
        new Thread(() -> {
            requestObserver.onNext(request);
        }).start();

        // wait for ack response
        responseObserver.awaitAckLatch();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return new SubscribeDataUtility.SubscribeDataCall(requestObserver, responseObserver);
    }

    protected SubscribeDataUtility.SubscribeDataCall initiateSubscribeDataRequest(
            SubscribeDataRequest request,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        return sendSubscribeData(request, expectedResponseCount, expectReject, expectedRejectMessage);
    }

    protected SubscribeDataUtility.SubscribeDataCall initiateSubscribeDataRequest(
            List<String> pvNameList,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataRequest(pvNameList);
        final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                sendSubscribeData(request, expectedResponseCount, expectReject, expectedRejectMessage);
        return subscribeDataCall;
    }

    protected void verifySubscribeDataResponse(
            IngestionTestBase.SubscribeDataResponseObserver responseObserver,
            List<String> pvNameList,
            Map<String, IngestionStreamInfo> ingestionValidationMap
    ) {
        responseObserver.awaitResponseLatch();

        assertFalse(responseObserver.isError());

        // verify responses against ingestion request params

        final List<SubscribeDataResponse> responseList = responseObserver.getResponseList();

        // create map of response dataTimestamps values by pvName
        Map<String, List<DataTimestamps>> responsePvTimestampsMap = new HashMap<>();
        for (SubscribeDataResponse response : responseList) {
            final String responsePvName = response.getSubscribeDataResult().getDataColumnsList().get(0).getName();
            final DataTimestamps responseDataTimestamps = response.getSubscribeDataResult().getDataTimestamps();
            List<DataTimestamps> pvTimestampsList = responsePvTimestampsMap.get(responsePvName);
            if (pvTimestampsList == null) {
                pvTimestampsList = new ArrayList<>();
                responsePvTimestampsMap.put(responsePvName, pvTimestampsList);
            }
            pvTimestampsList.add(responseDataTimestamps);
        }

        // iterate through request params for specified pvs
        for (String pvName : pvNameList) {
            for (IngestionTestBase.IngestionRequestParams requestParams : ingestionValidationMap.get(pvName).paramsList) {
                // check that a response was received that matches this PV name and the request dataTimestamps
                List<DataTimestamps> responsePvTimestampsList = responsePvTimestampsMap.get(pvName);
                assertNotNull(responsePvTimestampsList);
                boolean found = false;
                for (DataTimestamps responsePvTimestamps : responsePvTimestampsList) {
                    if ((responsePvTimestamps.getSamplingClock().getStartTime().getEpochSeconds()  == requestParams.samplingClockStartSeconds)
                            && (responsePvTimestamps.getSamplingClock().getStartTime().getNanoseconds() == requestParams.samplingClockStartNanos)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            }
        }
    }

    protected void cancelSubscribeDataCall(SubscribeDataUtility.SubscribeDataCall subscribeDataCall) {

        final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataCancelRequest();

        // send NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataCall.requestObserver.onNext(request);
        }).start();

        // wait for ack response stream to close
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver;
        responseObserver.awaitCloseLatch();

    }

    protected void closeSubscribeDataCall(SubscribeDataUtility.SubscribeDataCall subscribeDataCall) {

        // close the request stream
        new Thread(() -> {
            subscribeDataCall.requestObserver.onCompleted();
        }).start();

        // wait for ack response stream to close
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver;
        responseObserver.awaitCloseLatch();
    }

    protected QueryTableResponse.TableResult sendQueryTable(QueryTableRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseTableObserver responseObserver =
                new QueryTestBase.QueryResponseTableObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            final QueryTableResponse response = responseObserver.getQueryResponse();
            return response.getTableResult();
        }
    }
    protected QueryTableResponse.TableResult queryTable(QueryTestBase.QueryTableRequestParams params) {
        final QueryTableRequest request = QueryTestBase.buildQueryTableRequest(params);
        return sendQueryTable(request);
    }

    private void verifyQueryTableColumnResult(
            QueryTestBase.QueryTableRequestParams params,
            QueryTableResponse.TableResult tableResult,
            int numRowsExpected,
            List<String> pvNameList,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        // check table is correct format
        assertTrue(tableResult.hasColumnTable());

        final List<Timestamp> timestampList =
                tableResult.getColumnTable().getDataTimestamps().getTimestampList().getTimestampsList();
        assertEquals(numRowsExpected, timestampList.size());
        assertEquals(pvNameList.size(), tableResult.getColumnTable().getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();

            // check that timestamp is in query time range
            assertTrue(
                    (timestampSeconds > params.beginTimeSeconds)
                            || (timestampSeconds == params.beginTimeSeconds && timestampNanos >= params.beginTimeNanos));
            assertTrue(
                    (timestampSeconds < params.endTimeSeconds)
                            || (timestampSeconds == params.endTimeSeconds && timestampNanos <= params.endTimeNanos));

            for (DataColumn dataColumn : tableResult.getColumnTable().getDataColumnsList()) {
                // get column name and value from query result
                String columnName = dataColumn.getName();
                Double columnDataValue = dataColumn.getDataValues(rowIndex).getDoubleValue();

                // get expected value from validation map
                final TimestampMap<Double> columnValueMap = validationMap.get(columnName).valueMap;
                Double expectedColumnDataValue = columnValueMap.get(timestampSeconds, timestampNanos);
                if (expectedColumnDataValue != null) {
                    assertEquals(expectedColumnDataValue, columnDataValue, 0.0);
                } else {
                    assertEquals(0.0, columnDataValue, 0.0);
                }
            }
            rowIndex = rowIndex + 1;
        }
    }

    protected void sendAndVerifyQueryTablePvNameListColumnResult(
            int numRowsExpected,
            List<String> pvNameList,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN,
                        pvNameList,
                        null,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params);

        // validate query result contents in tableResult
        verifyQueryTableColumnResult(params, tableResult, numRowsExpected, pvNameList, validationMap);
    }

    protected void sendAndVerifyQueryTablePvNamePatternColumnResult(
            int numRowsExpected,
            String pvNamePattern,
            List<String> expectedPvNameMatches,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN,
                        null,
                        pvNamePattern,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params);

        // validate query result contents in tableResult
        verifyQueryTableColumnResult(params, tableResult, numRowsExpected, expectedPvNameMatches, validationMap);
    }

    private void verifyQueryTableRowResult(
            QueryTestBase.QueryTableRequestParams params,
            QueryTableResponse.TableResult tableResult,
            int numRowsExpected,
            List<String> pvNameList,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        // check table is correct format
        assertTrue(tableResult.hasRowMapTable());
        final QueryTableResponse.RowMapTable resultRowMapTable = tableResult.getRowMapTable();

        // verify result column names matches list of pv names plus timestamp column
        final List<String> resultColumnNamesList = resultRowMapTable.getColumnNamesList();
        assertTrue(resultColumnNamesList.contains(TableResponseDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME));
        for (String columnName : pvNameList) {
            assertTrue(resultColumnNamesList.contains(columnName));
        }
        assertEquals(pvNameList.size() + 1, resultColumnNamesList.size());

        // verify correct number of data rows
        assertEquals(numRowsExpected, resultRowMapTable.getRowsCount());

        // verify data row content
        for (QueryTableResponse.RowMapTable.DataRow resultDataRow : resultRowMapTable.getRowsList()) {
            final Map<String, DataValue> resultRowValueMap = resultDataRow.getColumnValuesMap();

            // check that map keys are present for each column
            assertEquals(resultColumnNamesList.size(), resultRowValueMap.keySet().size());
            for (String resultColumnName : resultColumnNamesList) {
                assertTrue(resultRowValueMap.containsKey(resultColumnName));
            }

            // get timestamp column value for row
            final DataValue resultRowTimestampDataValue =
                    resultRowValueMap.get(TableResponseDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
            assertTrue(resultRowTimestampDataValue.hasTimestampValue());
            final Timestamp resultRowTimestamp = resultRowTimestampDataValue.getTimestampValue();

            // check that timestamp is in query time range
            final long resultRowSeconds = resultRowTimestamp.getEpochSeconds();
            final long resultRowNanos = resultRowTimestamp.getNanoseconds();
            assertTrue(
                    (resultRowSeconds > params.beginTimeSeconds)
                            || (resultRowSeconds == params.beginTimeSeconds && resultRowNanos >= params.beginTimeNanos));
            assertTrue(
                    (resultRowSeconds < params.endTimeSeconds)
                            || (resultRowSeconds == params.endTimeSeconds && resultRowNanos <= params.endTimeNanos));

            // verify value for each column in row
            for (String columnName : pvNameList) {
                final DataValue resultColumnDataValue = resultRowValueMap.get(columnName);
                assertTrue(resultColumnDataValue.hasDoubleValue());
                double resultColumnDoubleValue = resultColumnDataValue.getDoubleValue();
                final TimestampMap<Double> columnValueMap = validationMap.get(columnName).valueMap;
                final Double expectedColumnDoubleValue = columnValueMap.get(
                        resultRowTimestamp.getEpochSeconds(), resultRowTimestamp.getNanoseconds());
                if (expectedColumnDoubleValue != null) {
                    assertEquals(expectedColumnDoubleValue, resultColumnDoubleValue, 0.0);
                } else {
                    assertEquals(0.0, resultColumnDoubleValue, 0.0);
                }
            }
        }
    }

    protected void sendAndVerifyQueryTablePvNameListRowResult(
            int numRowsExpected,
            List<String> pvNameList,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_ROW_MAP,
                        pvNameList,
                        null,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params);

        // validate query result contents in tableResult
        verifyQueryTableRowResult(params, tableResult, numRowsExpected, pvNameList, validationMap);
    }

    protected void sendAndVerifyQueryTablePvNamePatternRowResult(
            int numRowsExpected,
            String pvNamePattern,
            List<String> expectedPvNameMatches,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_ROW_MAP,
                        null,
                        pvNamePattern,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params);

        // validate query result contents in tableResult
        verifyQueryTableRowResult(params, tableResult, numRowsExpected, expectedPvNameMatches, validationMap);
    }

    protected List<QueryDataResponse.QueryData.DataBucket> sendQueryDataStream(QueryDataRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseStreamObserver responseObserver =
                new QueryTestBase.QueryResponseStreamObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            return responseObserver.getDataBucketList();
        }
    }

    protected List<QueryDataResponse.QueryData.DataBucket> queryDataStream(
            List<String> pvNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos
    ) {
        final QueryTestBase.QueryDataRequestParams params =
                new QueryTestBase.QueryDataRequestParams(pvNames, startSeconds, startNanos, endSeconds, endNanos);
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataStream(request);
    }

    protected void sendAndVerifyQueryDataStream(
            int numBucketsExpected,
            List<String> pvNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final List<QueryDataResponse.QueryData.DataBucket> dataBucketList =
                queryDataStream(pvNames, startSeconds, startNanos, endSeconds, endNanos);

        // build map of buckets in query response for vallidation
        Map<String, TimestampMap<QueryDataResponse.QueryData.DataBucket>> responseBucketMap =
                new TreeMap<>();
        for (QueryDataResponse.QueryData.DataBucket dataBucket : dataBucketList) {
            final String bucketColumnName = dataBucket.getDataColumn().getName();
            final DataTimestampsUtility.DataTimestampsModel bucketDataTimestampsModel
                    = new DataTimestampsUtility.DataTimestampsModel(dataBucket.getDataTimestamps());
            final Timestamp bucketStartTimestamp = bucketDataTimestampsModel.getFirstTimestamp();
            final long bucketStartSeconds = bucketStartTimestamp.getEpochSeconds();
            final long bucketStartNanos = bucketStartTimestamp.getNanoseconds();
            TimestampMap<QueryDataResponse.QueryData.DataBucket> columnTimestampMap =
                    responseBucketMap.get(bucketColumnName);
            if (columnTimestampMap == null) {
                columnTimestampMap = new TimestampMap<>();
                responseBucketMap.put(bucketColumnName, columnTimestampMap);
            }
            columnTimestampMap.put(bucketStartSeconds, bucketStartNanos, dataBucket);
        }

        // iterate through the expected buckets for each column,
        // and validate them against the corresponding response bucket
        int validatedBuckets = 0;
        for (var validationMapEntry : validationMap.entrySet()) {
            final String columnName = validationMapEntry.getKey();
            final IngestionStreamInfo columnStreamInfo = validationMapEntry.getValue();
            for (var bucketInfoMapEntry : columnStreamInfo.bucketInfoMap.entrySet()) {
                final long bucketSecond = bucketInfoMapEntry.getKey();
                final Map<Long, IngestionBucketInfo> bucketNanoMap = bucketInfoMapEntry.getValue();
                for (IngestionBucketInfo columnBucketInfo : bucketNanoMap.values()) {

                    // skip buckets outside the query range
                    if ((columnBucketInfo.startSeconds > endSeconds)
                            || ((columnBucketInfo.startSeconds == endSeconds) && (columnBucketInfo.startNanos >= endNanos))) {
                        // bucket starts after query end time
                        continue;
                    }
                    if ((columnBucketInfo.endSeconds < startSeconds)
                            || ((columnBucketInfo.endSeconds == startSeconds) && (columnBucketInfo.endNanos < startNanos))) {
                        // bucket ends before query start time
                        continue;
                    }

                    // find the response bucket corresponding to the expected bucket
                    final QueryDataResponse.QueryData.DataBucket responseBucket =
                            responseBucketMap.get(columnName).get(columnBucketInfo.startSeconds, startNanos);
                    final DataTimestampsUtility.DataTimestampsModel responseBucketDataTimestampsModel =
                            new DataTimestampsUtility.DataTimestampsModel(responseBucket.getDataTimestamps());

                    assertEquals(
                            columnBucketInfo.intervalNanos,
                            responseBucketDataTimestampsModel.getSamplePeriodNanos());
                    assertEquals(
                            columnBucketInfo.numValues,
                            responseBucketDataTimestampsModel.getSampleCount());

                    // validate bucket data values
                    int valueIndex = 0;
                    for (DataValue responseDataValue : responseBucket.getDataColumn().getDataValuesList()) {

                        final double actualDataValue = responseDataValue.getDoubleValue();

                        Object expectedValue = columnBucketInfo.dataValues.get(valueIndex);
                        assertTrue(expectedValue instanceof Double);
                        Double expectedDataValue = (Double) expectedValue;
                        assertEquals(expectedDataValue, actualDataValue, 0.0);

                        valueIndex = valueIndex + 1;
                    }

                    validatedBuckets = validatedBuckets + 1;
                }
            }
        }

        // check that we validated all buckets returned by the query, and that query returned expected number of buckets
        assertEquals(dataBucketList.size(), validatedBuckets);
        assertEquals(numBucketsExpected, dataBucketList.size());
    }

    protected List<QueryMetadataResponse.MetadataResult.PvInfo> sendQueryMetadata(
            QueryMetadataRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getPvInfoList();
    }

    private void sendAndVerifyQueryMetadata(
            QueryMetadataRequest request,
            List<String> columnNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<QueryMetadataResponse.MetadataResult.PvInfo> pvInfoList =
                sendQueryMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(0, pvInfoList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(columnNames.size(), pvInfoList.size());

        // build map of column info list for convenience
        final Map<String, QueryMetadataResponse.MetadataResult.PvInfo> pvInfoMap = new HashMap<>();
        for (QueryMetadataResponse.MetadataResult.PvInfo columnInfo : pvInfoList) {
            pvInfoMap.put(columnInfo.getPvName(), columnInfo);
        }

        // build list of pv names in response to verify against expected
        final List<String> responsePvNames = new ArrayList<>();
        for (QueryMetadataResponse.MetadataResult.PvInfo columnInfo : pvInfoList) {
            responsePvNames.add(columnInfo.getPvName());
        }

        // check that response pvNames are sorted (against list in sorted order)
        assertEquals(columnNames, responsePvNames);

        // check that a PvInfo was received for each name and verify its contents
        for (String columnName : columnNames) {
            final QueryMetadataResponse.MetadataResult.PvInfo pvInfo =
                    pvInfoMap.get(columnName);
            assertNotNull(pvInfo);
            assertEquals(columnName, pvInfo.getPvName());
            assertEquals(8, pvInfo.getLastBucketDataTypeCase());
            assertEquals("DOUBLEVALUE", pvInfo.getLastBucketDataType());
            assertEquals(1, pvInfo.getLastBucketDataTimestampsCase());
            assertEquals("SAMPLINGCLOCK", pvInfo.getLastBucketDataTimestampsType());

            // iterate through validationMap to get info for first and last bucket for column
            IngestionBucketInfo firstBucketInfo = null;
            IngestionBucketInfo lastBucketInfo = null;
            boolean first = true;
            for (var bucketMapEntry : validationMap.get(columnName).bucketInfoMap.entrySet()) {
                final var nanoMap = bucketMapEntry.getValue();
                for (var nanoMapEntry : nanoMap.entrySet()) {
                    if (first) {
                        firstBucketInfo = nanoMapEntry.getValue();
                        first = false;
                    }
                    lastBucketInfo = nanoMapEntry.getValue();
                }
            }

            // verify ColumnInfo contents for column against last and first bucket details
            assertNotNull(lastBucketInfo);
            assertEquals(lastBucketInfo.intervalNanos, pvInfo.getLastBucketSamplePeriod());
            assertEquals(lastBucketInfo.numValues, pvInfo.getLastBucketSampleCount());
            assertEquals(lastBucketInfo.endSeconds, pvInfo.getLastDataTimestamp().getEpochSeconds());
            assertEquals(lastBucketInfo.endNanos, pvInfo.getLastDataTimestamp().getNanoseconds());
            assertNotNull(firstBucketInfo);
            assertEquals(firstBucketInfo.startSeconds, pvInfo.getFirstDataTimestamp().getEpochSeconds());
            assertEquals(firstBucketInfo.startNanos, pvInfo.getFirstDataTimestamp().getNanoseconds());

            // check last bucket id
            final String expectedLastBucketId =
                    columnName + "-" + lastBucketInfo.startSeconds + "-" + lastBucketInfo.startNanos;
            assertEquals(expectedLastBucketId, pvInfo.getLastBucketId());

        }
    }

    protected void sendAndVerifyQueryMetadata(
            List<String> columnNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryMetadataRequest request = QueryTestBase.buildQueryMetadataRequest(columnNames);
        sendAndVerifyQueryMetadata(request, columnNames, validationMap, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryMetadata(
            String columnNamePattern,
            Map<String, IngestionStreamInfo> validationMap,
            List<String> expectedColumnNames,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryMetadataRequest request = QueryTestBase.buildQueryMetadataRequest(columnNamePattern);
        sendAndVerifyQueryMetadata(request, expectedColumnNames, validationMap, expectReject, expectedRejectMessage);
    }

    protected String sendCreateDataSet(
            CreateDataSetRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.CreateDataSetResponseObserver responseObserver =
                new AnnotationTestBase.CreateDataSetResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.createDataSet(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataSetId();
    }

    protected String sendAndVerifyCreateDataSet(
            AnnotationTestBase.CreateDataSetParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final CreateDataSetRequest request =
                AnnotationTestBase.buildCreateDataSetRequest(params);

        final String dataSetId = sendCreateDataSet(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(dataSetId);
            return "";
        }

        // validate response and database contents
        assertNotNull(dataSetId);
        assertFalse(dataSetId.isBlank());
        final DataSetDocument dataSetDocument = mongoClient.findDataSet(dataSetId);
        assertNotNull(dataSetDocument);
        final List<String> requestDiffs = dataSetDocument.diffRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.toString(), requestDiffs.isEmpty());

        this.createDataSetIdParamsMap.put(dataSetId, params);
        this.createDataSetParamsIdMap.put(params, dataSetId);

        return dataSetId;
    }

    protected List<DataSet> sendQueryDataSets(
            QueryDataSetsRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.QueryDataSetsResponseObserver responseObserver =
                new AnnotationTestBase.QueryDataSetsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataSets(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataSetsList();
    }

    protected List<DataSet> sendAndVerifyQueryDataSets(
            String datasetId,
            String ownerId,
            String descriptionText,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResult
    ) {
        final QueryDataSetsRequest request =
                AnnotationTestBase.buildQueryDataSetsRequest(datasetId, ownerId, descriptionText);

        final List<DataSet> resultDataSets = sendQueryDataSets(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(resultDataSets.isEmpty());
            return new ArrayList<>();
        }

        // validate response

        assertEquals(expectedQueryResult.size(), resultDataSets.size());

        // find each expected result in actual result list and match field values against request
        for (AnnotationTestBase.CreateDataSetParams requestParams : expectedQueryResult) {
            boolean found = false;
            DataSet foundDataSet = null;
            for (DataSet resultDataSet : resultDataSets) {
                if (requestParams.dataSet.name.equals(resultDataSet.getName())) {
                    found = true;
                    foundDataSet = resultDataSet;
                    break;
                }
            }
            assertTrue(found);
            assertNotNull(foundDataSet);
            final String expectedDataSetId = this.createDataSetParamsIdMap.get(requestParams);
            assertTrue(expectedDataSetId.equals(foundDataSet.getDataSetId()));
            if (datasetId != null) {
                assertEquals(datasetId, foundDataSet.getDataSetId());
            }
            assertTrue(requestParams.dataSet.description.equals(foundDataSet.getDescription()));
            assertTrue(requestParams.dataSet.ownerId.equals(foundDataSet.getOwnerId()));

            // compare data blocks from result with request
            final AnnotationTestBase.AnnotationDataSet requestDataSet = requestParams.dataSet;
            assertEquals(requestDataSet.dataBlocks.size(), foundDataSet.getDataBlocksCount());
            for (AnnotationTestBase.AnnotationDataBlock requestBlock : requestDataSet.dataBlocks) {
                boolean responseBlockFound = false;
                for (DataBlock responseBlock : foundDataSet.getDataBlocksList()) {
                    if (
                            (Objects.equals(requestBlock.beginSeconds, responseBlock.getBeginTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.beginNanos, responseBlock.getBeginTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.endSeconds, responseBlock.getEndTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.endNanos, responseBlock.getEndTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.pvNames, responseBlock.getPvNamesList()))
                    ) {
                        responseBlockFound = true;
                        break;
                    }
                }
                assertTrue(responseBlockFound);
            }
        }

        return resultDataSets;
    }

    protected String sendCreateAnnotation(
            CreateAnnotationRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.CreateAnnotationResponseObserver responseObserver =
                new AnnotationTestBase.CreateAnnotationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.createAnnotation(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertFalse(expectedRejectMessage.isBlank());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getAnnotationId();
    }

    protected String sendAndVerifyCreateAnnotation(
            AnnotationTestBase.CreateAnnotationRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final CreateAnnotationRequest request =
                AnnotationTestBase.buildCreateAnnotationRequest(params);

        final String annotationId = sendCreateAnnotation(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(annotationId);
            return null;
        }

        // validate response and database contents
        assertNotNull(annotationId);
        assertFalse(annotationId.isBlank());
        final AnnotationDocument annotationDocument = mongoClient.findAnnotation(annotationId);
        assertNotNull(annotationDocument);
        final List<String> requestDiffs = annotationDocument.diffCreateAnnotationRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.isEmpty());

        // save annotationId to map for use in validating queryAnnotations() result
        this.createAnnotationParamsIdMap.put(params, annotationId);

        return annotationId;
    }

    protected List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendQueryAnnotations(
            QueryAnnotationsRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.QueryAnnotationsResponseObserver responseObserver =
                new AnnotationTestBase.QueryAnnotationsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryAnnotations(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getAnnotationsList();
    }

    protected List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendAndVerifyQueryAnnotations(
            String annotationId,
            String ownerId,
            String datasetId,
            String commentText,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryResult
    ) {
        final QueryAnnotationsRequest request =
                AnnotationTestBase.buildQueryAnnotationsRequest(annotationId, ownerId, datasetId, commentText);

        final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> resultAnnotations =
                sendQueryAnnotations(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(resultAnnotations.isEmpty());
            return new ArrayList<>();
        }

        // validate response
        assertEquals(expectedQueryResult.size(), resultAnnotations.size());
        // find each expected result in actual result list and match field values against request
        for (AnnotationTestBase.CreateAnnotationRequestParams requestParams : expectedQueryResult) {
            boolean found = false;
            QueryAnnotationsResponse.AnnotationsResult.Annotation foundAnnotation = null;
            for (QueryAnnotationsResponse.AnnotationsResult.Annotation resultAnnotation : resultAnnotations) {
                if (
                        (requestParams.ownerId.equals(resultAnnotation.getOwnerId())) &&
                        (Objects.equals(requestParams.dataSetIds, resultAnnotation.getDataSetIdsList())) &&
                        (requestParams.name.equals(resultAnnotation.getName()))
                ) {
                    found = true;
                    foundAnnotation = resultAnnotation;
                    break;
                }
            }
            assertTrue(found);
            assertNotNull(foundAnnotation);
            final String expectedAnnotationId = this.createAnnotationParamsIdMap.get(requestParams);
            assertTrue(expectedAnnotationId.equals(foundAnnotation.getId()));
            if (annotationId != null) {
                assertEquals(annotationId, foundAnnotation.getId());
            }

            // compare required fields from request against found annotation
            assertTrue(requestParams.ownerId.equals(foundAnnotation.getOwnerId()));
            assertTrue(requestParams.name.equals(foundAnnotation.getName()));
            // use Objects.equals to compare list elements
            assertTrue(Objects.equals(requestParams.dataSetIds, foundAnnotation.getDataSetIdsList()));

            // compare optional fields from request against found annotation
            if (requestParams.comment != null) {
                assertTrue(requestParams.comment.equals(foundAnnotation.getComment()));
            }
            // TODO: add other optional fields like tags, attributes, eventMetadata, annotationIds



            // match annotation fields in result against query filter parameters
            if (ownerId != null) {
                assertEquals(ownerId, foundAnnotation.getOwnerId());
            }
            if (datasetId != null) {
                assertTrue(foundAnnotation.getDataSetIdsList().contains(datasetId));
            }
            if (commentText != null) {
                assertTrue(foundAnnotation.getComment().contains(commentText));
            }

// TODO: uncomment to check dataset list content after making changes to create query result containing datasets
//            // compare dataset content from result with what was requested when creating dataset
//            final AnnotationTestBase.CreateDataSetParams dataSetRequestParams =
//                    this.createDataSetIdParamsMap.get(requestParams.dataSetIds);
//            final AnnotationTestBase.AnnotationDataSet requestDataSet = dataSetRequestParams.dataSet;
//            final DataSet responseDataSet = foundAnnotation.getDataSet();
//            assertEquals(requestDataSet.dataBlocks.size(), responseDataSet.getDataBlocksCount());
//            assertTrue(requestDataSet.name.equals(responseDataSet.getName()));
//            assertTrue(requestDataSet.ownerId.equals(responseDataSet.getOwnerId()));
//            assertTrue(requestDataSet.description.equals(responseDataSet.getDescription()));
//            for (AnnotationTestBase.AnnotationDataBlock requestBlock : requestDataSet.dataBlocks) {
//                boolean responseBlockFound = false;
//                for (DataBlock responseBlock : responseDataSet.getDataBlocksList()) {
//                    if (
//                            (Objects.equals(requestBlock.beginSeconds, responseBlock.getBeginTime().getEpochSeconds()))
//                                    && (Objects.equals(requestBlock.beginNanos, responseBlock.getBeginTime().getNanoseconds()))
//                                    && (Objects.equals(requestBlock.endSeconds, responseBlock.getEndTime().getEpochSeconds()))
//                                    && (Objects.equals(requestBlock.endNanos, responseBlock.getEndTime().getNanoseconds()))
//                                    && (Objects.equals(requestBlock.pvNames, responseBlock.getPvNamesList()))
//                    ) {
//                        responseBlockFound = true;
//                        break;
//                    }
//                }
//                assertTrue(responseBlockFound);
//            }
        }

        return resultAnnotations;
    }

    protected ExportDataSetResponse.ExportDataSetResult sendExportDataSet(
            ExportDataSetRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.ExportDataSetResponseObserver responseObserver =
                new AnnotationTestBase.ExportDataSetResponseObserver();

        // start performance measurment timer
        final Instant t0 = Instant.now();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.exportDataSet(request, responseObserver);
        }).start();

        responseObserver.await();

        // stop performance measurement timer
        final Instant t1 = Instant.now();
        final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        final double secondsElapsed = dtMillis / 1_000.0;

        System.out.println("export format " + request.getOutputFormat().name() + " elapsed seconds: " + secondsElapsed);

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getResult();
    }

    private TimestampDataMap getTimestampDataMapForDataset(DataSetDocument dataset) {

        final TimestampDataMap expectedDataMap = new TimestampDataMap();
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final MongoCursor<BucketDocument> cursor = mongoClient.findDataBlockBuckets(dataBlock);
            final long beginSeconds = dataBlock.getBeginTimeSeconds();
            final long beginNanos = dataBlock.getBeginTimeNanos();
            final long endSeconds = dataBlock.getEndTimeSeconds();
            final long endNanos = dataBlock.getEndTimeNanos();
            final TabularDataUtility.TimestampDataMapSizeStats sizeStats =
                    TabularDataUtility.updateTimestampMapFromBucketCursor(
                            expectedDataMap,
                            cursor,
                            0,
                            null,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );
        }

        return expectedDataMap;
    }

    protected ExportDataSetResponse.ExportDataSetResult sendAndVerifyExportDataSet(
            String dataSetId,
            ExportDataSetRequest.ExportOutputFormat outputFormat,
            int expectedNumBuckets,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final ExportDataSetRequest request =
                AnnotationTestBase.buildExportDataSetRequest(
                        dataSetId,
                        outputFormat);

        final ExportDataSetResponse.ExportDataSetResult exportResult =
                sendExportDataSet(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(exportResult == null);
            return null;
        }

        // validate
        assertNotNull(exportResult);
        assertNotEquals("", exportResult.getFilePath());

//         assertNotEquals("", exportResult.getFileUrl());
//        // open file url to reproduce issue Mitch encountered from web app
//        String command = "curl " + exportResult.getFileUrl();
//        try {
//            Process process = Runtime.getRuntime().exec(command);
//            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//            String line;
//            while((line = reader.readLine()) != null) {
//                System.out.println(line);
//            }
//            reader.close();
//        } catch (IOException e) {
//            fail("exception calling curl for " + exportResult.getFilePath() + ": " + e.getMessage());
//        }

        // check that file is available (this is the same as the check done by WatchService (e.g., WatchService
        // won't solve our problem with corrupt excel file if this test succeeds)
        Path target = Path.of(exportResult.getFilePath());
        try {
            BasicFileAttributes attributes = Files.readAttributes(target, BasicFileAttributes.class);
            System.out.println("got file attributes for: " + target);
        } catch (IOException ex) {
            fail("IOException getting file attributes for: " + target);
        }

//        // copy file from url to reproduce issue Mitch enountered from web app (opening URL from Javascript)
//        final int filenameIndex = exportResult.getFilePath().lastIndexOf('/') + 1;
//        final String filename = exportResult.getFilePath().substring(filenameIndex);
//        try {
//            FileUtils.copyURLToFile(new URL(exportResult.getFileUrl()), new File("/tmp/" + filename));
//        } catch (IOException e) {
//            fail("IOException copying file from url " + exportResult.getFileUrl() + ": " + e.getMessage());
//        }
//
        // retrieve dataset for id
        DataSetDocument dataset = mongoClient.findDataSet(dataSetId);
        assertNotNull(dataset);

        // retrieve BucketDocuments for specified dataset
        final List<BucketDocument> datasetBuckets = mongoClient.findDataSetBuckets(dataset);
        assertEquals(expectedNumBuckets, datasetBuckets.size());

        // verify file content for specified output format
        switch (outputFormat) {

            case EXPORT_FORMAT_HDF5 -> {
                final IHDF5Reader reader = HDF5Factory.openForReading(exportResult.getFilePath());
                AnnotationTestBase.verifyDatasetHdf5Content(reader, dataset);
                for (BucketDocument bucket : datasetBuckets) {
                    AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, bucket);
                }
                reader.close();
            }

            case EXPORT_FORMAT_CSV -> {

                // build temporary tabular data structure from cursor
                final TimestampDataMap expectedDataMap = getTimestampDataMapForDataset(dataset);

                // verify file content against data map
                AnnotationTestBase.verifyCsvContentFromTimestampDataMap(exportResult, expectedDataMap);
            }

            case EXPORT_FORMAT_XLSX -> {

                // build temporary tabular data structure from cursor
                final TimestampDataMap expectedDataMap = getTimestampDataMapForDataset(dataset);

                // verify file content against data map
                AnnotationTestBase.verifyXlsxContentFromTimestampDataMap(exportResult, expectedDataMap);
            }
        }

        return exportResult;
    }

}
