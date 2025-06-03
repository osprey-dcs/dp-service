package com.ospreydcs.dp.service.integration;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.google.protobuf.InvalidProtocolBufferException;
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
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
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
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryTableDispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.ClassRule;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    // validation static variables
    protected static Map<String, AnnotationTestBase.CreateDataSetParams> createDataSetIdParamsMap = new TreeMap<>();
    protected static Map<AnnotationTestBase.CreateDataSetParams, String> createDataSetParamsIdMap = new HashMap<>();
    protected static Map<AnnotationTestBase.CreateAnnotationRequestParams, String> createAnnotationParamsIdMap =
            new HashMap<>();

    // constants
    protected static final int INGESTION_PROVIDER_ID = 1;
    protected static final String GCC_INGESTION_PROVIDER = "GCC Provider";
    protected static final String BPM_INGESTION_PROVIDER = "BPM Provider";
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected record IngestionProviderInfo(
            String providerId,
            Set<String> pvNameSet,
            long firstTimeSeconds,
            long firstTimeNanos,
            long lastTimeSeconds,
            long lastTimeNanos,
            int numBuckets
    ) {
    }

    /**
     * @param columnName               instance variables
     * @param useSerializedDataColumns
     */
    public record IngestionColumnInfo(
            String columnName,
            String requestIdBase,
            String providerId,
            long measurementInterval,
            int numBuckets,
            int numSecondsPerBucket,
            boolean useExplicitTimestampList,
            boolean useSerializedDataColumns, List<String> tags,
            Map<String, String> attributes,
            String eventDescription,
            Long eventStartSeconds,
            Long eventStartNanos,
            Long eventStopSeconds,
            Long eventStopNanos
    ) {
    }

    /**
     * @param providerId instance variables
     */
    protected record IngestionBucketInfo(
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
            List<Long> timestampNanosList,
            List<String> tags,
            Map<String, String> attributes,
            String eventDescription,
            Long eventStartSeconds,
            Long eventStartNanos,
            Long eventStopSeconds,
            Long eventStopNanos
    ) {
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

    protected record IngestionScenarioResult(
            Map<String, IngestionProviderInfo> providerInfoMap,
            Map<String, IngestionStreamInfo> validationMap
    ) {
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

    protected static RegisterProviderResponse sendRegsiterProvider(
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

    protected static String sendAndVerifyRegisterProvider(
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
        if (params.description != null) {
            assertEquals(params.description, providerDocument.getDescription());
        } else {
            assertEquals("", providerDocument.getDescription());
        }
        if (params.tags != null) {
            assertEquals(params.tags, providerDocument.getTags());
        } else {
            assertTrue(providerDocument.getTags() == null);
        }
        if (params.attributes != null) {
            assertEquals(params.attributes, providerDocument.getAttributes());
        } else {
            assertTrue(providerDocument.getAttributes() == null);
        }
        assertNotNull(providerDocument.getCreatedAt());
        assertNotNull(providerDocument.getUpdatedAt());

        // return id of ProviderDocument
        return providerId;
    }

    protected static String registerProvider(RegisterProviderUtility.RegisterProviderRequestParams params) {

        // send and verify register provider API request
        final boolean expectExceptionalResponse = false;
        final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
        final String expectedExceptionMessage = null;
        boolean expectedIsNew = true;
        final String expectedProviderId = null;
        final String providerId = sendAndVerifyRegisterProvider(
                params,
                expectExceptionalResponse,
                expectedExceptionStatus,
                expectedExceptionMessage,
                expectedIsNew,
                expectedProviderId);
        Objects.requireNonNull(providerId);

        return providerId;
    }

    protected static String registerProvider(String providerName, Map<String, String> attributeMap) {

        // create register provider params
        final RegisterProviderUtility.RegisterProviderRequestParams params
                = new RegisterProviderUtility.RegisterProviderRequestParams(providerName, attributeMap);

        return registerProvider(params);
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

    protected static List<IngestDataResponse> sendIngestDataBidiStream(List<IngestDataRequest> requestList) {

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
            int numSerializedDataColumnsExpected,
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
                bucketDocumentList.addAll(verifyIngestionHandling(params, request, numSerializedDataColumnsExpected));
            }
        }

        return bucketDocumentList;
    }

    protected static List<BucketDocument> verifyIngestionHandling(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            List<IngestDataResponse> responseList,
            int numSerializedDataColumnsExpected
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
            bucketDocumentList.addAll(verifyIngestionHandling(params, request, numSerializedDataColumnsExpected));
        }

        return bucketDocumentList;
    }

    protected static List<BucketDocument> verifyIngestionHandling(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest request,
            int numSerializedDataColumnsExpected
    ) {
        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        // validate database RequestStatusDocument
        final int numPvs = params.columnNames.size();
        final RequestStatusDocument statusDocument =
                mongoClient.findRequestStatus(params.providerId, params.requestId);
        assertNotNull(statusDocument);
        assertNotNull(statusDocument.getCreatedAt());
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
        int serializedDataColumnCount = 0;
        for (String expectedBucketId : expectedBucketIds) {

            final BucketDocument bucketDocument = mongoClient.findBucket(expectedBucketId);
            bucketDocumentList.add(bucketDocument);

            assertNotNull(bucketDocument);
            final String pvName = params.columnNames.get(pvIndex);
            assertEquals(pvName, bucketDocument.getPvName());
            assertEquals(expectedBucketId, bucketDocument.getId());

            // check createdAt time
            assertNotNull(bucketDocument.getCreatedAt());

            // check bucket start times
            assertEquals(
                    (long) params.samplingClockStartSeconds,
                    bucketDocument.getDataTimestamps().getFirstTime().getSeconds());
            assertEquals(
                    (long) params.samplingClockStartNanos,
                    bucketDocument.getDataTimestamps().getFirstTime().getNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(
                            params.samplingClockStartSeconds, params.samplingClockStartNanos)),
                    bucketDocument.getDataTimestamps().getFirstTime().getDateTime());

            // check sample count params
            assertEquals(
                    (int) params.samplingClockCount,
                    bucketDocument.getDataTimestamps().getSampleCount());
            DataColumn bucketDataColumn = null;
            try {
                bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
            } catch (DpException e) {
                throw new RuntimeException(e);
            }
            Objects.requireNonNull(bucketDataColumn);
            assertEquals(
                    (int) params.samplingClockCount,
                    bucketDataColumn.getDataValuesList().size());

            // check DataTimestamps (TimestampsList or SamplingClock depending on request)
            DataTimestamps bucketDataTimestamps = null;
            try {
                bucketDataTimestamps = bucketDocument.getDataTimestamps().toDataTimestamps();
            } catch (DpException e) {
                fail("exception deserializing DataTimestampsDocument.bytes: " + e.getMessage());
            }
            Objects.requireNonNull(bucketDataTimestamps);
            DataTimestampsUtility.DataTimestampsModel requestDataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(
                            request.getIngestionDataFrame().getDataTimestamps());
            final long endSeconds = requestDataTimestampsModel.getLastTimestamp().getEpochSeconds();
            final long endNanos = requestDataTimestampsModel.getLastTimestamp().getNanoseconds();
            assertEquals(
                    requestDataTimestampsModel.getSamplePeriodNanos(),
                    bucketDocument.getDataTimestamps().getSamplePeriod());

            if (params.timestampsSecondsList != null && !params.timestampsSecondsList.isEmpty()) {
                // check explicit TimestampsList
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.getNumber(),
                        bucketDocument.getDataTimestamps().getValueCase());
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.name(),
                        bucketDocument.getDataTimestamps().getValueType());

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
                        bucketDocument.getDataTimestamps().getValueCase());
                assertEquals(
                        DataTimestamps.ValueCase.SAMPLINGCLOCK.name(),
                        bucketDocument.getDataTimestamps().getValueType());

            }

            // check bucket end times against expected values determined above
            assertEquals(endSeconds, bucketDocument.getDataTimestamps().getLastTime().getSeconds());
            assertEquals(endNanos, bucketDocument.getDataTimestamps().getLastTime().getNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(endSeconds, endNanos)),
                    bucketDocument.getDataTimestamps().getLastTime().getDateTime());

            // compare data value vectors
            DataColumn requestDataColumn = null;

            if (params.useSerializedDataColumns) {
                // request contains SerializedDataColumns
                final List<SerializedDataColumn> serializedDataColumnList =
                        request.getIngestionDataFrame().getSerializedDataColumnsList();
                final SerializedDataColumn serializedDataColumn = serializedDataColumnList.get(pvIndex);
                // deserialize column for comparison
                try {
                    requestDataColumn = DataColumn.parseFrom(serializedDataColumn.getDataColumnBytes());
                } catch (InvalidProtocolBufferException e) {
                    fail("exception deserializing DataColumn: " + e.getMessage());
                }
                serializedDataColumnCount = serializedDataColumnCount + 1;

            } else {
                // request contains regular DataColumns
                final List<DataColumn> dataColumnList = request.getIngestionDataFrame().getDataColumnsList();
                requestDataColumn = dataColumnList.get(pvIndex);
            }
            // this compares each DataValue including ValueStatus, confirmed in debugger
            assertEquals(requestDataColumn, bucketDataColumn);

            // check tags
            if (params.tags != null) {
                assertEquals(params.tags, bucketDocument.getTags());
            } else {
                assertTrue(bucketDocument.getTags() == null || bucketDocument.getTags().isEmpty());
            }

            // check attributes
            if (params.attributes != null) {
                assertEquals(params.attributes, bucketDocument.getAttributes());
            } else {
                assertTrue(bucketDocument.getAttributes() == null || bucketDocument.getAttributes().isEmpty());
            }

            // check event metadata
            if (params.eventDescription != null) {
                assertNotNull(bucketDocument.getEvent());
                assertEquals(request.getEventMetadata(), bucketDocument.getEvent().toEventMetadata());
            } else {
                assertNull(bucketDocument.getEvent());
            }

            pvIndex = pvIndex + 1;
        }
        assertEquals(numSerializedDataColumnsExpected, serializedDataColumnCount);

        return bucketDocumentList;
    }

    protected List<BucketDocument> sendAndVerifyIngestData(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest,
            int numSerializedDataColumnsExpected
    ) {
        final IngestDataResponse response = sendIngestData(ingestionRequest);
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = Arrays.asList(response);
        return verifyIngestionHandling(paramsList, requestList, responseList, numSerializedDataColumnsExpected);
    }

    protected List<BucketDocument> sendAndVerifyIngestDataStream(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            int numSerializedDataColumnsExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {

        // send request
        final IngestDataStreamResponse response = sendIngestDataStream(requestList);
        return verifyIngestionHandling(
                paramsList,
                requestList,
                response,
                numSerializedDataColumnsExpected,
                expectReject,
                expectedRejectMessage);
    }

    protected List<BucketDocument> sendAndVerifyIngestDataBidiStream(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest,
            int numSerializedDataColumnsExpected
    ) {

        // send request
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = sendIngestDataBidiStream(requestList);
        return verifyIngestionHandling(paramsList, requestList, responseList, numSerializedDataColumnsExpected);
    }

    protected static IngestionStreamInfo ingestDataBidiStream(
            long startSeconds,
            long startNanos,
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
                            columnInfo.providerId,
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
                            columnValues,
                            columnInfo.tags,
                            columnInfo.attributes,
                            columnInfo.eventDescription,
                            columnInfo.eventStartSeconds,
                            columnInfo.eventStartNanos,
                            columnInfo.eventStopSeconds,
                            columnInfo.eventStopNanos,
                            columnInfo.useSerializedDataColumns
                    );
            paramsList.add(params);

            final Instant startTimeInstant = Instant.ofEpochSecond(currentSeconds, startNanos);
            final Instant endTimeInstant =
                    startTimeInstant.plusNanos(measurementInterval * (numSamplesPerBucket - 1));

            // capture data for later validation
            final long bucketInfoSamplePeriod = (columnInfo.useExplicitTimestampList) ? 0 : measurementInterval;
            final IngestionBucketInfo bucketInfo =
                    new IngestionBucketInfo(
                            columnInfo.providerId,
                            requestId,
                            currentSeconds,
                            startNanos,
                            endTimeInstant.getEpochSecond(),
                            endTimeInstant.getNano(),
                            numSamplesPerBucket,
                            bucketInfoSamplePeriod,
                            dataValuesList,
                            timestampSecondsList,
                            timestampNanosList,
                            columnInfo.tags,
                            columnInfo.attributes,
                            columnInfo.eventDescription,
                            columnInfo.eventStartSeconds,
                            columnInfo.eventStartNanos,
                            columnInfo.eventStopSeconds,
                            columnInfo.eventStopNanos
                    );
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

    protected static Map<String, IngestionStreamInfo> ingestDataBidiStreamFromColumn(
            List<IngestionColumnInfo> columnInfoList,
            long startSeconds,
            long startNanos,
            int numSerializedDataColumnsExpected
    ) {
        // create data structure for validating query result
        Map<String, IngestionStreamInfo> validationMap = new TreeMap<>();

        for (IngestionColumnInfo columnInfo : columnInfoList) {
            final IngestionStreamInfo streamInfo =
                    ingestDataBidiStream(
                            startSeconds,
                            startNanos,
                            columnInfo);
            verifyIngestionHandling(
                    streamInfo.paramsList,
                    streamInfo.requestList,
                    streamInfo.responseList,
                    numSerializedDataColumnsExpected);
            validationMap.put(columnInfo.columnName, streamInfo);
        }

        return validationMap;
    }

    protected IngestionScenarioResult simpleIngestionScenario() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

        // register providers used by scenario
        final String gccProviderName = GCC_INGESTION_PROVIDER;
        final String gccProviderId = registerProvider(gccProviderName, null);
        final String bpmProviderName = BPM_INGESTION_PROVIDER;
        final String bpmProviderId = registerProvider(bpmProviderName, null);

        List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();

        // create tags, attributes, and events for use in events
        final List<String> tags = List.of("gauges", "pumps");
        final Map<String, String> attributes = Map.of("sector", "01", "subsystem", "vacuum");
        final String eventDescription = "Vacuum pump maintenance";
        final long eventStartSeconds = startSeconds;
        final long eventStartNanos = startNanos;
        final long eventStopSeconds = startSeconds + 1;
        final long eventStopNanos = 0L;

        // create data for 10 sectors, each containing 3 gauges and 3 bpms
        final Set<String> gccPvNames = new TreeSet<>();
        final Set<String> bpmPvNames = new TreeSet<>();
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
                                gccProviderId,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false,
                                false, tags,
                                attributes,
                                eventDescription,
                                eventStartSeconds,
                                eventStartNanos,
                                eventStopSeconds,
                                eventStopNanos);
                gccPvNames.add(gccName);
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
                                bpmProviderId,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false, false, null, null, null, null, null, null, null);
                bpmPvNames.add(bpmName);
                ingestionColumnInfoList.add(columnInfoTenths);
            }
        }
        
        // build map of provider info
        final Map<String, IngestionProviderInfo> providerInfoMap = new HashMap<>();
        final IngestionProviderInfo gccProviderInfo = new IngestionProviderInfo(
                gccProviderId,
                gccPvNames,
                startSeconds,
                startNanos,
                startSeconds + 10 - 1,
                0L,
                3 * 10 * 10);
        providerInfoMap.put(gccProviderName, gccProviderInfo);
        final IngestionProviderInfo bpmProviderInfo = new IngestionProviderInfo(
                bpmProviderId,
                bpmPvNames,
                startSeconds,
                startNanos,
                startSeconds + 10 - 1,
                0L,
                3 * 10 * 10);
        providerInfoMap.put(bpmProviderName, bpmProviderInfo);

        Map<String, IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestDataBidiStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, 0);
        }

        return new IngestionScenarioResult(providerInfoMap, validationMap);
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
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return new QueryRequestStatusResult(responseObserver.getRequestStatusList(), false);
    }

    protected void sendAndVerifyQueryRequestStatus(
            IngestionTestBase.QueryRequestStatusParams params,
            IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryRequestStatusRequest request = IngestionTestBase.buildQueryRequestStatusRequest(params);
        QueryRequestStatusResult result = sendQueryRequestStatus(request, expectReject, expectedRejectMessage);
        final List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> requestStatusList = result.statusList;

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

    record SubscriptionResponseColumn(
            DataColumn dataColumn,
            boolean isSerialized
    ) {
    }

    private void addPvTimestampColumnMapEntry(
            Map<String, TimestampMap<SubscriptionResponseColumn>> pvTimestampColumnMap,
            long responseSeconds,
            long responseNanos,
            DataColumn dataColumn,
            boolean isSerialized
    ) {
        final String pvName = dataColumn.getName();
        TimestampMap<SubscriptionResponseColumn> pvTimestampMap = pvTimestampColumnMap.get(pvName);
        if (pvTimestampMap == null) {
            pvTimestampMap = new TimestampMap<>();
            pvTimestampColumnMap.put(pvName, pvTimestampMap);
        }
        pvTimestampMap.put(responseSeconds, responseNanos, new SubscriptionResponseColumn(dataColumn, isSerialized));
    }

    protected void verifySubscribeDataResponse(
            IngestionTestBase.SubscribeDataResponseObserver responseObserver,
            List<String> pvNameList,
            Map<String, IngestionStreamInfo> ingestionValidationMap,
            int numExpectedSerializedColumns
    ) {
        // wait for completion of API method response stream and confirm not in error state
        responseObserver.awaitResponseLatch();
        assertFalse(responseObserver.isError());

        // get subscription responses for verification of expected contents
        final List<SubscribeDataResponse> responseList = responseObserver.getResponseList();

        // create map of response DataColumns organized by PV name and timestamp
        Map<String, TimestampMap<SubscriptionResponseColumn>> pvTimestampColumnMap = new HashMap<>();
        int responseColumnCount = 0;
        for (SubscribeDataResponse response : responseList) {

            assertTrue(response.hasSubscribeDataResult());
            final SubscribeDataResponse.SubscribeDataResult responseResult = response.getSubscribeDataResult();
            final DataTimestamps responseDataTimestamps = responseResult.getDataTimestamps();
            final DataTimestampsUtility.DataTimestampsModel responseTimestampsModel = 
                    new DataTimestampsUtility.DataTimestampsModel(responseDataTimestamps);
            final long responseSeconds = responseTimestampsModel.getFirstTimestamp().getEpochSeconds();
            final long responseNanos = responseTimestampsModel.getFirstTimestamp().getNanoseconds();

            // add entries to pvTimestampColumnMap for regular DataColumns in response
            for (DataColumn dataColumn : responseResult.getDataColumnsList()) {
                addPvTimestampColumnMapEntry(
                        pvTimestampColumnMap, responseSeconds, responseNanos, dataColumn, false);
                responseColumnCount = responseColumnCount + 1;
            }

            // add entries to pvTimestampColumnMap for SerializedDataColumns in response
            for (SerializedDataColumn serializedDataColumn : responseResult.getSerializedDataColumnsList()) {
                DataColumn deserializedDataColumn = null;
                try {
                    deserializedDataColumn = DataColumn.parseFrom(serializedDataColumn.getDataColumnBytes());
                } catch (InvalidProtocolBufferException e) {
                    fail("exception deserializing response SerializedDataColumn: " + e.getMessage());
                }
                assertNotNull(deserializedDataColumn);
                addPvTimestampColumnMapEntry(
                        pvTimestampColumnMap, responseSeconds, responseNanos, deserializedDataColumn, true);
                responseColumnCount = responseColumnCount + 1;
            }

        }

        // confirm that we received the expected DataColumns in subscription responses
        // by comparing to ingestion requests for subscribed PVs
        int requestColumnCount = 0;
        int serializedColumnCount = 0;
        for (String pvName : pvNameList) {
            for (IngestDataRequest pvIngestionRequest : ingestionValidationMap.get(pvName).requestList) {

                // check that pvTimestampColumnMap contains an entry for each PV column in request's data frame
                final IngestDataRequest.IngestionDataFrame requestFrame = pvIngestionRequest.getIngestionDataFrame();
                final DataTimestamps requestDataTimestamps = requestFrame.getDataTimestamps();
                final DataTimestampsUtility.DataTimestampsModel requestTimestampsModel = 
                        new DataTimestampsUtility.DataTimestampsModel(requestDataTimestamps);
                final long requestSeconds = requestTimestampsModel.getFirstTimestamp().getEpochSeconds();
                final long requestNanos = requestTimestampsModel.getFirstTimestamp().getNanoseconds();
                final TimestampMap<SubscriptionResponseColumn> responsePvTimestampMap = pvTimestampColumnMap.get(pvName);
                assertNotNull(responsePvTimestampMap);
                final SubscriptionResponseColumn responsePvTimestampColumn =
                        responsePvTimestampMap.get(requestSeconds, requestNanos);
                assertNotNull(responsePvTimestampColumn);
                final DataColumn responseDataColumn = responsePvTimestampColumn.dataColumn;
                final boolean responseColumnIsSerialized = responsePvTimestampColumn.isSerialized();

                // check response received for each regular DataColumns for subscribed PV in request
                for (DataColumn requestDataColumn : requestFrame.getDataColumnsList()) {
                    // ignore ingestion request columns not for this PV, which shouldn't be the case, but...
                    if ( ! requestDataColumn.getName().equals(pvName)) {
                        continue;
                    }
                    assertEquals(requestDataColumn, responseDataColumn);
                    assertFalse(responseColumnIsSerialized);
                    requestColumnCount = requestColumnCount + 1;
                }

                for (SerializedDataColumn requestSerializedColumn : requestFrame.getSerializedDataColumnsList()) {
                    // ignore ingestion request columns not for this PV, which shouldn't be the case, but...
                    if ( ! requestSerializedColumn.getName().equals(pvName)) {
                        continue;
                    }
                    try {
                        assertEquals(DataColumn.parseFrom(requestSerializedColumn.getDataColumnBytes()), responseDataColumn);
                    } catch (InvalidProtocolBufferException e) {
                        fail("exception deserializing request SerializedDatacolumn: " + e.getMessage());
                    }
                    assertTrue(responseColumnIsSerialized);
                    requestColumnCount = requestColumnCount + 1;
                    serializedColumnCount = serializedColumnCount + 1;
                }

            }
        }
        assertEquals(requestColumnCount, responseColumnCount);
        assertEquals(numExpectedSerializedColumns, serializedColumnCount);
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
        new Thread(subscribeDataCall.requestObserver::onCompleted).start();

        // wait for ack response stream to close
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver;
        responseObserver.awaitCloseLatch();
    }

    protected QueryTableResponse.TableResult sendQueryTable(
            QueryTableRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryTableResponseObserver responseObserver =
                new QueryTestBase.QueryTableResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            final QueryTableResponse response = responseObserver.getQueryResponse();
            assertTrue(response.hasTableResult());
            return response.getTableResult();
        }
    }

    protected QueryTableResponse.TableResult queryTable(
            QueryTestBase.QueryTableRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryTableRequest request = QueryTestBase.buildQueryTableRequest(params);
        return sendQueryTable(request, expectReject, expectedRejectMessage);
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
        final QueryTableResponse.ColumnTable resultColumnTable = tableResult.getColumnTable();
        
        if (numRowsExpected == 0) {
            assertEquals(0, resultColumnTable.getDataColumnsCount());
            assertFalse(resultColumnTable.hasDataTimestamps());
            return;
        }

        final List<Timestamp> timestampList =
                resultColumnTable.getDataTimestamps().getTimestampList().getTimestampsList();
        assertEquals(numRowsExpected, timestampList.size());
        assertEquals(pvNameList.size(), resultColumnTable.getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();

            // check that timestamp is in query time range
            assertTrue(
                    (timestampSeconds > params.beginTimeSeconds())
                            || (timestampSeconds == params.beginTimeSeconds() && timestampNanos >= params.beginTimeNanos()));
            assertTrue(
                    (timestampSeconds < params.endTimeSeconds())
                            || (timestampSeconds == params.endTimeSeconds() && timestampNanos <= params.endTimeNanos()));

            for (DataColumn dataColumn : resultColumnTable.getDataColumnsList()) {
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
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
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
        final QueryTableResponse.TableResult tableResult = queryTable(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(tableResult);
            return;
        }

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
        final QueryTableResponse.TableResult tableResult = queryTable(params, false, "");

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

        if (numRowsExpected == 0) {
            assertEquals(0, resultRowMapTable.getColumnNamesCount());
            assertEquals(0, resultRowMapTable.getRowsCount());
            return;
        }

        // verify result column names matches list of pv names plus timestamp column
        final List<String> resultColumnNamesList = resultRowMapTable.getColumnNamesList();
        assertTrue(resultColumnNamesList.contains(QueryTableDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME));
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
                    resultRowValueMap.get(QueryTableDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
            assertTrue(resultRowTimestampDataValue.hasTimestampValue());
            final Timestamp resultRowTimestamp = resultRowTimestampDataValue.getTimestampValue();

            // check that timestamp is in query time range
            final long resultRowSeconds = resultRowTimestamp.getEpochSeconds();
            final long resultRowNanos = resultRowTimestamp.getNanoseconds();
            assertTrue(
                    (resultRowSeconds > params.beginTimeSeconds())
                            || (resultRowSeconds == params.beginTimeSeconds() && resultRowNanos >= params.beginTimeNanos()));
            assertTrue(
                    (resultRowSeconds < params.endTimeSeconds())
                            || (resultRowSeconds == params.endTimeSeconds() && resultRowNanos <= params.endTimeNanos()));

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
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
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
        final QueryTableResponse.TableResult tableResult =
                queryTable(params, expectReject, expectedRejectMessage);

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
        final QueryTableResponse.TableResult tableResult = queryTable(params, false, "");

        // validate query result contents in tableResult
        verifyQueryTableRowResult(params, tableResult, numRowsExpected, expectedPvNameMatches, validationMap);
    }

    private void verifyQueryDataResult(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, IngestionStreamInfo> validationMap,
            List<DataBucket> dataBucketList
    ) {
        final List<String> pvNames = params.columnNames();
        final long beginSeconds = params.beginTimeSeconds();
        final long beginNanos = params.beginTimeNanos();
        final long endSeconds = params.endTimeSeconds();
        final long endNanos = params.endTimeNanos();

        // build map of buckets in query response for vallidation
        Map<String, TimestampMap<DataBucket>> responseBucketMap = new TreeMap<>();
        for (DataBucket dataBucket : dataBucketList) {

            // get column name from either regular DataColumn or SerializedDataColumn
            String bucketColumnName = "";
            if (dataBucket.hasDataColumn()) {
                bucketColumnName = dataBucket.getDataColumn().getName();
            } else if (dataBucket.hasSerializedDataColumn()) {
                bucketColumnName = dataBucket.getSerializedDataColumn().getName();
            }
            assertFalse(bucketColumnName.isBlank());

            final DataTimestampsUtility.DataTimestampsModel bucketDataTimestampsModel
                    = new DataTimestampsUtility.DataTimestampsModel(dataBucket.getDataTimestamps());
            final Timestamp bucketStartTimestamp = bucketDataTimestampsModel.getFirstTimestamp();
            final long bucketStartSeconds = bucketStartTimestamp.getEpochSeconds();
            final long bucketStartNanos = bucketStartTimestamp.getNanoseconds();
            TimestampMap<DataBucket> columnTimestampMap =
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
        int serializedDataColumnCount = 0;
        for (var validationMapEntry : validationMap.entrySet()) {
            final String columnName = validationMapEntry.getKey();
            if ( ! pvNames.contains(columnName)) {
                // skip pv if not included in query
                continue;
            }
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
                    if ((columnBucketInfo.endSeconds < beginSeconds)
                            || ((columnBucketInfo.endSeconds == beginSeconds) && (columnBucketInfo.endNanos < beginNanos))) {
                        // bucket ends before query start time
                        continue;
                    }

                    // find the response bucket corresponding to the expected bucket
                    final DataBucket responseBucket =
                            responseBucketMap.get(columnName).get(columnBucketInfo.startSeconds, beginNanos);
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

                    // get DataColumn from bucket, or parse from bucket's SerializedDataColumn
                    DataColumn responseDataColumn = null;
                    if (responseBucket.hasDataColumn()) {
                        responseDataColumn = responseBucket.getDataColumn();

                    } else if (responseBucket.hasSerializedDataColumn()) {
                        try {
                            responseDataColumn = DataColumn.parseFrom(responseBucket.getSerializedDataColumn().getDataColumnBytes());
                        } catch (InvalidProtocolBufferException e) {
                            fail("exception parsing DataColumn from SerializedDataColumn: " + e.getMessage());
                        }
                        serializedDataColumnCount = serializedDataColumnCount + 1;

                    } else {
                        fail("responseBucket doesn't contain either DataColumn or SerializedDataColumn");
                    }
                    assertNotNull(responseDataColumn);

                    // compare expected and actual data values
                    for (DataValue responseDataValue : responseDataColumn.getDataValuesList()) {

                        final double actualDataValue = responseDataValue.getDoubleValue();

                        Object expectedValue = columnBucketInfo.dataValues.get(valueIndex);
                        assertTrue(expectedValue instanceof Double);
                        Double expectedDataValue = (Double) expectedValue;
                        assertEquals(expectedDataValue, actualDataValue, 0.0);

                        valueIndex = valueIndex + 1;
                    }

                    // check tags
                    if (columnBucketInfo.tags != null) {
                        assertEquals(columnBucketInfo.tags, responseBucket.getTagsList());
                    } else {
                        assertTrue(responseBucket.getTagsList().isEmpty());
                    }

                    // check attributes
                    if (columnBucketInfo.attributes != null) {
                        assertEquals(
                                columnBucketInfo.attributes,
                                AttributesUtility.attributeMapFromList(responseBucket.getAttributesList()));
                    } else {
                        assertTrue(responseBucket.getAttributesList().isEmpty());
                    }

                    // check event metadata
                    if (columnBucketInfo.eventDescription != null) {
                        assertEquals(
                                columnBucketInfo.eventDescription,
                                responseBucket.getEventMetadata().getDescription());
                    }
                    if (columnBucketInfo.eventStartSeconds != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStartSeconds,
                                responseBucket.getEventMetadata().getStartTimestamp().getEpochSeconds());
                    }
                    if (columnBucketInfo.eventStartNanos != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStartNanos,
                                responseBucket.getEventMetadata().getStartTimestamp().getNanoseconds());
                    }
                    if (columnBucketInfo.eventStopSeconds != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStopSeconds,
                                responseBucket.getEventMetadata().getStopTimestamp().getEpochSeconds());
                    }
                    if (columnBucketInfo.eventStopNanos != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStopNanos,
                                responseBucket.getEventMetadata().getStopTimestamp().getNanoseconds());
                    }

                    validatedBuckets = validatedBuckets + 1;
                }
            }
        }

        // check that we validated all buckets returned by the query, and that query returned expected number of buckets
        assertEquals(dataBucketList.size(), validatedBuckets);
        assertEquals(numBucketsExpected, dataBucketList.size());
        assertEquals(numSerializedDataColumnsExpected, serializedDataColumnCount);
    }

    protected List<DataBucket> sendQueryData(
            QueryDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataUnaryObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataBucketList();
    }

    protected List<DataBucket> queryData(
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryData(request, expectReject, expectedRejectMessage);
    }

    protected List<DataBucket> sendAndVerifyQueryData(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryData(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(dataBucketList.isEmpty());
            return dataBucketList;
        }

        verifyQueryDataResult(
                numBucketsExpected, numSerializedDataColumnsExpected, params, validationMap, dataBucketList);

        return dataBucketList;
    }

    protected List<DataBucket> sendQueryDataStream(
            QueryDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataStreamObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
            return responseObserver.getDataBucketList();
        }
    }

    protected List<DataBucket> queryDataStream(
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataStream(request, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryDataStream(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryDataStream(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(dataBucketList);
            return;
        }

        verifyQueryDataResult(
                numBucketsExpected, numSerializedDataColumnsExpected, params, validationMap, dataBucketList);
    }

    protected List<DataBucket> sendQueryDataBidiStream(
            QueryDataRequest request,
            int numBucketsExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataBidiStreamObserver(numBucketsExpected);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            StreamObserver<QueryDataRequest> requestObserver = asyncStub.queryDataBidiStream(responseObserver);
            responseObserver.setRequestObserver(requestObserver);
            requestObserver.onNext(request);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
            return responseObserver.getDataBucketList();
        }
    }

    protected List<DataBucket> queryDataBidiStream(
            int numBucketsExpected,
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataBidiStream(request, numBucketsExpected, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryDataBidiStream(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryDataBidiStream(numBucketsExpected, params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(null, dataBucketList);
            return;
        }

        verifyQueryDataResult(
                numBucketsExpected,
                numSerializedDataColumnsExpected,
                params,
                validationMap,
                dataBucketList);
    }

    protected List<QueryPvMetadataResponse.MetadataResult.PvInfo> sendQueryPvMetadata(
            QueryPvMetadataRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryPvMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryPvMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryPvMetadata(request, responseObserver);
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

    private void sendAndVerifyQueryPvMetadata(
            QueryPvMetadataRequest request,
            List<String> pvNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty) {
        final List<QueryPvMetadataResponse.MetadataResult.PvInfo> pvInfoList =
                sendQueryPvMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject || expectEmpty) {
            assertEquals(0, pvInfoList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(pvNames.size(), pvInfoList.size());

        // build map of pv info list for convenience
        final Map<String, QueryPvMetadataResponse.MetadataResult.PvInfo> pvInfoMap = new HashMap<>();
        for (QueryPvMetadataResponse.MetadataResult.PvInfo columnInfo : pvInfoList) {
            pvInfoMap.put(columnInfo.getPvName(), columnInfo);
        }

        // build list of pv names in response to verify against expected
        final List<String> responsePvNames = new ArrayList<>();
        for (QueryPvMetadataResponse.MetadataResult.PvInfo pvInfo : pvInfoList) {
            responsePvNames.add(pvInfo.getPvName());
        }

        // check that response pvNames are sorted (against list in sorted order)
        assertEquals(pvNames, responsePvNames);

        // check that a PvInfo was received for each name and verify its contents
        for (String pvName : pvNames) {
            final QueryPvMetadataResponse.MetadataResult.PvInfo pvInfo =
                    pvInfoMap.get(pvName);
            assertNotNull(pvInfo);
            assertEquals(pvName, pvInfo.getPvName());
            assertEquals(8, pvInfo.getLastBucketDataTypeCase());
            assertEquals("DOUBLEVALUE", pvInfo.getLastBucketDataType());
            assertEquals(1, pvInfo.getLastBucketDataTimestampsCase());
            assertEquals("SAMPLINGCLOCK", pvInfo.getLastBucketDataTimestampsType());

            // iterate through validationMap to get info for first and last bucket for pv, number of buckets
            IngestionBucketInfo firstBucketInfo = null;
            IngestionBucketInfo lastBucketInfo = null;
            int numBuckets = 0;
            boolean first = true;
            for (var bucketMapEntry : validationMap.get(pvName).bucketInfoMap.entrySet()) {
                final var nanoMap = bucketMapEntry.getValue();
                for (var nanoMapEntry : nanoMap.entrySet()) {
                    numBuckets = numBuckets + 1;
                    if (first) {
                        firstBucketInfo = nanoMapEntry.getValue();
                        first = false;
                    }
                    lastBucketInfo = nanoMapEntry.getValue();
                }
            }

            // verify pvInfo contents for column against last and first bucket details
            assertNotNull(lastBucketInfo);
            assertEquals(lastBucketInfo.intervalNanos, pvInfo.getLastBucketSamplePeriod());
            assertEquals(lastBucketInfo.numValues, pvInfo.getLastBucketSampleCount());
            assertEquals(lastBucketInfo.endSeconds, pvInfo.getLastDataTimestamp().getEpochSeconds());
            assertEquals(lastBucketInfo.endNanos, pvInfo.getLastDataTimestamp().getNanoseconds());
            assertNotNull(firstBucketInfo);
            assertEquals(firstBucketInfo.startSeconds, pvInfo.getFirstDataTimestamp().getEpochSeconds());
            assertEquals(firstBucketInfo.startNanos, pvInfo.getFirstDataTimestamp().getNanoseconds());
            assertEquals(numBuckets, pvInfo.getNumBuckets());

            // check last bucket id
            final String expectedLastBucketId =
                    pvName + "-" + lastBucketInfo.startSeconds + "-" + lastBucketInfo.startNanos;
            assertEquals(expectedLastBucketId, pvInfo.getLastBucketId());

        }
    }

    protected void sendAndVerifyQueryPvMetadata(
            List<String> columnNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty
    ) {
        final QueryPvMetadataRequest request = QueryTestBase.buildQueryPvMetadataRequest(columnNames);
        sendAndVerifyQueryPvMetadata(
                request, columnNames, validationMap, expectReject, expectedRejectMessage, expectEmpty);
    }

    protected void sendAndVerifyQueryPvMetadata(
            String columnNamePattern,
            Map<String, IngestionStreamInfo> validationMap,
            List<String> expectedColumnNames,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty
    ) {
        final QueryPvMetadataRequest request = QueryTestBase.buildQueryPvMetadataRequest(columnNamePattern);
        sendAndVerifyQueryPvMetadata(
                request, expectedColumnNames, validationMap, expectReject, expectedRejectMessage, expectEmpty);
    }

    private static List<QueryProvidersResponse.ProvidersResult.ProviderInfo> sendQueryProviders(
            QueryProvidersRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryProvidersResponseObserver responseObserver =
                new QueryTestBase.QueryProvidersResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviders(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getProviderInfoList();
    }

    protected static void sendAndVerifyQueryProviders(
            QueryTestBase.QueryProvidersRequestParams queryParams,
            int numMatchesExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryProvidersRequest request = QueryTestBase.buildQueryProvidersRequest(queryParams);

        final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> queryResultProviderList =
                sendQueryProviders(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(0, queryResultProviderList.size());
            return;
        }

        // verify query results
        assertEquals(numMatchesExpected, queryResultProviderList.size());

        // confirm that each query result corresponds to search criteria
        for (QueryProvidersResponse.ProvidersResult.ProviderInfo providerInfo : queryResultProviderList) {

            if (queryParams.idCriterion != null) {
                assertEquals(queryParams.idCriterion, providerInfo.getId());
            }

            if (queryParams.textCriterion != null) {
                assertTrue((providerInfo.getName().contains(queryParams.textCriterion)) ||
                        (providerInfo.getDescription().contains(queryParams.textCriterion)));
            }

            if (queryParams.tagsCriterion != null) {
                assertTrue(providerInfo.getTagsList().contains(queryParams.tagsCriterion));
            }

            if (queryParams.attributesCriterionKey != null) {
                assertNotNull(queryParams.attributesCriterionValue);
                final Map<String, String> resultAttributeMap =
                        AttributesUtility.attributeMapFromList(providerInfo.getAttributesList());
                assertEquals(
                        queryParams.attributesCriterionValue,
                        resultAttributeMap.get(queryParams.attributesCriterionKey));
            }
        }
    }

    private static List<QueryProviderMetadataResponse.MetadataResult.ProviderMetadata> sendQueryProviderMetadata(
            QueryProviderMetadataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryProviderMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryProviderMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviderMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getProviderMetadataList();
    }

    protected static void sendAndVerifyQueryProviderMetadata(
            String providerId,
            IngestionProviderInfo providerInfo,
            boolean expectReject,
            String expectedRejectMessage,
            int numMatchesExpected
    ) {

        final QueryProviderMetadataRequest request = QueryTestBase.buildQueryProviderMetadataRequest(providerId);

        final List<QueryProviderMetadataResponse.MetadataResult.ProviderMetadata> providerMetadataList =
                sendQueryProviderMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject || numMatchesExpected == 0) {
            assertEquals(0, providerMetadataList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(numMatchesExpected, providerMetadataList.size());
        final QueryProviderMetadataResponse.MetadataResult.ProviderMetadata responseProviderMetadata =
                providerMetadataList.get(0);
        assertEquals(providerId, responseProviderMetadata.getId());
        assertEquals(providerInfo.numBuckets, responseProviderMetadata.getNumBuckets());
        assertEquals(providerInfo.firstTimeSeconds, responseProviderMetadata.getFirstBucketTime().getEpochSeconds());
        assertEquals(providerInfo.firstTimeNanos, responseProviderMetadata.getFirstBucketTime().getNanoseconds());
        assertEquals(providerInfo.lastTimeSeconds, responseProviderMetadata.getLastBucketTime().getEpochSeconds());
        assertEquals(providerInfo.lastTimeNanos, responseProviderMetadata.getLastBucketTime().getNanoseconds());
        assertEquals(providerInfo.pvNameSet.size(), responseProviderMetadata.getPvNamesCount());
        assertEquals(providerInfo.pvNameSet, new HashSet<>(responseProviderMetadata.getPvNamesList()));
    }

    protected static String sendCreateDataSet(
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

    protected static String sendAndVerifyCreateDataSet(
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
        assertNotNull(dataSetDocument.getCreatedAt());
        final List<String> requestDiffs = dataSetDocument.diffRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.toString(), requestDiffs.isEmpty());

        createDataSetIdParamsMap.put(dataSetId, params);
        createDataSetParamsIdMap.put(params, dataSetId);

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
            AnnotationTestBase.QueryDataSetsParams queryParams,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResult
    ) {
        final QueryDataSetsRequest request =
                AnnotationTestBase.buildQueryDataSetsRequest(queryParams);

        final List<DataSet> resultDataSets = sendQueryDataSets(request, expectReject, expectedRejectMessage);

        if (expectReject || expectedQueryResult.isEmpty()) {
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
                if (requestParams.dataSet().name().equals(resultDataSet.getName())) {
                    found = true;
                    foundDataSet = resultDataSet;
                    break;
                }
            }
            assertTrue(found);
            assertNotNull(foundDataSet);
            final String expectedDataSetId = this.createDataSetParamsIdMap.get(requestParams);

            // check required dataset fields match
            assertTrue(expectedDataSetId.equals(foundDataSet.getId()));
            assertTrue(requestParams.dataSet().description().equals(foundDataSet.getDescription()));
            assertTrue(requestParams.dataSet().ownerId().equals(foundDataSet.getOwnerId()));

            // check that result corresponds to query criteria
            if (queryParams.idCriterion != null) {
                assertEquals(queryParams.idCriterion, foundDataSet.getId());
            }
            if (queryParams.ownerCriterion != null) {
                assertEquals(queryParams.ownerCriterion, foundDataSet.getOwnerId());
            }
            if (queryParams.textCriterion != null) {
                assertTrue(
                        foundDataSet.getName().contains(queryParams.textCriterion)
                                || foundDataSet.getDescription().contains(queryParams.textCriterion));
            }
            if (queryParams.pvNameCriterion != null) {
                boolean foundPvName = false;
                for (DataBlock foundDataSetDataBlock : foundDataSet.getDataBlocksList()) {
                    if (foundDataSetDataBlock.getPvNamesList().contains(queryParams.pvNameCriterion)) {
                        foundPvName = true;
                        break;
                    }
                }
                assertTrue(foundPvName);
            }

            // compare data blocks from result with request
            final AnnotationTestBase.AnnotationDataSet requestDataSet = requestParams.dataSet();
            assertEquals(requestDataSet.dataBlocks().size(), foundDataSet.getDataBlocksCount());
            for (AnnotationTestBase.AnnotationDataBlock requestBlock : requestDataSet.dataBlocks()) {
                boolean responseBlockFound = false;
                for (DataBlock responseBlock : foundDataSet.getDataBlocksList()) {
                    if (
                            (Objects.equals(requestBlock.beginSeconds(), responseBlock.getBeginTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.beginNanos(), responseBlock.getBeginTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.endSeconds(), responseBlock.getEndTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.endNanos(), responseBlock.getEndTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.pvNames(), responseBlock.getPvNamesList()))
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

    protected static String sendCreateAnnotation(
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

    protected static String sendAndVerifyCreateAnnotation(
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
        assertNotNull(annotationDocument.getCreatedAt());
        final List<String> requestDiffs = annotationDocument.diffCreateAnnotationRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.toString(), requestDiffs.isEmpty());

        // validate calculations if specified
        if (params.calculations != null) {
            assertNotNull(annotationDocument.getCalculationsId());
            final CalculationsDocument calculationsDocument =
                    mongoClient.findCalculations(annotationDocument.getCalculationsId());
            assertNotNull(calculationsDocument);
            assertNotNull(calculationsDocument.getCreatedAt());
            final List<String> calculationsDiffs = calculationsDocument.diffCalculations(params.calculations);
            assertNotNull(calculationsDiffs);
            assertTrue(calculationsDiffs.toString(), calculationsDiffs.isEmpty());

        } else {
            assertNull(annotationDocument.getCalculationsId());
        }

        // save annotationId to map for use in validating queryAnnotations() result
        createAnnotationParamsIdMap.put(params, annotationId);

        return annotationId;
    }

    protected static List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendQueryAnnotations(
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

    protected static List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendAndVerifyQueryAnnotations(
            AnnotationTestBase.QueryAnnotationsParams queryParams,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryResult
    ) {
        final QueryAnnotationsRequest request =
                AnnotationTestBase.buildQueryAnnotationsRequest(queryParams);

        final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> resultAnnotations =
                sendQueryAnnotations(request, expectReject, expectedRejectMessage);

        if (expectReject || expectedQueryResult.isEmpty()) {
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
            final String expectedAnnotationId = createAnnotationParamsIdMap.get(requestParams);
            assertTrue(expectedAnnotationId.equals(foundAnnotation.getId()));

            // compare required fields from request against found annotation
            assertTrue(requestParams.ownerId.equals(foundAnnotation.getOwnerId()));
            assertTrue(requestParams.name.equals(foundAnnotation.getName()));
            // use Objects.equals to compare list elements
            assertTrue(Objects.equals(requestParams.dataSetIds, foundAnnotation.getDataSetIdsList()));

            // confirm that query results correspond to specify query criteria
            
            // check IdCriterion
            if (queryParams.idCriterion != null) {
                assertTrue(foundAnnotation.getId().equals(queryParams.idCriterion));
            }
            
            // check OwnerCriterion
            if (queryParams.ownerCriterion != null) {
                assertTrue(foundAnnotation.getOwnerId().equals(queryParams.ownerCriterion));
            }

            // check DataSetCriterion
            if (queryParams.datasetsCriterion != null) {
                assertTrue(foundAnnotation.getDataSetIdsList().contains(queryParams.datasetsCriterion));
            }

            // check AssociatedAnnotationCriterion
            if (queryParams.annotationsCriterion != null) {
                assertTrue(foundAnnotation.getAnnotationIdsList().contains(queryParams.annotationsCriterion));
            }

            // check TextCriterion
            if (queryParams.textCriterion != null) {
                assertTrue(
                        foundAnnotation.getComment().contains(queryParams.textCriterion)
                                || foundAnnotation.getName().contains(queryParams.textCriterion)
                                || foundAnnotation.getEventMetadata().getDescription().contains(queryParams.textCriterion));
            }

            // check TagsCriterion
            if (queryParams.tagsCriterion != null) {
                assertTrue(foundAnnotation.getTagsList().contains(queryParams.tagsCriterion));
            }

            // check AttributesCriterion
            if (queryParams.attributesCriterionKey != null) {
                assertNotNull(queryParams.attributesCriterionValue);
                final Map<String, String> resultAttributeMap =
                        AttributesUtility.attributeMapFromList(foundAnnotation.getAttributesList());
                assertEquals(
                        resultAttributeMap.get(queryParams.attributesCriterionKey), queryParams.attributesCriterionValue);
            }

            // check EventCriterion
            if (queryParams.eventCriterion != null) {
                assertTrue(foundAnnotation.getEventMetadata().getDescription().contains(queryParams.eventCriterion));
            }

            // compare dataset content from result with dataset in database
            for (DataSet responseDataSet : foundAnnotation.getDataSetsList()) {
                final DataSetDocument dbDataSetDocument = mongoClient.findDataSet(responseDataSet.getId());
                final DataSet dbDataSet = dbDataSetDocument.toDataSet();
                assertEquals(dbDataSet.getDataBlocksList().size(), responseDataSet.getDataBlocksCount());
                assertTrue(dbDataSet.getName().equals(responseDataSet.getName()));
                assertTrue(dbDataSet.getOwnerId().equals(responseDataSet.getOwnerId()));
                assertTrue(dbDataSet.getDescription().equals(responseDataSet.getDescription()));
                for (DataBlock dbDataBlock : dbDataSet.getDataBlocksList()) {
                    boolean responseBlockFound = false;
                    for (DataBlock responseBlock : responseDataSet.getDataBlocksList()) {
                        if (
                                (Objects.equals(dbDataBlock.getBeginTime().getEpochSeconds(), responseBlock.getBeginTime().getEpochSeconds()))
                                        && (Objects.equals(dbDataBlock.getBeginTime().getNanoseconds(), responseBlock.getBeginTime().getNanoseconds()))
                                        && (Objects.equals(dbDataBlock.getEndTime().getEpochSeconds(), responseBlock.getEndTime().getEpochSeconds()))
                                        && (Objects.equals(dbDataBlock.getEndTime().getNanoseconds(), responseBlock.getEndTime().getNanoseconds()))
                                        && (Objects.equals(dbDataBlock.getPvNamesList(), responseBlock.getPvNamesList()))
                        ) {
                            responseBlockFound = true;
                            break;
                        }
                    }
                    assertTrue(responseBlockFound);
                }
            }

            // compare calculations content from result with calculations document in database
            if (requestParams.calculations != null) {
                assertTrue(foundAnnotation.hasCalculations());
                final Calculations resultCalculations = foundAnnotation.getCalculations();
                final CalculationsDocument dbCalculationsDocument =
                        mongoClient.findCalculations(resultCalculations.getId());
                try {
                    final Calculations dbCalculations = dbCalculationsDocument.toCalculations();
                    assertEquals(dbCalculations, resultCalculations);
                } catch (DpException e) {
                    fail("exception in CalculationsDocument.toCalculations(): " + e.getMessage());
                }
            }
        }

        return resultAnnotations;
    }

    protected ExportDataResponse.ExportDataResult sendExportData(
            ExportDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(annotationChannel);

        final AnnotationTestBase.ExportDataResponseObserver responseObserver =
                new AnnotationTestBase.ExportDataResponseObserver();

        // start performance measurment timer
        final Instant t0 = Instant.now();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.exportData(request, responseObserver);
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

    private TimestampDataMap getTimestampDataMapForDataset(DataSetDocument dataset) throws DpException {

        final TimestampDataMap expectedDataMap = new TimestampDataMap();
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final MongoCursor<BucketDocument> cursor = mongoClient.findDataBlockBuckets(dataBlock);
            final long beginSeconds = dataBlock.getBeginTime().getSeconds();
            final long beginNanos = dataBlock.getBeginTime().getNanos();
            final long endSeconds = dataBlock.getEndTime().getSeconds();
            final long endNanos = dataBlock.getEndTime().getNanos();
            final TabularDataUtility.TimestampDataMapSizeStats sizeStats =
                    TabularDataUtility.addBucketsToTable(
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

    protected ExportDataResponse.ExportDataResult sendAndVerifyExportData(
            String dataSetId,
            AnnotationTestBase.CreateDataSetParams createDataSetParams,
            CalculationsSpec calculationsSpec,
            Calculations calculationsRequestParams,
            ExportDataRequest.ExportOutputFormat outputFormat,
            int expectedNumBuckets,
            int expectedNumRows,
            Map<String, IngestionStreamInfo> pvValidationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final ExportDataRequest request =
                AnnotationTestBase.buildExportDataRequest(
                        dataSetId,
                        calculationsSpec,
                        outputFormat);

        final ExportDataResponse.ExportDataResult exportResult =
                sendExportData(request, expectReject, expectedRejectMessage);

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
        final Path target = Path.of(exportResult.getFilePath());
        try {
            final BasicFileAttributes attributes = Files.readAttributes(target, BasicFileAttributes.class);
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

        // create list of expected PV column names
        Set<String> expectedPvColumnNames = new HashSet<>();
        DataSetDocument datasetDocument = null;
        List<BucketDocument> datasetBuckets = null;
        if (dataSetId != null) {

            // retrieve dataset for id
            datasetDocument = mongoClient.findDataSet(dataSetId);
            assertNotNull(datasetDocument);

            // retrieve BucketDocuments for specified dataset
            datasetBuckets = mongoClient.findDataSetBuckets(datasetDocument);
            assertEquals(expectedNumBuckets, datasetBuckets.size());

            // generate collection of unique pv names for dataset from creation request params
            // these are the columns expected in the export output file
            for (AnnotationTestBase.AnnotationDataBlock requestDataBlock : createDataSetParams.dataSet().dataBlocks()) {
                expectedPvColumnNames.addAll(requestDataBlock.pvNames());
            }
        }

        // create list of expected calculations column names
        CalculationsDocument calculationsDocument = null;
        List<String> expectedCalculationsColumnNames = new ArrayList<>();
        Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap = null;
        if (calculationsSpec != null) {

            // retrieve calculations for id
            calculationsDocument = mongoClient.findCalculations(calculationsSpec.getCalculationsId());
            assertNotNull(calculationsDocument);

            // add expected column names for calculations
            if (calculationsSpec.getDataFrameColumnsMap().isEmpty()) {

                // add all columns for calculations
                for (Calculations.CalculationsDataFrame requestCalculationsFrame :
                        calculationsRequestParams.getCalculationDataFramesList()) {
                    final String requestCalculationsFrameName = requestCalculationsFrame.getName();
                    for (DataColumn requestCalculationsFrameColumn : requestCalculationsFrame.getDataColumnsList()) {
                        expectedCalculationsColumnNames.add(requestCalculationsFrameColumn.getName());
                    }
                }

            } else {

                // add only columns from map in calculationsSpec
                frameColumnNamesMap = calculationsSpec.getDataFrameColumnsMap();
                for (var frameColumnMapEntry : frameColumnNamesMap.entrySet()) {
                    final String frameName = frameColumnMapEntry.getKey();
                    final List<String> frameColumns = frameColumnMapEntry.getValue().getColumnNamesList();
                    for (String frameColumnName : frameColumns) {
                        expectedCalculationsColumnNames.add(frameColumnName);
                    }
                }
            }
        }

        // create map of calculations column data values for use in verification
        Map<String, TimestampMap<Double>> calculationsValidationMap = new HashMap<>();
        if (calculationsRequestParams != null) {
            for (Calculations.CalculationsDataFrame requestCalculationsFrame :
                    calculationsRequestParams.getCalculationDataFramesList()) {
                final DataTimestamps requestCalculationsFrameDataTimestamps =
                        requestCalculationsFrame.getDataTimestamps();
                for (DataColumn requestCalculationsFrameColumn : requestCalculationsFrame.getDataColumnsList()) {
                    final String requestCalculationsFrameColumnName = requestCalculationsFrameColumn.getName();
                    if (expectedCalculationsColumnNames.contains(requestCalculationsFrameColumnName)) {
                        calculationsValidationMap.put(
                                requestCalculationsFrameColumnName,
                                DataColumnUtility.toTimestampMapDouble(
                                        requestCalculationsFrameColumn, requestCalculationsFrameDataTimestamps));
                    }
                }
            }
        }

        // verify file content for specified output format
        switch (outputFormat) {

            case EXPORT_FORMAT_HDF5 -> {

                final IHDF5Reader reader = HDF5Factory.openForReading(exportResult.getFilePath());

                if (datasetDocument != null) {
                    AnnotationTestBase.verifyDatasetHdf5Content(reader, datasetDocument);
                }

                if (datasetBuckets != null) {
                    for (BucketDocument bucket : datasetBuckets) {
                        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, bucket);
                    }
                }

                if (calculationsDocument != null) {
                    AnnotationTestBase.verifyCalculationsDocumentHdf5Content(
                            reader, calculationsDocument, frameColumnNamesMap);
                }

                reader.close();
            }

            case EXPORT_FORMAT_CSV -> {
                // verify file content against data map
                verifyExportTabularCsv(
                        exportResult,
                        expectedPvColumnNames,
                        pvValidationMap,
                        expectedCalculationsColumnNames,
                        calculationsValidationMap,
                        expectedNumRows);
            }

            case EXPORT_FORMAT_XLSX -> {
                // verify file content against data map
                verifyExportTabularXlsx(
                        exportResult,
                        expectedPvColumnNames,
                        pvValidationMap,
                        expectedCalculationsColumnNames,
                        calculationsValidationMap,
                        expectedNumRows);
            }
        }

        return exportResult;
    }

    public static void verifyExportTabularCsv(
            ExportDataResponse.ExportDataResult exportResult,
            Set<String> expectedPvColumnNames,
            Map<String, IngestionStreamInfo> pvValidationMap,
            List<String> expectedCalculationsColumnNames,
            Map<String, TimestampMap<Double>> calculationsValidationMap,
            int expectedNumRows
    ) {
        // open csv file and create reader
        final Path exportFilePath = Paths.get(exportResult.getFilePath());
        CsvReader<CsvRecord> csvReader = null;
        try {
            csvReader = CsvReader.builder().ofCsvRecord(exportFilePath);
        } catch (IOException e) {
            fail("IOException reading csv file " + exportResult.getFilePath() + ": " + e.getMessage());
        }
        assertNotNull(csvReader);

        final Iterator<CsvRecord> csvRecordIterator = csvReader.iterator();
        final int expectedNumColumns = 2 + expectedPvColumnNames.size() + expectedCalculationsColumnNames.size();

        // verify header row
        List<String> csvColumnHeaders;
        {
            assertTrue(csvRecordIterator.hasNext());
            final CsvRecord csvRecord = csvRecordIterator.next();

            // check number of csv header columns matches expected
            assertEquals(expectedNumColumns, csvRecord.getFieldCount());

            csvColumnHeaders = csvRecord.getFields();

            // check that the csv file contains each of the expected PV columns
            for (String columnName : expectedPvColumnNames) {
                assertTrue(csvColumnHeaders.contains(columnName));
            }

            // check that csv file contains each of the expected calculations columns
            for (String columnName : expectedCalculationsColumnNames) {
                assertTrue(csvColumnHeaders.contains(columnName));
            }
        }

        // verify data rows
        {
            int dataRowCount = 0;
            while (csvRecordIterator.hasNext()) {

                // read row from csv file
                final CsvRecord csvRecord = csvRecordIterator.next();
                assertEquals(expectedNumColumns, csvRecord.getFieldCount());
                final List<String> csvRowValues = csvRecord.getFields();

                // get timestamp column values
                // we don't validate them directly, but by 1) checking the number of expected rows matches and
                // 2) accessing data from validationMap via seconds/nanos
                final long csvSeconds = Long.valueOf(csvRowValues.get(0));
                final long csvNanos = Long.valueOf(csvRowValues.get(1));

                // compare data values from csv file with expected
                for (int columnIndex = 2; columnIndex < csvRowValues.size(); columnIndex++) {

                    // get csv file data value and column name for column index
                    final String csvDataValue = csvRowValues.get(columnIndex);
                    final String csvColumnName = csvColumnHeaders.get(columnIndex);

                    Double expectedColumnDoubleValue = null;
                    if (expectedPvColumnNames.contains(csvColumnName)) {
                        // check expected data value for PV column
                        final TimestampMap<Double> columnValueMap = pvValidationMap.get(csvColumnName).valueMap;
                        expectedColumnDoubleValue = columnValueMap.get(csvSeconds, csvNanos);

                    } else if (expectedCalculationsColumnNames.contains(csvColumnName)) {
                        // check expected data value for calculations column
                        final TimestampMap<Double> columnValueMap = calculationsValidationMap.get(csvColumnName);
                        expectedColumnDoubleValue = columnValueMap.get(csvSeconds, csvNanos);

                    } else {
                        fail("unexpected export column (neither PV nor calculations): " + csvColumnName);
                    }
                }
                dataRowCount = dataRowCount + 1;
            }
            assertEquals(expectedNumRows, dataRowCount);
        }
    }

    public static void verifyExportTabularXlsx(
            ExportDataResponse.ExportDataResult exportResult,
            Set<String> expectedPvColumnNames,
            Map<String, IngestionStreamInfo> pvValidationMap,
            List<String> expectedCalculationsColumnNames,
            Map<String, TimestampMap<Double>> calculationsValidationMap,
            int expectedNumRows
    ) {
        // open excel file
        OPCPackage filePackage = null;
        try {
            filePackage = OPCPackage.open(new File(exportResult.getFilePath()));
        } catch (InvalidFormatException e) {
            fail(
                    "InvalidFormatException opening package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());
        }
        assertNotNull(filePackage);

        // open excel workbook
        XSSFWorkbook fileWorkbook = null;
        try {
            fileWorkbook = new XSSFWorkbook(filePackage);
        } catch (IOException e) {
            fail(
                    "IOException creating workbook from excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
        assertNotNull(fileWorkbook);

        // open worksheet and create iterator
        Sheet fileSheet = fileWorkbook.getSheetAt(0);
        assertNotNull(fileSheet);
        final Iterator<Row> fileRowIterator = fileSheet.rowIterator();
        assertTrue(fileRowIterator.hasNext());

        final int expectedNumColumns = 2 + expectedPvColumnNames.size() + expectedCalculationsColumnNames.size();

        // verify header row from file
        List<String> fileColumnHeaders = new ArrayList<>();
        {
            final Row fileHeaderRow = fileRowIterator.next();
            assertNotNull(fileHeaderRow);

            // check number of header columns matches expected
            assertEquals(expectedNumColumns, fileHeaderRow.getLastCellNum());

            // build list of actual column headers
            for (int columnIndex = 0; columnIndex < fileHeaderRow.getLastCellNum(); columnIndex++) {
                final String fileHeaderValue = fileHeaderRow.getCell(columnIndex).getStringCellValue();
                fileColumnHeaders.add(fileHeaderValue);
            }

            // check that list of actual headers contains each of the expected PV column headers
            for (String columnName : expectedPvColumnNames) {
                assertTrue(fileColumnHeaders.contains(columnName));
            }

            // check list of actual headers contains each of the expected calculations column headers
            for (String columnName : expectedCalculationsColumnNames) {
                assertTrue(fileColumnHeaders.contains(columnName));
            }
        }

        // verify data rows from file
        {
            int dataRowCount = 0;
            while (fileRowIterator.hasNext()) {

                // read row from excel file
                final Row fileDataRow = fileRowIterator.next();
                assertEquals(expectedNumColumns, fileDataRow.getLastCellNum());

                // get timestamp column values
                // we don't validate them directly, but by 1) checking the number of expected rows matches and
                // 2) accessing data from validationMap via seconds/nanos
                final long fileSeconds = Double.valueOf(fileDataRow.getCell(0).getNumericCellValue()).longValue();
                final long fileNanos = Double.valueOf(fileDataRow.getCell(1).getNumericCellValue()).longValue();

                // verify data columns
                for (int fileColumnIndex = 2; fileColumnIndex < fileDataRow.getLastCellNum(); fileColumnIndex++) {

                    // get column data value and corresponding column name from file
                    final String fileColumnName = fileColumnHeaders.get(fileColumnIndex);
                    final Cell fileCell = fileDataRow.getCell(fileColumnIndex);
                    Double fileColumnDoubleValue = null;
                    if ( ! fileCell.getCellType().equals(CellType.STRING) ) {
                        fileColumnDoubleValue = Double.valueOf(fileCell.getNumericCellValue()).doubleValue();
                    }

                    Double expectedColumnDoubleValue = null;
                    if (expectedPvColumnNames.contains(fileColumnName)) {
                        // get expected data value for column (we assume value is double)
                        final TimestampMap<Double> columnValueMap = pvValidationMap.get(fileColumnName).valueMap;
                        expectedColumnDoubleValue = columnValueMap.get(fileSeconds, fileNanos);

                    } else if (expectedCalculationsColumnNames.contains(fileColumnName)) {
                        // check expected data value for calculations column
                        final TimestampMap<Double> columnValueMap = calculationsValidationMap.get(fileColumnName);
                        expectedColumnDoubleValue = columnValueMap.get(fileSeconds, fileNanos);

                    } else {
                        fail("unexpected export column (neither PV nor calculations): " + fileColumnName);
                    }

                    if (expectedColumnDoubleValue != null) {
                        assertEquals(expectedColumnDoubleValue, fileColumnDoubleValue, 0);
                    } else {
                        assertEquals(null, fileColumnDoubleValue);
                    }

// Keeping this code around in case we want to handle expectedDataValue with other type than Double.
//                    switch (expectedDataValue.getValueCase()) {
//                        case STRINGVALUE -> {
//                            assertEquals(expectedDataValue.getStringValue(), fileCell.getStringCellValue());
//                        }
//                        case BOOLEANVALUE -> {
//                            assertEquals(expectedDataValue.getBooleanValue(), fileCell.getBooleanCellValue());
//                        }
//                        case UINTVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getUintValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
//                        }
//                        case ULONGVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getUlongValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
//                        }
//                        case INTVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getIntValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
//                        }
//                        case LONGVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getLongValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
//                        }
//                        case FLOATVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getFloatValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).floatValue(),
//                                    0.0);
//                        }
//                        case DOUBLEVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getDoubleValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).doubleValue(),
//                                    0);
//                        }
//                        case BYTEARRAYVALUE -> {
//                        }
//                        case ARRAYVALUE -> {
//                        }
//                        case STRUCTUREVALUE -> {
//                        }
//                        case IMAGEVALUE -> {
//                        }
//                        case TIMESTAMPVALUE -> {
//                        }
//                        case VALUE_NOT_SET -> {
//                        }
//                        default -> {
//                            assertEquals(expectedDataValue.toString(), fileCell.getStringCellValue());
//                        }
//                    }
                }

                dataRowCount = dataRowCount + 1;
            }

            assertEquals(expectedNumRows, dataRowCount);
        }

        // close excel file
        try {
            filePackage.close();
        } catch (IOException e) {
            fail(
                    "IOException closing package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
    }

}
