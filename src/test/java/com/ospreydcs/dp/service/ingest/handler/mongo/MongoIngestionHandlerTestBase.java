package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.EventMetadataDocument;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.IngestDataJob;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MongoIngestionHandlerTestBase extends IngestionTestBase {

    protected static MongoIngestionHandler handler = null;
    protected static TestClientInterface clientTestInterface = null;
    private static String collectionNamePrefix = null;

    protected interface TestClientInterface extends MongoIngestionClientInterface {
        public BucketDocument findBucketWithId(String id);
        public List<RequestStatusDocument> findRequestStatusList(Integer providerId, String requestId);
    }

    /**
     * Sets up for jUnit test execution.
     *
     * @throws Exception
     */
    public static void setUp(MongoIngestionHandler handler, TestClientInterface clientInterface) throws Exception {
        System.out.println("setUp");
        MongoIngestionHandlerTestBase.handler = handler;
        clientTestInterface = clientInterface;
        assertTrue("dbHandler init failed", clientTestInterface.init());
    }

    /**
     * Cleans up after jUnit test execution.
     * @throws Exception
     */
    public static void tearDown() throws Exception {
        System.out.println("tearDown");
        assertTrue("dbHandler fini failed", clientTestInterface.fini());
        handler = null;
        clientTestInterface = null;
        collectionNamePrefix = null;
    }

    private static String getTestCollectionNamePrefix() {
        if (collectionNamePrefix == null) {
            collectionNamePrefix = "test-" + System.currentTimeMillis() + "-";
        }
        return collectionNamePrefix;
    }

    protected static String getTestCollectionNameBuckets() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_BUCKETS;
    }

    protected static String getTestCollectionNameRequestStatus() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_REQUEST_STATUS;
    }

    private RequestStatusDocument findRequestStatus(Integer providerId, String requestId, String status) {
        List<RequestStatusDocument> matchingDocuments =
                clientTestInterface.findRequestStatusList(providerId, requestId);
        RequestStatusDocument statusDocument = null;
        for (RequestStatusDocument document : matchingDocuments) {
            if (document.getStatus().equals(status)) {
                return document;
            }
        }
        return null;
    }

    private void verifyFailedRequest(
            IngestionRequestParams params, String status, String statusMsg, boolean checkBuckets) {

        if (checkBuckets) {
            // check database contents, no buckets should be created
            int columnIndex = 0;
            long firstSeconds = params.samplingClockStartSeconds;
            long firstNanos = params.samplingClockStartNanos;
            for (String columnName : params.columnNames) {
                String id = columnName + "-" + firstSeconds + "-" + firstNanos;
                BucketDocument bucket = clientTestInterface.findBucketWithId(id);
                assertTrue("unexpected bucket found with id: " + id,
                        bucket == null);

                columnIndex = columnIndex + 1;
            }
        }

        // check database contents for request status document with specified status
        RequestStatusDocument statusDocument =
                findRequestStatus(params.providerId, params.requestId, status);
        assertTrue(statusDocument != null);
        assertTrue(statusDocument.getIdsCreated().size() == 0);
        assertTrue(statusDocument.getUpdateTime() != null);
        assertTrue(statusDocument.getMsg().contains(statusMsg));
    }

    private void verifySuccessfulRequest(IngestionRequestParams params) {

        // get status document
        final RequestStatusDocument statusDocument =
                findRequestStatus(
                        params.providerId, params.requestId, BsonConstants.BSON_VALUE_STATUS_SUCCESS);

        // check bucket in database for each column in request
        final long firstSeconds = params.samplingClockStartSeconds;
        final long firstNanos = params.samplingClockStartNanos;
        final long sampleIntervalNanos = params.samplingClockPeriodNanos;
        final int numSamples = params.samplingClockCount;
        final Instant startInstant = Instant.ofEpochSecond(firstSeconds, firstNanos);
        int columnIndex = 0;
        for (String columnName : params.columnNames) {
            final String id = columnName + "-" + firstSeconds + "-" + firstNanos;
            final List<Object> columnDataList = params.values.get(columnIndex);
            final Instant lastInstant =
                    startInstant.plusNanos(sampleIntervalNanos * (numSamples - 1));
            final long lastSeconds = lastInstant.getEpochSecond();
            final long lastNanos = lastInstant.getNano();
            final BucketDocument bucket = clientTestInterface.findBucketWithId(id);
            assertTrue(bucket != null);
            assertTrue(statusDocument.getIdsCreated().contains(id));
            assertTrue(bucket.getPvName().equals(columnName));
            assertTrue(bucket.getFirstSeconds() == firstSeconds);
            assertTrue(bucket.getFirstNanos() == firstNanos);
            assertTrue(bucket.getLastSeconds() == lastSeconds);
            assertTrue(bucket.getLastNanos() == lastNanos);

            // compare column data values to expected
            final DataColumn dataColumn = bucket.readDataColumnContent();
            int dataValueIndex = 0;
            for (Object columnValue : columnDataList) {
                if (columnValue instanceof Double) {
                    assertEquals((Double) columnValue, dataColumn.getDataValues(dataValueIndex).getDoubleValue(), 0.0);
                } else if (columnValue instanceof Long) {
                    assertEquals(columnValue, dataColumn.getDataValues(dataValueIndex).getLongValue());
                } else if (columnValue instanceof String) {
                    assertEquals(columnValue, dataColumn.getDataValues(dataValueIndex).getStringValue());
                } else if (columnValue instanceof Boolean) {
                    assertEquals(columnValue, dataColumn.getDataValues(dataValueIndex).getBooleanValue());
                } else if (columnValue instanceof List) {
                    int rowIndex = 0;
                    final List listDataValue = (List) columnValue;
                    final Object firstListValue = listDataValue.get(rowIndex);
                    if (firstListValue instanceof Double) {
                        // compare array of values to expected
                        for (Double doubleValue : (List<Double>) listDataValue) {
                            assertEquals(
                                    (Double) listDataValue.get(rowIndex),
                                    dataColumn
                                            .getDataValues(dataValueIndex)
                                            .getArrayValue()
                                            .getDataValues(rowIndex)
                                            .getDoubleValue(),
                                    0.0);
                            rowIndex = rowIndex + 1;
                        }
                    }
                } else {
                    fail("unexpected data value type: " + columnValue.getClass().getCanonicalName());
                }

                dataValueIndex = dataValueIndex + 1;
            }

            assertTrue(bucket.getSamplePeriod() == sampleIntervalNanos);
            assertTrue(bucket.getSampleCount() == numSamples);
            assertTrue(bucket.getAttributeMap().equals(params.attributes));
            final EventMetadataDocument eventMetadataDocument = bucket.getEventMetadata();
            assertTrue(eventMetadataDocument.getDescription().equals(params.eventDescription));
            assertTrue(eventMetadataDocument.getStartSeconds() == params.eventStartSeconds);
            assertTrue(eventMetadataDocument.getStartNanos() == params.eventStartNanos);
            assertTrue(eventMetadataDocument.getStopSeconds() == params.eventStopSeconds);
            assertTrue(eventMetadataDocument.getStopNanos() == params.eventStopNanos);

            columnIndex = columnIndex + 1;
        }

        // check database contents for request status (success) document
        assertTrue(statusDocument.getIdsCreated().size() == params.columnNames.size());
        assertTrue(statusDocument.getUpdateTime() != null);
        assertTrue(statusDocument.getMsg().isEmpty());
    }

    public void testHandleIngestionRequestReject() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-2";
        String pvName = "pv_01";
        List<String> columnNames = Arrays.asList(pvName);
        double value1 = 12.34;
        double value2 = 42.00;
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 3_000_000L; // offset nanos so that bucket id is different than previous test
        Instant startInstant = Instant.ofEpochSecond(firstSeconds, firstNanos);
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        null,
                        null);
        params.setRequestTime(false); // this would cause a reject for missing request time, but we aren't really validating the request
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        String rejectMsg = "requestTime must be specified";
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(
                        request,
                        null,
                        true,
                        rejectMsg); // force request to be marked as reject
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertTrue("error flag not set", result.isError);
        verifyFailedRequest(params, BsonConstants.BSON_VALUE_STATUS_REJECTED, rejectMsg, true);
    }

    /**
     * Tests data type mismatch for column values.
     */
    public void testHandleIngestionRequestErrorDataTypeMismatch() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-8";
        String pvName = "pv_08";
        List<String> columnNames = Arrays.asList(pvName);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.ARRAY_DOUBLE,
                        null, // don't set any column values, we're going to override
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        null,
                        null);

        // override the column data with both string and float data, to trigger mismatch exception
        List<DataColumn> dataColumnList = new ArrayList<>();
        DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
        dataColumnBuilder.setName(pvName);
        DataValue stringValue = DataValue.newBuilder().setStringValue("junk").build();
        dataColumnBuilder.addDataValues(stringValue);
        DataValue doubleValue = DataValue.newBuilder().setDoubleValue(12.34).build();
        dataColumnBuilder.addDataValues(doubleValue);
        dataColumnList.add(dataColumnBuilder.build());

        IngestDataRequest request = buildIngestionRequest(params, dataColumnList);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertTrue("error flag is not set", result.isError);
        verifyFailedRequest(
                params,
                BsonConstants.BSON_VALUE_STATUS_ERROR,
                "data type mismatch: DOUBLEVALUE expected: STRINGVALUE",
                true);
    }

    public void testHandleIngestionRequestSuccessFloat() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-1";
        String pvName = "pv_01";
        List<String> columnNames = Arrays.asList(pvName);
        double value1 = 12.34;
        double value2 = 42.00;
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        firstSeconds + 1,
                        firstNanos + 999_000_000L);
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertFalse("error flag is set", result.isError);
        verifySuccessfulRequest(params);

        // now test sending duplicate request
        job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        result = job.handleIngestionRequest(handlerIngestionRequest);
        assertTrue(
                "isError not set",
                result.isError);
        assertTrue("message not set", result.message.contains("duplicate key error"));
        verifyFailedRequest(
                params, BsonConstants.BSON_VALUE_STATUS_ERROR, "E11000 duplicate key error", false);

    }

    public void testHandleIngestionRequestSuccessString() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-4";
        String pvName = "pv_04";
        List<String> columnNames = Arrays.asList(pvName);
        String value1 = "junk";
        String value2 = "stuff";
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.STRING,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        firstSeconds + 1,
                        firstNanos + 999_000_000L);
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertFalse("error flag is set", result.isError);
        verifySuccessfulRequest(params);
    }

    public void testHandleIngestionRequestSuccessInt() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-5";
        String pvName = "pv_05";
        List<String> columnNames = Arrays.asList(pvName);
        Long value1 = 14L;
        Long value2 = 42L;
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.INT,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        firstSeconds + 1,
                        firstNanos + 999_000_000L);
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertFalse("error flag is set", result.isError);
        verifySuccessfulRequest(params);
    }

    public void testHandleIngestionRequestSuccessBoolean() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-6";
        String pvName = "pv_06";
        List<String> columnNames = Arrays.asList(pvName);
        Boolean value1 = true;
        Boolean value2 = false;
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.BOOLEAN,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        firstSeconds + 1,
                        firstNanos + 999_000_000L);
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertFalse("error flag is set", result.isError);
        verifySuccessfulRequest(params);
    }

    /**
     * Tests that array data is not handled and leads to error status in mongo.
     */
    public void testHandleIngestionRequestSuccessArray() {

        // assemble IngestionRequest
        int providerId = 1;
        String requestId = "request-7";
        String pvName = "pv_07";
        List<String> columnNames = Arrays.asList(pvName);
        // use arrays as the values for this test
        List<Double> value1 = Arrays.asList(12.34, 56.78);
        List<Double> value2 = Arrays.asList(98.76, 54.32);
        List<Object> columnDataList = Arrays.asList(value1, value2);
        List<List<Object>> values = Arrays.asList(columnDataList);
        Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
        String eventDescription = "calibration test";
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        long sampleIntervalNanos = 1_000_000L;
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        firstSeconds,
                        firstNanos,
                        sampleIntervalNanos,
                        numSamples,
                        columnNames,
                        IngestionDataType.ARRAY_DOUBLE,
                        values,
                        attributes,
                        eventDescription,
                        firstSeconds,
                        firstNanos,
                        firstSeconds + 1,
                        firstNanos + 999_000_000L);
        IngestDataRequest request = buildIngestionRequest(params);

        // send request and examine responses
        HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, null, false, "");
        IngestDataJob job = new IngestDataJob(handlerIngestionRequest, clientTestInterface, handler);
        HandlerIngestionResult result = job.handleIngestionRequest(handlerIngestionRequest);
        assertFalse(result.isError);
        verifySuccessfulRequest(params);
    }

}
