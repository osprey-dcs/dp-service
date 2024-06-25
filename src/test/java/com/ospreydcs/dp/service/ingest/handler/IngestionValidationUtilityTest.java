package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IngestionValidationUtilityTest extends IngestionTestBase {

    @Test
    public void testValidateRequestUnspecifiedRequestTime() {
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        params.setRequestTime(false);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("requestTime must be specified"));
    }

    @Test
    public void testValidateRequestUnspecifiedProvider() {
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        null,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("providerId must be specified"));
    }

    @Test
    public void testValidateRequestUnspecifiedRequestId() {
        int providerId = 1;
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        null,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("clientRequestId must be specified"));
    }

    @Test
    public void testValidateRequestInvalidTimeIterator() {
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        0,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals(
                result.msg,
                "IngestDataRequest.ingestionDataFrame.dataTimestamps.value must specify SamplingClock or list of timestamps");
    }

    /**
     * Provides test coverage of validation check for empty columns list.
     */
    @Test
    public void testValidateRequestEmptyColumnsList() {
        int providerId = 1;
        String requestId = "request-1";
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        null,
                        IngestionDataType.DOUBLE,
                        null);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("columns list cannot be empty"));
    }

    /**
     * Provides test coverage of validation check that each column contains the same number of values as
     * the timestamps list.
     */
    @Test
    public void testValidateRequestColumnSizeMismatch() {
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("mismatch numValues:"));
    }

    /**
     * Provides test coverage of validation check that a name is provided for each column.
     */
    @Test
    public void testValidateRequestColumnNameMissing() {
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        ValidationResult result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("name must be specified for all data columns"));
    }

}
