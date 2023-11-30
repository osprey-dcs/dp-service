package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IngestionHandlerBaseTest extends IngestionTestBase {

    private TestIngestionHandler handler = new TestIngestionHandler();

    protected static class TestIngestionHandler extends IngestionHandlerBase {
    }

    @Test
    public void test01ValidateRequestUnspecifiedRequestTime() {
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
                        IngestionDataType.FLOAT,
                        values);
        params.setRequestTime(false);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue("validation result msg not set to expected value", result.msg.equals("requestTime must be specified"));
    }

    @Test
    public void test02ValidateRequestUnspecifiedProvider() {
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
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue("validation result msg not set to expected value", result.msg.equals("providerId must be specified"));
    }

    @Test
    public void test03ValidateRequestUnspecifiedRequestId() {
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
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue(
                "validation result msg not set to expected value",
                result.msg.equals("clientRequestId must be specified"));
    }

    /**
     * Provides test coverage of validation check for invalid time spec using timestamp list instead of iterator.
     */
    @Test
    public void test04ValidateRequestInvalidTimeSpec() {
        int providerId = 1;
        String requestId = "request-1";
        List<Long> timestampsSecondsList = Arrays.asList(12345L);
        List<Long> timestampsNanosList = Arrays.asList(0L);
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        timestampsSecondsList,
                        timestampsNanosList,
                        null,
                        null,
                        null,
                        null,
                        columnNames,
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue(
                "validation result message not set",
                result.msg.equals("only timestamp iterator is currently supported for dataTimeSpec"));
    }

    @Test
    public void test05ValidateRequestInvalidTimeIterator() {
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
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue(
                "validation result message not set",
                result.msg.equals("dataTimeSpec must specify list of timestamps or iterator with numSamples"));
    }

    /**
     * Provides test coverage of validation check for empty columns list.
     */
    @Test
    public void test06ValidateRequestEmptyColumnsList() {
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
                        IngestionDataType.FLOAT,
                        null);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue(
                "validation result msg not set",
                result.msg.equals("columns list cannot be empty"));
    }

    /**
     * Provides test coverage of validation check that each column contains the same number of values as
     * the timestamps list.
     */
    @Test
    public void test07ValidateRequestColumnSizeMismatch() {
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
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue("validation result msg not set to expected value", result.msg.contains("mismatch numValues:"));
    }

    /**
     * Provides test coverage of validation check that a name is provided for each column.
     */
    @Test
    public void test08ValidateRequestColumnNameMissing() {
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
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);
        ValidationResult result = handler.validateIngestionRequest(request);
        assertTrue("validation result error flag not set", result.isError);
        assertTrue("validation result msg not set to expected value", result.msg.equals("name must be specified for all data columns"));
    }

}
