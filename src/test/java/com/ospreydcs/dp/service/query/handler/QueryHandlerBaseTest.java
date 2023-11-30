package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.model.ValidateQueryRequestResult;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryHandlerBaseTest extends QueryTestBase {

    protected static class TestQueryHandler extends QueryHandlerBase {
    }

    private TestQueryHandler handler = new TestQueryHandler();

    @Test
    public void test01ValidateRequestUnspecifiedColumnName() {
        String columnName = null;
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                null,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);
        ValidateQueryRequestResult result = handler.validateQueryDataByTimeRequest(request);
        assertTrue("isError not set", result.isError);
        assertTrue("msg not set", result.msg.equals("columnName must be specified"));
    }

    @Test
    public void test02ValidateRequestUnspecifiedStartTime() {
        String columnName = "pv_01";
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                columnName,
                null,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);
        ValidateQueryRequestResult result = handler.validateQueryDataByTimeRequest(request);
        assertTrue("isError not set", result.isError);
        assertTrue("msg not set", result.msg.equals("startTime must be specified"));
    }

    @Test
    public void test03ValidateRequestUnspecifiedEndTime() {
        String columnName = "pv_01";
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                columnName,
                nowSeconds,
                0L,
                null,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);
        ValidateQueryRequestResult result = handler.validateQueryDataByTimeRequest(request);
        assertTrue("isError not set", result.isError);
        assertTrue("msg not set", result.msg.equals("endTime must be specified"));
    }

    @Test
    public void test04ValidateRequestInvalidEndTimeSeconds() {
        String columnName = "pv_01";
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                columnName,
                nowSeconds,
                0L,
                nowSeconds - 1,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);
        ValidateQueryRequestResult result = handler.validateQueryDataByTimeRequest(request);
        assertTrue("isError not set", result.isError);
        assertTrue("msg not set", result.msg.equals("endTime seconds must be >= startTime seconds"));
    }

    @Test
    public void test05ValidateRequestInvalidEndTimeNanos() {
        String columnName = "pv_01";
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                columnName,
                nowSeconds,
                200L,
                nowSeconds,
                100L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);
        ValidateQueryRequestResult result = handler.validateQueryDataByTimeRequest(request);
        assertTrue("isError not set", result.isError);
        assertTrue("msg not set", result.msg.equals("endTime nanos must be > startTime nanos when seconds match"));
    }

}
