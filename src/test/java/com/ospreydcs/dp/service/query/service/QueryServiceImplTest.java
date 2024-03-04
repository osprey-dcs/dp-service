package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.*;

public class QueryServiceImplTest extends QueryTestBase {

    private static final QueryServiceImpl serviceImpl = new QueryServiceImpl();

    @Test
    public void testDateFromTimestamp() {

        long epochSeconds = 1691438936L;
        long nanos = 999000000L;

        // create a grpc timestamp, and the convert to java date
        final Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
        timestampBuilder.setEpochSeconds(epochSeconds);
        timestampBuilder.setNanoseconds(nanos);
        Timestamp timestamp = timestampBuilder.build();
        Date dateFromTimestamp = GrpcUtility.dateFromTimestamp(timestamp);

        // create a java instant, and use to create java date
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanos);
        Date dateFromInstant = Date.from(instant);

        // check that the two dates are equal
        assertTrue(dateFromTimestamp.equals(dateFromInstant));
    }

    @Test
    public void testQueryResponseReject() {

        final String msg = "test";
        QueryDataResponse response = serviceImpl.queryResponseDataReject(msg);

        // check response contains message and reason
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getExceptionalResult().getMessage().equals(msg));
    }

    @Test
    public void testQueryResponseError() {

        final String msg = "error message";
        QueryDataResponse response = serviceImpl.queryResponseDataError(msg);

        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getExceptionalResult().getMessage().equals(msg));
    }

    @Test
    public void testQueryResponseEmpty() {

        QueryDataResponse response = serviceImpl.queryResponseDataEmpty();

        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY,
                response.getExceptionalResult().getExceptionalResultStatus());
    }

    @Test
    public void testQueryResponseNotReady() {

        QueryDataResponse response = serviceImpl.queryResponseDataNotReady();

        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_NOT_READY,
                response.getExceptionalResult().getExceptionalResultStatus());
    }

    @Test
    public void testQueryResponseData() {

        QueryDataResponse.QueryData.Builder resultDataBuilder =
                QueryDataResponse.QueryData.newBuilder();

        QueryDataResponse response = serviceImpl.queryResponseData(resultDataBuilder);

        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryData());
    }

}
