package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
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
        QueryResponse response =
                serviceImpl.queryResponseReject(msg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);

        // check response contains message and reason
        assertTrue(response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReject());
        assertTrue(response.getQueryReject().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue(response.getQueryReject().getMessage().equals(msg));
    }

    @Test
    public void testQueryResponseError() {

        final String msg = "error message";
        QueryResponse response = serviceImpl.queryResponseError(msg);

        assertEquals(ResponseType.ERROR_RESPONSE, response.getResponseType());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReport());
        assertTrue(response.getQueryReport().hasQueryStatus());
        QueryResponse.QueryReport.QueryStatus status = response.getQueryReport().getQueryStatus();
        assertEquals(
                QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR,
                status.getQueryStatusType());
        assertTrue(status.getStatusMessage().equals(msg));
    }

    @Test
    public void testQueryResponseEmpty() {

        QueryResponse response = serviceImpl.queryResponseEmpty();

        assertEquals(ResponseType.STATUS_RESPONSE, response.getResponseType());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReport());
        assertTrue(response.getQueryReport().hasQueryStatus());
        QueryResponse.QueryReport.QueryStatus status = response.getQueryReport().getQueryStatus();
        assertEquals(
                QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY,
                status.getQueryStatusType());
    }

    @Test
    public void testQueryResponseNotReady() {

        QueryResponse response = serviceImpl.queryResponseNotReady();

        assertEquals(ResponseType.STATUS_RESPONSE, response.getResponseType());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReport());
        assertTrue(response.getQueryReport().hasQueryStatus());
        QueryResponse.QueryReport.QueryStatus status = response.getQueryReport().getQueryStatus();
        assertEquals(
                QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_NOT_READY,
                status.getQueryStatusType());
    }

    @Test
    public void testQueryResponseData() {

        QueryResponse.QueryReport.QueryData.Builder resultDataBuilder =
                QueryResponse.QueryReport.QueryData.newBuilder();

        QueryResponse response = serviceImpl.queryResponseData(resultDataBuilder);

        assertEquals(ResponseType.DETAIL_RESPONSE, response.getResponseType());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReport());
        assertTrue(response.getQueryReport().hasQueryData());
    }

}
