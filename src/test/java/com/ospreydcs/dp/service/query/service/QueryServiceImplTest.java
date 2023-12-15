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
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        assertTrue("dateFromTimestamp date mismatch with date from instant", dateFromTimestamp.equals(dateFromInstant));
    }

    @Test
    public void testQueryResponseReject() {

        final String msg = "test";
        QueryResponse response =
                serviceImpl.queryResponseReject(msg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);

        // check response contains message and reason
        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("response details not set",
                response.hasQueryReject());
        assertTrue("reject reason not set",
                response.getQueryReject().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue("reject message not set",
                response.getQueryReject().getMessage().equals(msg));
    }

    @Test
    public void testQueryResponseError() {

        final String msg = "error message";
        QueryResponse response = serviceImpl.queryResponseError(msg);

        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.ERROR_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("QueryReport not set",
                response.hasQueryReport());
        assertTrue("QueryError not set",
                response.getQueryReport().hasQueryError());
        assertTrue("msg not set",
                response.getQueryReport().getQueryError().getMessage().equals(msg));
    }

    @Test
    public void testQueryResponseSummary() {

        final int numBuckets = 42;
        final String msg = "";
        QueryResponse response = serviceImpl.queryResponseSummary(numBuckets);

        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.SUMMARY_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("QueryReport not set",
                response.hasQueryReport());
        assertTrue("QuerySummary not set",
                response.getQueryReport().hasQuerySummary());
        assertTrue("numBuckets not set",
                response.getQueryReport().getQuerySummary().getNumBuckets() == numBuckets);
    }

}
