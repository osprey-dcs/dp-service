package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IngestionServiceImplTest extends IngestionTestBase {

    private static final IngestionServiceImpl serviceImpl = new IngestionServiceImpl();

    /**
     * Provides test coverage for MongoDbserviceImpl.dateFromTimestamp().
     */
    @Test
    public void test01DateFromTimestamp() {

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

    /**
     * Provides test coverage for MongoDbserviceImpl.ingestionResponseSuccess().
     */
    @Test
    public void test02IngestionResponseAck() {

        // create IngestionRequest
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
        Instant instantNow = Instant.now();
        int numSamples = 2;
        IngestionRequestParams params =
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
                        numSamples,
                        columnNames,
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);

        // test ingestionResponseAck
        IngestionResponse response = serviceImpl.ingestionResponseAck(request);
        assertTrue("providerId not set", response.getProviderId() == providerId);
        assertTrue("requestId not set", response.getClientRequestId().equals(requestId));
        assertTrue("responseType not set", response.getResponseType() == ResponseType.ACK_RESPONSE);
        assertTrue("response time not set", response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("response details not set", response.hasAckDetails());
        assertTrue(
                "num rows not set",
                response.getAckDetails().getNumRows() == numSamples);
        assertTrue("num columns not set", response.getAckDetails().getNumColumns() == columnNames.size());
    }

    @Test
    public void test03IngestionResponseReject() {

        // create IngestionRequest
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        int numSamples = 2;
        IngestionRequestParams params =
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
                        numSamples,
                        columnNames,
                        IngestionDataType.FLOAT,
                        values);
        IngestionRequest request = buildIngestionRequest(params);

        // test ingestionResponseRejectInvalid
        String msg = "test";
        IngestionResponse response =
                serviceImpl.ingestionResponseReject(request, msg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue("providerId not set",
                response.getProviderId() == providerId);
        assertTrue("requestId not set",
                response.getClientRequestId().equals(requestId));
        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("response details not set",
                response.hasRejectDetails());
        assertTrue("reject reason not set",
                response.getRejectDetails().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue("reject message not set",
                response.getRejectDetails().getMessage().equals(msg));
    }

}
