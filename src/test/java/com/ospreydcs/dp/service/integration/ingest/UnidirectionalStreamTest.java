package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UnidirectionalStreamTest extends GrpcIntegrationTestBase {

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void testSuccess() {

        // create containers
        final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
        final List<IngestDataRequest> requestList = new ArrayList<>();

        final int providerId = 1;

        // create 1st request
        {
            final String requestId = "request-1";
            final List<String> columnNames = Arrays.asList("PV_01");
            final List<List<Object>> values = Arrays.asList(Arrays.asList(1.01));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
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
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // create 2nd request
        {
            final String requestId = "request-2";
            final List<String> columnNames = Arrays.asList("PV_02");
            final List<List<Object>> values = Arrays.asList(Arrays.asList(2.02));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
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
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // send request and examine response
        sendAndVerifyIngestDataStream(paramsList, requestList, false, "");
    }

    @Test
    public void testReject() {

        // create containers
        final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
        final List<IngestDataRequest> requestList = new ArrayList<>();

        final int providerId = 1;

        // create valid request
        {
            final String requestId = "request-3";
            final List<String> columnNames = Arrays.asList("PV_03"); // use different pv name than above or will get failures due to duplicate database id
            final List<List<Object>> values = Arrays.asList(Arrays.asList(3.03));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
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
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // create invalid request, rejected due to invalid pv name
        {
            final String requestId = "request-4";
            final List<String> columnNames = Arrays.asList(""); // should be rejected for empty pv name
            final List<List<Object>> values = Arrays.asList(Arrays.asList(4.04));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
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
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // send request and examine response
        sendAndVerifyIngestDataStream(
                paramsList, requestList, true, "one or more requests were rejected");
    }

}
