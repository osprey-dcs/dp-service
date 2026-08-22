package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.client.AnnotationClient;
import com.ospreydcs.dp.client.result.DeleteSampleStatusesApiResult;
import com.ospreydcs.dp.client.result.QuerySampleStatusesApiResult;
import com.ospreydcs.dp.client.result.SaveSampleStatusesApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusBucket;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusFrame;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Provides integration test coverage for the Sample Status API support in the
 * com.ospreydcs.dp.client convenience layer, exercising AnnotationClient against a running
 * annotation service.  Server-side behavior is covered separately by SampleStatusIT; these tests
 * cover the client wrapper — request building from the params records, the success payloads
 * (savedCount, buckets + nextPageToken, deletedCount), the streaming accumulation, and the
 * surfacing of server rejections through ApiResultBase.resultStatus.
 */
public class SampleStatusClientIT extends AnnotationIntegrationTestIntermediate {

    private static final String DOMAIN = "data_quality";
    private static final String LAYER = "ml_model_v1";
    private static final String PV_1 = "TEST:PV:001";
    private static final String PV_2 = "TEST:PV:002";

    private static final long START_SECONDS = 1_700_000_000L;
    private static final long PERIOD = 100_000_000L; // 100ms

    private AnnotationClient annotationClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        annotationClient = new AnnotationClient(annotationServiceWrapper.getAnnotationChannel());
    }

    @After
    public void tearDown() {
        annotationClient = null;
        super.tearDown();
    }

    // ------------------- helpers ---------------------------

    private static Timestamp timestamp(long seconds, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(nanos).build();
    }

    private static SampleStatusFrame clockFrame(String pvName, int count, int statusCode) {
        final SampleStatusColumn.Builder column = SampleStatusColumn.newBuilder().setPvName(pvName);
        for (int i = 0; i < count; i++) {
            column.addStatusCodes(statusCode);
        }
        return SampleStatusFrame.newBuilder()
                .setDomain(DOMAIN)
                .setLayer(LAYER)
                .setDataTimestamps(DataTimestampsUtility.dataTimestampsWithSamplingClock(
                        START_SECONDS, 0, PERIOD, count))
                .addStatusColumns(column)
                .build();
    }

    private AnnotationClient.QuerySampleStatusesParams queryParams(int limit, String pageToken) {
        return new AnnotationClient.QuerySampleStatusesParams(
                timestamp(START_SECONDS, 0), timestamp(START_SECONDS + 100, 0),
                null, List.of(DOMAIN), List.of(LAYER), limit, pageToken);
    }

    // =========================================================================
    // request builder tests
    // =========================================================================

    @Test
    public void testBuildQueryRequestOmitsUnsuppliedOptionalFields() {
        final QuerySampleStatusesRequest request = AnnotationClient.buildQuerySampleStatusesRequest(
                new AnnotationClient.QuerySampleStatusesParams(
                        timestamp(START_SECONDS, 0), timestamp(START_SECONDS + 10, 0),
                        null, null, null, 0, null));
        assertTrue(request.hasTimeRange());
        assertEquals(0, request.getPvNamesCount());
        assertEquals(0, request.getDomainsCount());
        assertEquals(0, request.getLayersCount());
        assertEquals(0, request.getLimit());
        assertEquals("", request.getPageToken());
    }

    @Test
    public void testBuildDeleteRequestPopulatesSuppliedFields() {
        final DeleteSampleStatusesRequest request = AnnotationClient.buildDeleteSampleStatusesRequest(
                new AnnotationClient.DeleteSampleStatusesParams(
                        timestamp(START_SECONDS, 0), timestamp(START_SECONDS + 10, 0),
                        List.of(PV_1), DOMAIN, LAYER));
        assertTrue(request.hasTimeRange());
        assertEquals(List.of(PV_1), request.getPvNamesList());
        assertEquals(DOMAIN, request.getDomain());
        assertEquals(LAYER, request.getLayer());
    }

    // =========================================================================
    // round-trip tests
    // =========================================================================

    @Test
    public void testSaveQueryDeleteRoundTrip() {
        // save: 10 statuses across one PV
        final SaveSampleStatusesApiResult saveResult = annotationClient.saveSampleStatuses(
                List.of(clockFrame(PV_1, 10, 1)), "client-source", "client-user");
        assertFalse(saveResult.resultStatus.msg, saveResult.isError());
        assertEquals(10, saveResult.savedCount);

        // query: one bucket back, carrying the provenance and PV inside the status column
        final QuerySampleStatusesApiResult queryResult =
                annotationClient.querySampleStatuses(queryParams(0, null));
        assertFalse(queryResult.resultStatus.msg, queryResult.isError());
        assertEquals(1, queryResult.sampleStatusBuckets.size());
        final SampleStatusBucket bucket = queryResult.sampleStatusBuckets.get(0);
        assertEquals(PV_1, bucket.getStatusColumn().getPvName());
        assertEquals(10, bucket.getStatusColumn().getStatusCodesCount());
        assertEquals("client-source", bucket.getSource());
        assertEquals("client-user", bucket.getModifiedBy());
        assertTrue(bucket.hasUpdatedTime());
        assertEquals("", queryResult.nextPageToken);

        // delete: exact range removes half the statuses
        final DeleteSampleStatusesApiResult deleteResult = annotationClient.deleteSampleStatuses(
                new AnnotationClient.DeleteSampleStatusesParams(
                        timestamp(START_SECONDS, 0),
                        timestamp(START_SECONDS, 5 * PERIOD),
                        List.of(PV_1), DOMAIN, LAYER));
        assertFalse(deleteResult.resultStatus.msg, deleteResult.isError());
        assertEquals(5, deleteResult.deletedCount);

        // remaining statuses still queryable
        final QuerySampleStatusesApiResult afterDelete =
                annotationClient.querySampleStatuses(queryParams(0, null));
        assertFalse(afterDelete.isError());
        assertEquals(1, afterDelete.sampleStatusBuckets.size());
        assertEquals(5, afterDelete.sampleStatusBuckets.get(0).getStatusColumn().getStatusCodesCount());
    }

    @Test
    public void testQueryPaginationThroughClient() {
        annotationClient.saveSampleStatuses(
                List.of(clockFrame(PV_1, 3, 1), clockFrame(PV_2, 3, 2)), null, null);

        // page 1 of 2
        final QuerySampleStatusesApiResult page1 =
                annotationClient.querySampleStatuses(queryParams(1, null));
        assertFalse(page1.isError());
        assertEquals(1, page1.sampleStatusBuckets.size());
        assertFalse(page1.nextPageToken.isEmpty());

        // page 2 resumes from the token and is the last page
        final QuerySampleStatusesApiResult page2 =
                annotationClient.querySampleStatuses(queryParams(1, page1.nextPageToken));
        assertFalse(page2.isError());
        assertEquals(1, page2.sampleStatusBuckets.size());
        assertTrue(page2.nextPageToken.isEmpty());

        assertEquals(PV_1, page1.sampleStatusBuckets.get(0).getStatusColumn().getPvName());
        assertEquals(PV_2, page2.sampleStatusBuckets.get(0).getStatusColumn().getPvName());
    }

    @Test
    public void testQueryStreamAccumulatesAllChunks() {
        annotationClient.saveSampleStatuses(
                List.of(clockFrame(PV_1, 3, 1), clockFrame(PV_2, 3, 2)), null, null);

        // chunk size 1 forces one message per bucket; the wrapper accumulates them all
        final QuerySampleStatusesApiResult streamResult =
                annotationClient.querySampleStatusesStream(queryParams(1, null));
        assertFalse(streamResult.resultStatus.msg, streamResult.isError());
        assertEquals(2, streamResult.sampleStatusBuckets.size());
        assertEquals("", streamResult.nextPageToken);
    }

    @Test
    public void testQueryStreamEmptyResultIsSuccess() {
        final QuerySampleStatusesApiResult streamResult =
                annotationClient.querySampleStatusesStream(queryParams(0, null));
        assertFalse(streamResult.isError());
        assertTrue(streamResult.sampleStatusBuckets.isEmpty());
    }

    // =========================================================================
    // rejection surfacing tests
    // =========================================================================

    @Test
    public void testSaveRejectSurfacedThroughResult() {
        final SaveSampleStatusesApiResult result =
                annotationClient.saveSampleStatuses(List.of(), null, null);
        assertTrue(result.isError());
        assertTrue(result.isReject());
        assertTrue(result.resultStatus.msg.contains("at least one SampleStatusFrame"));
        assertEquals(0, result.savedCount);
    }

    @Test
    public void testQueryRejectSurfacedThroughResult() {
        // begin == end fails validation
        final QuerySampleStatusesApiResult result = annotationClient.querySampleStatuses(
                new AnnotationClient.QuerySampleStatusesParams(
                        timestamp(START_SECONDS, 0), timestamp(START_SECONDS, 0),
                        null, null, null, 0, null));
        assertTrue(result.isError());
        assertTrue(result.isReject());
        assertTrue(result.resultStatus.msg.contains("beginTime must be before endTime"));
        assertNull(result.sampleStatusBuckets);
    }

    @Test
    public void testQueryStreamRejectSurfacedThroughResult() {
        // a non-empty pageToken is rejected on the streaming method
        final QuerySampleStatusesApiResult result = annotationClient.querySampleStatusesStream(
                queryParams(0, "some-token"));
        assertTrue(result.isError());
        assertTrue(result.isReject());
        assertTrue(result.resultStatus.msg.contains("pageToken must be empty"));
    }

    @Test
    public void testDeleteRejectSurfacedThroughResult() {
        final DeleteSampleStatusesApiResult result = annotationClient.deleteSampleStatuses(
                new AnnotationClient.DeleteSampleStatusesParams(
                        timestamp(START_SECONDS, 0), timestamp(START_SECONDS + 10, 0),
                        null, null, LAYER));
        assertTrue(result.isError());
        assertTrue(result.isReject());
        assertTrue(result.resultStatus.msg.contains("domain must be specified"));
        assertEquals(0, result.deletedCount);
    }
}
