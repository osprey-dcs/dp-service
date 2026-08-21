package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusBucket;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusFrame;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Integration tests for the Sample Status API: saveSampleStatuses (carve-and-insert upsert),
 * querySampleStatuses (keyset paging), querySampleStatusesStream (fire-and-consume chunking),
 * deleteSampleStatuses (exact at the sample axis), and the deferred domain-registry stubs.
 */
public class SampleStatusIT extends AnnotationIntegrationTestIntermediate {

    private static final String DOMAIN = "data_quality";
    private static final String DOMAIN_OTHER = "ml_anomaly";
    private static final String LAYER = "ml_model_v1";
    private static final String LAYER_OTHER = "operator_override";
    private static final String PV_1 = "TEST:PV:001";
    private static final String PV_2 = "TEST:PV:002";

    private static final long START_SECONDS = 1_700_000_000L;
    private static final long SECOND_NANOS = 1_000_000_000L;
    private static final long PERIOD = 100_000_000L; // 100ms

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    // ------------------- helpers ---------------------------

    private static Timestamp timestamp(long seconds, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(nanos).build();
    }

    /** Epoch nanos of the i'th tick of the standard test clock (start + i * PERIOD). */
    private static long tickNanos(int i) {
        return START_SECONDS * SECOND_NANOS + i * PERIOD;
    }

    private static Timestamp tick(int i) {
        return timestamp(tickNanos(i) / SECOND_NANOS, tickNanos(i) % SECOND_NANOS);
    }

    private static DataTimestamps clockAxis(int count) {
        return DataTimestampsUtility.dataTimestampsWithSamplingClock(START_SECONDS, 0, PERIOD, count);
    }

    private static DataTimestamps listAxis(Timestamp... timestamps) {
        return DataTimestampsUtility.dataTimestampsWithTimestampList(List.of(timestamps));
    }

    private static List<Integer> codes(int value, int count) {
        return Collections.nCopies(count, value);
    }

    private static SampleStatusFrame frame(
            String domain, String layer, DataTimestamps axis, SampleStatusColumn... columns) {
        return AnnotationTestBase.buildSampleStatusFrame(domain, layer, axis, List.of(columns));
    }

    private static SampleStatusColumn column(String pvName, List<Integer> statusCodes) {
        return AnnotationTestBase.buildSampleStatusColumn(pvName, statusCodes, null, null);
    }

    private long save(SampleStatusFrame... frames) {
        final SaveSampleStatusesRequest request =
                AnnotationTestBase.buildSaveSampleStatusesRequest(List.of(frames), "source-1", "user-1");
        return annotationServiceWrapper.sendAndVerifySaveSampleStatuses(request, false, null);
    }

    private QuerySampleStatusesRequest queryRequest(
            Timestamp beginTime, Timestamp endTime,
            List<String> pvNames, List<String> domains, List<String> layers,
            int limit, String pageToken) {
        return AnnotationTestBase.buildQuerySampleStatusesRequest(
                beginTime, endTime, pvNames, domains, layers, limit, pageToken);
    }

    /** Collects the epoch-nanos timestamps of every status in the given buckets. */
    private static Set<Long> statusTimestamps(List<SampleStatusBucket> buckets) {
        final Set<Long> result = new TreeSet<>();
        for (SampleStatusBucket bucket : buckets) {
            final DataTimestampsUtility.DataTimestampsIterator iterator =
                    DataTimestampsUtility.dataTimestampsIterator(bucket.getDataTimestamps());
            while (iterator.hasNext()) {
                final Timestamp timestamp = iterator.next();
                result.add(timestamp.getEpochSeconds() * SECOND_NANOS + timestamp.getNanoseconds());
            }
        }
        return result;
    }

    // =========================================================================
    // saveSampleStatuses tests
    // =========================================================================

    @Test
    public void testSaveDenseClockMultiplePvs() {
        // dense save: SamplingClock axis, two PVs -> one document per PV, savedCount = count x PVs
        final long savedCount = save(frame(DOMAIN, LAYER, clockAxis(10),
                column(PV_1, codes(1, 10)), column(PV_2, codes(2, 10))));
        assertEquals(20, savedCount);

        final List<SampleStatusBucketDocument> pv1Documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, pv1Documents.size());
        final SampleStatusBucketDocument document = pv1Documents.get(0);
        assertEquals(codes(1, 10), document.getStatusCodes());
        assertEquals(tickNanos(0), document.getFirstTimeNanos());
        assertEquals(tickNanos(9), document.getLastTimeNanos());
        assertEquals("source-1", document.getSource());
        assertEquals("user-1", document.getModifiedBy());
        assertNotNull(document.getUpdatedTime());
        assertNull(document.getConfidence());
        assertNull(document.getReasons());

        assertEquals(1, mongoClient.findSampleStatusBuckets(PV_2, DOMAIN, LAYER).size());
    }

    @Test
    public void testSaveSparseTimestampList() {
        // sparse labeling: 3 suspect points; nothing is stored for unlabeled samples
        final long savedCount = save(frame(DOMAIN, LAYER, listAxis(tick(1), tick(5), tick(9)),
                column(PV_1, List.of(4, 4, 4))));
        assertEquals(3, savedCount);

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, documents.size());
        assertEquals(3, documents.get(0).getStatusCodes().size());
        assertEquals(tickNanos(1), documents.get(0).getFirstTimeNanos());
        assertEquals(tickNanos(9), documents.get(0).getLastTimeNanos());
    }

    @Test
    public void testSaveUpsertIdenticalTimestampsIncomingWins() {
        save(frame(DOMAIN, LAYER, clockAxis(5), column(PV_1, codes(1, 5))));

        final List<SampleStatusBucketDocument> firstDocuments =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, firstDocuments.size());

        // re-save at identical timestamps with different codes and provenance: incoming wins whole
        final SampleStatusColumn columnWithCompanions = AnnotationTestBase.buildSampleStatusColumn(
                PV_1, codes(2, 5), List.of(0.9f, 0.9f, 0.9f, 0.9f, 0.9f), List.of("a", "b", "c", "d", "e"));
        final SaveSampleStatusesRequest request = AnnotationTestBase.buildSaveSampleStatusesRequest(
                List.of(frame(DOMAIN, LAYER, clockAxis(5), columnWithCompanions)), "source-2", "user-2");
        assertEquals(5, annotationServiceWrapper.sendAndVerifySaveSampleStatuses(request, false, null));

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, documents.size());
        final SampleStatusBucketDocument document = documents.get(0);
        assertEquals(codes(2, 5), document.getStatusCodes());
        assertEquals(5, document.getConfidence().size());
        assertEquals("source-2", document.getSource());
        assertEquals("user-2", document.getModifiedBy());
    }

    @Test
    public void testSaveFullReplaceClearsCompanions() {
        // first save carries confidence and reasons
        save(frame(DOMAIN, LAYER, clockAxis(3), AnnotationTestBase.buildSampleStatusColumn(
                PV_1, codes(1, 3), List.of(0.5f, 0.6f, 0.7f), List.of("x", "y", "z"))));
        assertNotNull(mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER).get(0).getConfidence());

        // re-saving the same keys with empty companions clears the stored values (full replace)
        save(frame(DOMAIN, LAYER, clockAxis(3), column(PV_1, codes(1, 3))));

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, documents.size());
        assertNull(documents.get(0).getConfidence());
        assertNull(documents.get(0).getReasons());
    }

    @Test
    public void testSaveCrossFrameDuplicateKeyLaterFrameWins() {
        // same identity keys in two frames of one request: later frame wins, savedCount counts both
        final long savedCount = save(
                frame(DOMAIN, LAYER, clockAxis(4), column(PV_1, codes(1, 4))),
                frame(DOMAIN, LAYER, clockAxis(4), column(PV_1, codes(2, 4))));
        assertEquals(8, savedCount);

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, documents.size());
        assertEquals(codes(2, 4), documents.get(0).getStatusCodes());
    }

    @Test
    public void testSaveInterleavedListLeavesClockDocumentUntouched() {
        // clock document plus list points between its ticks: no identity collision, so the
        // original document is untouched (provenance intact) and the two documents coexist
        save(frame(DOMAIN, LAYER, clockAxis(3), column(PV_1, codes(1, 3))));

        final Timestamp betweenTicks1 = timestamp(START_SECONDS, PERIOD / 2);
        final Timestamp betweenTicks2 = timestamp(START_SECONDS, PERIOD + PERIOD / 2);
        final SaveSampleStatusesRequest secondRequest = AnnotationTestBase.buildSaveSampleStatusesRequest(
                List.of(frame(DOMAIN, LAYER, listAxis(betweenTicks1, betweenTicks2),
                        column(PV_1, List.of(9, 9)))),
                "source-2", "user-2");
        assertEquals(2, annotationServiceWrapper.sendAndVerifySaveSampleStatuses(secondRequest, false, null));

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(2, documents.size());
        // first document (by firstTimeNanos) is the original clock document, provenance untouched
        assertEquals("source-1", documents.get(0).getSource());
        assertEquals(codes(1, 3), documents.get(0).getStatusCodes());
        assertEquals("source-2", documents.get(1).getSource());

        // query returns the union of all five statuses
        final AnnotationTestBase.QuerySampleStatusesResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(0), tick(10), List.of(PV_1), null, null, 0, null),
                        false, null, 2);
        assertEquals(5, statusTimestamps(observer.getSampleStatusBuckets()).size());
    }

    @Test
    public void testSavePartialOverlapCarvesCollidingTimestamps() {
        // overwrite one interior tick of an existing clock document: the colliding sample is
        // carved out of the original, which takes the incoming provenance; a new document holds
        // the incoming status
        save(frame(DOMAIN, LAYER, clockAxis(5), column(PV_1, codes(1, 5))));

        final SaveSampleStatusesRequest overwriteRequest = AnnotationTestBase.buildSaveSampleStatusesRequest(
                List.of(frame(DOMAIN, LAYER, listAxis(tick(2)), column(PV_1, List.of(99)))),
                "source-2", "user-2");
        assertEquals(1, annotationServiceWrapper.sendAndVerifySaveSampleStatuses(overwriteRequest, false, null));

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(2, documents.size());

        // carved document: 4 surviving samples, rewritten with the incoming save's provenance
        final SampleStatusBucketDocument carvedDocument = documents.get(0);
        assertEquals(4, carvedDocument.getStatusCodes().size());
        assertEquals("source-2", carvedDocument.getSource());
        assertEquals("user-2", carvedDocument.getModifiedBy());

        // total statuses across documents: 4 + 1, with code 99 at tick 2
        final AnnotationTestBase.QuerySampleStatusesResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(0), tick(10), List.of(PV_1), null, null, 0, null),
                        false, null, 2);
        assertEquals(5, statusTimestamps(observer.getSampleStatusBuckets()).size());
        boolean foundOverwrite = false;
        for (SampleStatusBucket bucket : observer.getSampleStatusBuckets()) {
            if (bucket.getStatusColumn().getStatusCodesList().equals(List.of(99))) {
                foundOverwrite = true;
            }
        }
        assertTrue(foundOverwrite);
    }

    @Test
    public void testSaveRejectEmptyFrames() {
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(List.of(), null, null),
                true, "at least one SampleStatusFrame");
    }

    @Test
    public void testSaveRejectDuplicatePvInFrame() {
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(
                        List.of(frame(DOMAIN, LAYER, clockAxis(2),
                                column(PV_1, codes(1, 2)), column(PV_1, codes(2, 2)))),
                        null, null),
                true, "more than one statusColumn for PV");
    }

    @Test
    public void testSaveRejectStatusCodesLengthMismatch() {
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(
                        List.of(frame(DOMAIN, LAYER, clockAxis(3), column(PV_1, codes(1, 2)))),
                        null, null),
                true, "statusCodes.length mismatch");
    }

    @Test
    public void testSaveRejectZeroPeriodClock() {
        final DataTimestamps axis =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(START_SECONDS, 0, 0, 2);
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(
                        List.of(frame(DOMAIN, LAYER, axis, column(PV_1, codes(1, 2)))),
                        null, null),
                true, "periodNanos must be > 0");
    }

    @Test
    public void testSaveRejectNonIncreasingTimestampList() {
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(
                        List.of(frame(DOMAIN, LAYER, listAxis(tick(2), tick(1)),
                                column(PV_1, codes(1, 2)))),
                        null, null),
                true, "not strictly increasing");
    }

    @Test
    public void testSaveRejectionPersistsNothing() {
        // second frame is invalid: the whole request is rejected and the first frame's valid
        // statuses are not persisted
        annotationServiceWrapper.sendAndVerifySaveSampleStatuses(
                AnnotationTestBase.buildSaveSampleStatusesRequest(
                        List.of(
                                frame(DOMAIN, LAYER, clockAxis(2), column(PV_1, codes(1, 2))),
                                frame("", LAYER, clockAxis(2), column(PV_2, codes(1, 2)))),
                        null, null),
                true, "domain must be specified");
        assertTrue(mongoClient.findSampleStatusBucketsNoRetry(PV_1, DOMAIN, LAYER).isEmpty());
    }

    // =========================================================================
    // querySampleStatuses tests
    // =========================================================================

    @Test
    public void testQueryOverlapEdgesAndWholeBuckets() {
        // one document with samples at ticks 0..9
        save(frame(DOMAIN, LAYER, clockAxis(10), column(PV_1, codes(1, 10))));

        // begin exactly at the document's last sample time: included (lastTime >= beginTime)
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(9), tick(20), List.of(PV_1), null, null, 0, null),
                false, null, 1);

        // begin one nano past the last sample time: excluded
        final Timestamp justPastLast = timestamp(START_SECONDS, 9 * PERIOD + 1);
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(justPastLast, tick(20), List.of(PV_1), null, null, 0, null),
                false, null, 0);

        // end exactly at the document's first sample time: excluded (firstTime < endTime, half-open)
        final Timestamp wayBefore = timestamp(START_SECONDS - 100, 0);
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(wayBefore, tick(0), List.of(PV_1), null, null, 0, null),
                false, null, 0);

        // end one nano past the first sample time: included
        final Timestamp justPastFirst = timestamp(START_SECONDS, 1);
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(wayBefore, justPastFirst, List.of(PV_1), null, null, 0, null),
                false, null, 1);

        // boundary bucket is returned WHOLE: a query covering one interior tick returns all 10 statuses
        final AnnotationTestBase.QuerySampleStatusesResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(4), tick(5), List.of(PV_1), null, null, 0, null),
                        false, null, 1);
        assertEquals(10, observer.getSampleStatusBuckets().get(0).getStatusColumn().getStatusCodesCount());
    }

    @Test
    public void testQueryEmptyPvNamesWildcardEnumeratesLabeledPvs() {
        save(frame(DOMAIN, LAYER, clockAxis(2),
                column(PV_1, codes(1, 2)), column(PV_2, codes(2, 2))));
        save(frame(DOMAIN, LAYER_OTHER, clockAxis(2), column(PV_1, codes(3, 2))));

        // empty pvNames matches all PVs the (domain, layer) has labeled
        final AnnotationTestBase.QuerySampleStatusesResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(0), tick(10), null, List.of(DOMAIN), List.of(LAYER), 0, null),
                        false, null, 2);
        final Set<String> labeledPvs = new TreeSet<>();
        for (SampleStatusBucket bucket : observer.getSampleStatusBuckets()) {
            labeledPvs.add(bucket.getStatusColumn().getPvName());
        }
        assertEquals(Set.of(PV_1, PV_2), labeledPvs);
    }

    @Test
    public void testQueryFilterCombinations() {
        save(frame(DOMAIN, LAYER, clockAxis(2), column(PV_1, codes(1, 2)), column(PV_2, codes(1, 2))));
        save(frame(DOMAIN, LAYER_OTHER, clockAxis(2), column(PV_1, codes(2, 2))));
        save(frame(DOMAIN_OTHER, LAYER, clockAxis(2), column(PV_1, codes(3, 2))));

        // no filters: everything in range
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), null, null, null, 0, null), false, null, 4);

        // AND across fields: pvName AND domain AND layer
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), List.of(PV_1), List.of(DOMAIN), List.of(LAYER), 0, null),
                false, null, 1);

        // OR within a field: both layers of DOMAIN for PV_1
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), List.of(PV_1), List.of(DOMAIN),
                        List.of(LAYER, LAYER_OTHER), 0, null),
                false, null, 2);

        // domain filter alone
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), null, List.of(DOMAIN_OTHER), null, 0, null),
                false, null, 1);

        // non-matching pvName
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), List.of("TEST:PV:NOPE"), null, null, 0, null),
                false, null, 0);
    }

    @Test
    public void testQueryPaginationKeysetTokens() {
        // five buckets in deterministic order: PV_1 has three disjoint documents (different time
        // spans), PV_2 has two; result order is (pvName, domain, layer, bucket start time)
        save(frame(DOMAIN, LAYER, listAxis(tick(0), tick(1)), column(PV_1, codes(1, 2)), column(PV_2, codes(1, 2))));
        save(frame(DOMAIN, LAYER, listAxis(tick(10), tick(11)), column(PV_1, codes(2, 2)), column(PV_2, codes(2, 2))));
        save(frame(DOMAIN, LAYER, listAxis(tick(20), tick(21)), column(PV_1, codes(3, 2))));

        final List<SampleStatusBucket> allBuckets = new ArrayList<>();

        // page 1
        AnnotationTestBase.QuerySampleStatusesResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(0), tick(30), null, null, null, 2, null), false, null, 2);
        assertFalse(observer.getNextPageToken().isEmpty());
        allBuckets.addAll(observer.getSampleStatusBuckets());

        // page 2, resuming from the token
        observer = annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(30), null, null, null, 2, observer.getNextPageToken()),
                false, null, 2);
        assertFalse(observer.getNextPageToken().isEmpty());
        allBuckets.addAll(observer.getSampleStatusBuckets());

        // page 3: last page has one bucket and an empty token
        observer = annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(30), null, null, null, 2, observer.getNextPageToken()),
                false, null, 1);
        assertTrue(observer.getNextPageToken().isEmpty());
        allBuckets.addAll(observer.getSampleStatusBuckets());

        // all five buckets seen exactly once (no bucket split or repeated across pages), in
        // (pvName, domain, layer, bucket start time) order
        assertEquals(5, allBuckets.size());
        assertEquals(PV_1, allBuckets.get(0).getStatusColumn().getPvName());
        assertEquals(List.of(1, 1), allBuckets.get(0).getStatusColumn().getStatusCodesList());
        assertEquals(List.of(2, 2), allBuckets.get(1).getStatusColumn().getStatusCodesList());
        assertEquals(List.of(3, 3), allBuckets.get(2).getStatusColumn().getStatusCodesList());
        assertEquals(PV_2, allBuckets.get(3).getStatusColumn().getPvName());
        assertEquals(List.of(1, 1), allBuckets.get(3).getStatusColumn().getStatusCodesList());
        assertEquals(List.of(2, 2), allBuckets.get(4).getStatusColumn().getStatusCodesList());
    }

    @Test
    public void testQueryRejectInvalidPageToken() {
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), null, null, null, 0, "bogus-token"),
                true, "not a valid page token", 0);
    }

    @Test
    public void testQueryRejectMissingTimeRange() {
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(null, null, null, null, null, 0, null),
                true, "timeRange must be provided", 0);
    }

    @Test
    public void testQueryRejectBeginNotBeforeEnd() {
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(5), tick(5), null, null, null, 0, null),
                true, "beginTime must be before endTime", 0);
    }

    @Test
    public void testQueryNoMatchIsEmptySuccess() {
        annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                queryRequest(tick(0), tick(10), null, null, null, 0, null), false, null, 0);
    }

    // =========================================================================
    // querySampleStatusesStream tests
    // =========================================================================

    @Test
    public void testQueryStreamChunksByLimit() {
        save(frame(DOMAIN, LAYER, listAxis(tick(0)), column(PV_1, List.of(1)), column(PV_2, List.of(1))));
        save(frame(DOMAIN, LAYER, listAxis(tick(10)), column(PV_1, List.of(2)), column(PV_2, List.of(2))));
        save(frame(DOMAIN, LAYER, listAxis(tick(20)), column(PV_1, List.of(3))));

        // five buckets streamed in chunks of two: sizes [2, 2, 1], every nextPageToken empty
        // (asserted inside the wrapper)
        final AnnotationTestBase.QuerySampleStatusesStreamResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatusesStream(
                        queryRequest(tick(0), tick(30), null, null, null, 2, null), false, null, 5);
        assertEquals(List.of(2, 2, 1), observer.getChunkSizes());
    }

    @Test
    public void testQueryStreamEmptyResultIsSingleEmptyMessage() {
        final AnnotationTestBase.QuerySampleStatusesStreamResponseObserver observer =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatusesStream(
                        queryRequest(tick(0), tick(10), null, null, null, 0, null), false, null, 0);
        assertEquals(List.of(0), observer.getChunkSizes());
    }

    @Test
    public void testQueryStreamRejectNonEmptyPageToken() {
        // build a real token so the rejection is specifically about streaming, not parseability
        save(frame(DOMAIN, LAYER, listAxis(tick(0)), column(PV_1, List.of(1))));
        save(frame(DOMAIN, LAYER, listAxis(tick(10)), column(PV_1, List.of(2))));
        final AnnotationTestBase.QuerySampleStatusesResponseObserver unaryObserver =
                annotationServiceWrapper.sendAndVerifyQuerySampleStatuses(
                        queryRequest(tick(0), tick(30), null, null, null, 1, null), false, null, 1);
        final String realToken = unaryObserver.getNextPageToken();
        assertFalse(realToken.isEmpty());

        annotationServiceWrapper.sendAndVerifyQuerySampleStatusesStream(
                queryRequest(tick(0), tick(30), null, null, null, 1, realToken),
                true, "pageToken must be empty", 0);
    }

    // =========================================================================
    // deleteSampleStatuses tests
    // =========================================================================

    private DeleteSampleStatusesRequest deleteRequest(
            Timestamp beginTime, Timestamp endTime, List<String> pvNames, String domain, String layer) {
        return AnnotationTestBase.buildDeleteSampleStatusesRequest(beginTime, endTime, pvNames, domain, layer);
    }

    @Test
    public void testDeleteBoundaryTrimNotWholeDocument() {
        // deleting [tick 0, tick 3) from a 10-tick document trims it, not deletes it whole
        save(frame(DOMAIN, LAYER, clockAxis(10), column(PV_1, codes(1, 10))));

        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(3), List.of(PV_1), DOMAIN, LAYER), false, null);
        assertEquals(3, deletedCount);

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(1, documents.size());
        assertEquals(7, documents.get(0).getStatusCodes().size());
        assertEquals(tickNanos(3), documents.get(0).getFirstTimeNanos());
        assertEquals(tickNanos(9), documents.get(0).getLastTimeNanos());
    }

    @Test
    public void testDeleteInteriorRangeSplitsDocumentInTwo() {
        // deleting an interior range [tick 3, tick 7) splits the clock document in two
        save(frame(DOMAIN, LAYER, clockAxis(10), column(PV_1, codes(1, 10))));

        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(3), tick(7), List.of(PV_1), DOMAIN, LAYER), false, null);
        assertEquals(4, deletedCount);

        final List<SampleStatusBucketDocument> documents =
                mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER);
        assertEquals(2, documents.size());
        assertEquals(tickNanos(0), documents.get(0).getFirstTimeNanos());
        assertEquals(tickNanos(2), documents.get(0).getLastTimeNanos());
        assertEquals(tickNanos(7), documents.get(1).getFirstTimeNanos());
        assertEquals(tickNanos(9), documents.get(1).getLastTimeNanos());

        // deletion does not refresh provenance: survivors keep the original save's identity
        assertEquals("source-1", documents.get(0).getSource());
        assertEquals("user-1", documents.get(0).getModifiedBy());
    }

    @Test
    public void testDeleteDocumentFullyInsideRangeRemoved() {
        save(frame(DOMAIN, LAYER, clockAxis(5), column(PV_1, codes(1, 5))));

        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(10), List.of(PV_1), DOMAIN, LAYER), false, null);
        assertEquals(5, deletedCount);
        assertTrue(mongoClient.findSampleStatusBucketsNoRetry(PV_1, DOMAIN, LAYER).isEmpty());
    }

    @Test
    public void testDeleteEmptyPvNamesWildcardScopedToDomainLayer() {
        save(frame(DOMAIN, LAYER, clockAxis(3), column(PV_1, codes(1, 3)), column(PV_2, codes(1, 3))));
        save(frame(DOMAIN, LAYER_OTHER, clockAxis(3), column(PV_1, codes(2, 3))));

        // empty pvNames deletes the (domain, layer)'s statuses for ALL PVs in the range
        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(10), null, DOMAIN, LAYER), false, null);
        assertEquals(6, deletedCount);
        assertTrue(mongoClient.findSampleStatusBucketsNoRetry(PV_1, DOMAIN, LAYER).isEmpty());
        assertTrue(mongoClient.findSampleStatusBucketsNoRetry(PV_2, DOMAIN, LAYER).isEmpty());

        // the other layer is untouched: deletes are scoped to a single producer stream
        assertEquals(1, mongoClient.findSampleStatusBuckets(PV_1, DOMAIN, LAYER_OTHER).size());
    }

    @Test
    public void testDeleteCountsStatusesNotDocuments() {
        // two documents for the PV: counts accumulate individual statuses across documents
        save(frame(DOMAIN, LAYER, listAxis(tick(0), tick(1)), column(PV_1, codes(1, 2))));
        save(frame(DOMAIN, LAYER, listAxis(tick(10), tick(11), tick(12)), column(PV_1, codes(2, 3))));

        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(20), List.of(PV_1), DOMAIN, LAYER), false, null);
        assertEquals(5, deletedCount);
    }

    @Test
    public void testDeleteNoMatchIsSuccessWithZeroCount() {
        final long deletedCount = annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(10), List.of(PV_1), DOMAIN, LAYER), false, null);
        assertEquals(0, deletedCount);
    }

    @Test
    public void testDeleteRejectMissingDomain() {
        annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(10), null, null, LAYER),
                true, "domain must be specified");
    }

    @Test
    public void testDeleteRejectMissingLayer() {
        annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(tick(0), tick(10), null, DOMAIN, null),
                true, "layer must be specified");
    }

    @Test
    public void testDeleteRejectMissingTimeRange() {
        annotationServiceWrapper.sendAndVerifyDeleteSampleStatuses(
                deleteRequest(null, null, null, DOMAIN, LAYER),
                true, "timeRange must be provided");
    }

    // =========================================================================
    // deferred domain-registry stubs
    // =========================================================================

    @Test
    public void testSaveSampleStatusDomainStub() {
        annotationServiceWrapper.sendAndVerifySaveSampleStatusDomainStub();
    }

    @Test
    public void testQuerySampleStatusDomainsStub() {
        annotationServiceWrapper.sendAndVerifyQuerySampleStatusDomainsStub();
    }
}
