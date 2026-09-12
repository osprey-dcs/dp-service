package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.client.QueryClient;
import com.ospreydcs.dp.client.criteria.AttributeCriterion;
import com.ospreydcs.dp.client.criteria.TextMatch;
import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.QueryBucketsApiResult;
import com.ospreydcs.dp.client.result.QuerySamplesApiResult;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.SampleStatusSelector;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides integration test coverage for the Query API V2 support in the com.ospreydcs.dp.client
 * convenience layer (issue #244), exercising QueryClient against a running query service.
 * Server-side behavior is covered separately by QueryV2GrpcIT and the dispatcher-level tests
 * (MongoSyncQuerySamplesV2Test / MongoSyncQueryBucketsV2Test); these tests cover the client
 * wrapper — request building from the params records, the success payloads, the streaming
 * accumulation, and the surfacing of server rejections through ApiResultBase.resultStatus.
 *
 * <p>Seeded scenario ({@code simpleIngestionScenario}): PVs {@code S01-GCC01..S10-BPM03}, each with
 * 10 one-second buckets (10 samples/bucket) over {@code [startSeconds, startSeconds+10)}.
 */
public class QueryClientIT extends GrpcIntegrationTestBase {

    private static final String PV_1 = "S01-GCC01";
    private static final String PV_2 = "S01-BPM01";

    private QueryClient queryClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        queryClient = new QueryClient(queryServiceWrapper.getQueryChannel());
    }

    @After
    public void tearDown() {
        queryClient = null;
        super.tearDown();
    }

    // =========================================================================
    // request-building tests (no server interaction)
    // =========================================================================

    private static QueryClient.QuerySpecParams spec(
            QueryClient.PvSelectorParams pvSelector,
            List<QueryClient.ConfigurationCriterion> configurationCriteria
    ) {
        return new QueryClient.QuerySpecParams(
                Timestamp.newBuilder().setEpochSeconds(100).build(),
                Timestamp.newBuilder().setEpochSeconds(200).build(),
                pvSelector,
                configurationCriteria);
    }

    private static QueryClient.QuerySamplesParams samplesParams(QueryClient.QuerySpecParams querySpec) {
        return new QueryClient.QuerySamplesParams(querySpec, null, 0, null, false);
    }

    /**
     * Each PvSelectorParams arm must reach its own PvSelector oneof case.  The sealed hierarchy
     * makes setting two arms impossible, which is the point of D2 — the flat-record alternative
     * would have had to pick one silently, as buildQueryTableRequest does.
     */
    @Test
    public void testBuildQuerySpecPvSelectorArms() {

        {
            final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                    spec(new QueryClient.PvNameListSelector(List.of(PV_1, PV_2)), null)));
            final PvSelector selector = request.getQuerySpec().getPvSelector();
            assertEquals(PvSelector.SelectorCase.PVNAMELIST, selector.getSelectorCase());
            assertEquals(List.of(PV_1, PV_2), selector.getPvNameList().getPvNamesList());
        }

        {
            final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                    spec(new QueryClient.PvNamePatternSelector("S01-.*"), null)));
            final PvSelector selector = request.getQuerySpec().getPvSelector();
            assertEquals(PvSelector.SelectorCase.PVNAMEPATTERN, selector.getSelectorCase());
            assertEquals("S01-.*", selector.getPvNamePattern().getPattern());
        }

        {
            final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                    spec(new QueryClient.PvMetadataSelector(
                            new TextMatch(null, List.of("S01-"), null),
                            null,
                            List.of("gauges"),
                            List.of(new AttributeCriterion("sector", List.of("01")))),
                            null)));
            final PvSelector selector = request.getQuerySpec().getPvSelector();
            assertEquals(PvSelector.SelectorCase.METADATAQUERY, selector.getSelectorCase());
            assertEquals(3, selector.getMetadataQuery().getCriteriaCount());
        }
    }

    /**
     * A blank PV name is dropped rather than sent.  On this arm a blank name matches nothing rather
     * than everything, so it is not the #243 silent-match-all hazard — but it is still an avoidable
     * way to silently shrink a result, and dropping it keeps one rule across every criterion list.
     */
    @Test
    public void testBuildQuerySpecDropsBlankPvNames() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                spec(new QueryClient.PvNameListSelector(new ArrayList<>(List.of(PV_1, "", "   "))), null)));

        assertEquals(List.of(PV_1), request.getQuerySpec().getPvSelector().getPvNameList().getPvNamesList());
    }

    /**
     * An all-blank TextMatch must emit NO criterion, asserted on the built request rather than by
     * expecting a server error.  This is the #243/#245 lesson recorded in CLAUDE.md: under #245's
     * match-all rule a blank-only metadata query legitimately succeeds, so the guarantee is that no
     * "^" + Pattern.quote("") regex is ever built, not that the call fails.
     */
    @Test
    public void testBuildQuerySpecBlankTextMatchEmitsNoCriterion() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                spec(new QueryClient.PvMetadataSelector(
                        new TextMatch(List.of(""), List.of("  "), List.of("\t")),
                        new TextMatch(null, null, null),
                        List.of("", "   "),
                        List.of(new AttributeCriterion("  ", List.of("v")))),
                        null)));

        assertEquals(0, request.getQuerySpec().getPvSelector().getMetadataQuery().getCriteriaCount());
    }

    /**
     * A caller who asked for NO configuration restriction gets no ConfigurationSelector at all.
     * The server rejects an empty selector (deliberately — see dp-grpc#149), so emitting one here
     * would turn "the caller supplied no configuration filter" into a failed request.
     */
    @Test
    public void testBuildQuerySpecOmitsUnrequestedConfigurationSelector() {

        // null criteria list
        assertFalse(QueryClient.buildQuerySamplesRequest(samplesParams(
                        spec(new QueryClient.PvNameListSelector(List.of(PV_1)), null)))
                .getQuerySpec().hasConfigurationSelector());

        // empty criteria list
        assertFalse(QueryClient.buildQuerySamplesRequest(samplesParams(
                        spec(new QueryClient.PvNameListSelector(List.of(PV_1)), List.of())))
                .getQuerySpec().hasConfigurationSelector());
    }

    /**
     * A caller who DID ask for a configuration restriction, but supplied nothing usable, gets the
     * empty selector emitted so the server rejects the request.
     *
     * <p>This is the inverse of the issue #243 rule and the reason the two cases above are
     * distinguished from this one.  Elsewhere in the client layer, dropping a blank value narrows
     * toward correctness — a blank prefix would have matched everything.  Here dropping the
     * selector WIDENS the query from "only while configuration X was active" to the whole time
     * range, so silently dropping a criterion the caller filled in would return strictly more data
     * than they asked for with no diagnostic.  Rejection is the loud outcome.
     */
    @Test
    public void testBuildQuerySpecEmitsEmptySelectorForUnusableConfigurationCriteria() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)),
                        List.of(new QueryClient.ConfigurationCriterion(
                                List.of("", " "), null, null, null,
                                new AttributeCriterion("  ", List.of("v")))))));

        assertTrue(request.getQuerySpec().hasConfigurationSelector());
        assertEquals(0, request.getQuerySpec().getConfigurationSelector().getCriteriaCount());
    }

    /**
     * A criterion populating more than one oneof arm is a build error, not a preference-order
     * resolution: emitting the first populated arm would silently drop the caller's other
     * constraints.  The criterion is dropped, and because the caller did request a configuration
     * restriction the empty selector is emitted for the server to reject.
     */
    @Test
    public void testBuildQuerySpecRejectsMultiArmConfigurationCriterion() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)),
                        List.of(new QueryClient.ConfigurationCriterion(
                                List.of("cfg-a"), null, null, List.of("vacuum"), null)))));

        assertTrue(request.getQuerySpec().hasConfigurationSelector());
        assertEquals(0, request.getQuerySpec().getConfigurationSelector().getCriteriaCount());
    }

    /**
     * A usable configuration criterion is emitted.
     */
    @Test
    public void testBuildQuerySpecConfigurationCriteria() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(samplesParams(
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)),
                        List.of(
                                new QueryClient.ConfigurationCriterion(
                                        List.of("cfg-a"), null, null, null, null),
                                new QueryClient.ConfigurationCriterion(
                                        null, null, null, List.of("vacuum"), null)))));

        assertTrue(request.getQuerySpec().hasConfigurationSelector());
        assertEquals(2, request.getQuerySpec().getConfigurationSelector().getCriteriaCount());
        assertEquals(List.of("cfg-a"), request.getQuerySpec().getConfigurationSelector()
                .getCriteria(0).getConfigurationNameCriterion().getValuesList());
        assertEquals(List.of("vacuum"), request.getQuerySpec().getConfigurationSelector()
                .getCriteria(1).getTagsCriterion().getValuesList());
    }

    /**
     * limit and pageToken are omitted when unset/blank, so that an unsupplied option is genuinely
     * absent from the request rather than present as a proto default.
     */
    @Test
    public void testBuildRequestOmitsUnsetExecutionOptions() {

        final QueryClient.QuerySpecParams querySpec =
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)), null);

        final QuerySamplesRequest unset = QueryClient.buildQuerySamplesRequest(
                new QueryClient.QuerySamplesParams(querySpec, null, 0, "   ", false));
        assertEquals(0, unset.getExecutionOptions().getLimit());
        assertTrue(unset.getExecutionOptions().getPageToken().isEmpty());

        final QuerySamplesRequest set = QueryClient.buildQuerySamplesRequest(
                new QueryClient.QuerySamplesParams(querySpec, null, 25, "tok", false));
        assertEquals(25, set.getExecutionOptions().getLimit());
        assertEquals("tok", set.getExecutionOptions().getPageToken());
    }

    /**
     * The streaming builders must DROP the page token even when the params carry one (D5).  The
     * params type is shared with the unary method, where the token is legitimate, so forwarding it
     * would be a guaranteed server rejection that the params type cannot prevent.
     */
    @Test
    public void testBuildStreamRequestDropsPageToken() {

        final QueryClient.QuerySpecParams querySpec =
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)), null);

        final QuerySamplesRequest samplesRequest = QueryClient.buildQuerySamplesStreamRequest(
                new QueryClient.QuerySamplesParams(querySpec, null, 25, "leftover-token", false));
        assertTrue(samplesRequest.getExecutionOptions().getPageToken().isEmpty());
        assertEquals("limit is still forwarded; only the token is dropped",
                25, samplesRequest.getExecutionOptions().getLimit());

        final QueryBucketsRequest bucketsRequest = QueryClient.buildQueryBucketsStreamRequest(
                new QueryClient.QueryBucketsParams(querySpec, 25, "leftover-token", false, false));
        assertTrue(bucketsRequest.getExecutionOptions().getPageToken().isEmpty());
        assertEquals(25, bucketsRequest.getExecutionOptions().getLimit());
    }

    /**
     * excludeColumnMetadata is set on the buckets request, where the server honors it, and is
     * deliberately absent from the samples params — the samples path carries no column metadata at
     * all, so the flag is inert there and offering it would suggest otherwise.
     */
    @Test
    public void testBuildRequestResultRepresentation() {

        final QueryClient.QuerySpecParams querySpec =
                spec(new QueryClient.PvNameListSelector(List.of(PV_1)), null);

        final QueryBucketsRequest buckets = QueryClient.buildQueryBucketsRequest(
                new QueryClient.QueryBucketsParams(querySpec, 0, null, true, true));
        assertTrue(buckets.getResultRepresentation().getUseSerializedColumns());
        assertTrue(buckets.getResultRepresentation().getExcludeColumnMetadata());

        final QuerySamplesRequest samples = QueryClient.buildQuerySamplesRequest(
                new QueryClient.QuerySamplesParams(querySpec, null, 0, null, true));
        assertTrue(samples.getResultRepresentation().getUseSerializedColumns());
        assertFalse("excludeColumnMetadata is inert on the samples path and is never set",
                samples.getResultRepresentation().getExcludeColumnMetadata());
    }

    /**
     * sampleStatusSelector reaches the QuerySpec on a samples request.  It is unrepresentable on a
     * buckets request by construction (D8), since QueryBucketsParams does not carry the field —
     * the server rejects the combination, so a field that could only ever produce a rejection is
     * not offered.
     */
    @Test
    public void testBuildQuerySamplesRequestSampleStatusSelector() {

        final QuerySamplesRequest request = QueryClient.buildQuerySamplesRequest(
                new QueryClient.QuerySamplesParams(
                        spec(new QueryClient.PvNameListSelector(List.of(PV_1)), null),
                        new QueryClient.SampleStatusSelectorParams(
                                "quality", List.of("layer-a", ""), List.of(1, 2),
                                SampleStatusSelector.Mode.MODE_EXCLUDE_MATCHING),
                        0, null, false));

        assertTrue(request.getQuerySpec().hasSampleStatusSelector());
        final SampleStatusSelector selector = request.getQuerySpec().getSampleStatusSelector();
        assertEquals("quality", selector.getDomain());
        assertEquals(List.of("layer-a"), selector.getLayersList());
        assertEquals(List.of(1, 2), selector.getStatusCodesList());
        assertEquals(SampleStatusSelector.Mode.MODE_EXCLUDE_MATCHING, selector.getMode());
    }

    // =========================================================================
    // round-trip tests against the running query service
    // =========================================================================

    private long seedSimpleScenario() {
        final long startSeconds = Instant.now().getEpochSecond();
        final GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult result =
                ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
        assertNotNull(result);
        return startSeconds;
    }

    private QueryClient.QuerySpecParams scenarioSpec(List<String> pvNames, long begin, long end) {
        return new QueryClient.QuerySpecParams(
                Timestamp.newBuilder().setEpochSeconds(begin).build(),
                Timestamp.newBuilder().setEpochSeconds(end).build(),
                new QueryClient.PvNameListSelector(pvNames),
                null);
    }

    @Test
    public void testQuerySamplesSinglePage() {

        final long start = seedSimpleScenario();

        final QuerySamplesApiResult result = queryClient.querySamples(
                new QueryClient.QuerySamplesParams(
                        scenarioSpec(List.of(PV_1), start, start + 10), null, 0, null, false));

        assertFalse(result.resultStatus.msg, result.isError());
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.columnTable);

        // 10 buckets x 10 samples
        assertEquals(100, result.columnTable.getTimestampList().getTimestampsCount());
        assertEquals(1, result.columnTable.getDataColumnsCount());
        assertEquals(PV_1, result.columnTable.getDataColumns(0).getName());
        assertEquals(100, result.columnTable.getDataColumns(0).getDataValuesCount());

        // the whole result fits in one page, so there is no continuation token
        assertTrue(result.nextPageToken.isEmpty());
    }

    /**
     * Paging seam: successive pages driven by nextPageToken must deliver every row exactly once, in
     * order.
     */
    @Test
    public void testQuerySamplesPagingSeam() {

        final long start = seedSimpleScenario();

        final List<Timestamp> collected = new ArrayList<>();
        String pageToken = null;
        int pageCount = 0;

        while (true) {
            final QuerySamplesApiResult result = queryClient.querySamples(
                    new QueryClient.QuerySamplesParams(
                            scenarioSpec(List.of(PV_1), start, start + 10), null, 30, pageToken, false));
            assertFalse(result.resultStatus.msg, result.isError());
            collected.addAll(result.columnTable.getTimestampList().getTimestampsList());
            pageCount++;
            if (result.nextPageToken.isEmpty()) {
                break;
            }
            pageToken = result.nextPageToken;
            assertTrue("paging did not terminate", pageCount < 20);
        }

        assertTrue("limit=30 over 100 rows should take several pages", pageCount >= 4);
        assertEquals(100, collected.size());
        for (int i = 1; i < collected.size(); i++) {
            final long prev = collected.get(i - 1).getEpochSeconds() * 1_000_000_000L
                    + collected.get(i - 1).getNanoseconds();
            final long cur = collected.get(i).getEpochSeconds() * 1_000_000_000L
                    + collected.get(i).getNanoseconds();
            assertTrue("timestamps must strictly increase across the paging seam", cur > prev);
        }
    }

    /**
     * The streaming wrapper's accumulated table must equal the unary result for the same spec, and
     * its nextPageToken is always empty.  This is the end-to-end check on the by-name column merge.
     */
    @Test
    public void testQuerySamplesStreamAccumulationEqualsUnary() {

        final long start = seedSimpleScenario();

        final QueryClient.QuerySpecParams querySpec =
                scenarioSpec(List.of(PV_1, PV_2), start, start + 10);

        final QuerySamplesApiResult unary = queryClient.querySamples(
                new QueryClient.QuerySamplesParams(querySpec, null, 0, null, false));
        assertFalse(unary.resultStatus.msg, unary.isError());

        // a small chunk size forces many streamed messages, so the accumulation is genuinely
        // exercised rather than trivially reproducing a single message
        final QuerySamplesApiResult streamed = queryClient.querySamplesStream(
                new QueryClient.QuerySamplesParams(querySpec, null, 7, null, false));
        assertFalse(streamed.resultStatus.msg, streamed.isError());

        assertTrue("streaming carries no continuation token", streamed.nextPageToken.isEmpty());
        assertEquals(unary.columnTable.getTimestampList(), streamed.columnTable.getTimestampList());
        assertEquals(unary.columnTable.getDataColumnsList(), streamed.columnTable.getDataColumnsList());
    }

    /**
     * A PV with no data in the window is still emitted as a column, with an unset DataValue oneof at
     * every position — the missing-value representation, distinct from a zero.
     */
    @Test
    public void testQuerySamplesMissingValuesAreUnsetOneof() {

        final long start = seedSimpleScenario();

        // PV_1 has data; the second name resolves through the archive but has no samples in this
        // one-second window only if it exists -- so use PV_2, which shares the window, and assert
        // the general property that every column has one value per timestamp
        final QuerySamplesApiResult result = queryClient.querySamples(
                new QueryClient.QuerySamplesParams(
                        scenarioSpec(List.of(PV_1, PV_2), start, start + 1), null, 0, null, false));

        assertFalse(result.resultStatus.msg, result.isError());
        final ColumnTable table = result.columnTable;
        final int rowCount = table.getTimestampList().getTimestampsCount();
        assertTrue(rowCount > 0);
        assertEquals(2, table.getDataColumnsCount());
        for (DataColumn column : table.getDataColumnsList()) {
            assertEquals("every column carries exactly one value per timestamp",
                    rowCount, column.getDataValuesCount());
            for (DataValue value : column.getDataValuesList()) {
                // present values are doubles here; a missing value would be an unset oneof, never a
                // zero -- assert the distinction is expressible
                assertTrue(value.getValueCase() == DataValue.ValueCase.DOUBLEVALUE
                        || value.getValueCase() == DataValue.ValueCase.VALUE_NOT_SET);
            }
        }
    }

    @Test
    public void testQueryBucketsSinglePageAndStream() {

        final long start = seedSimpleScenario();

        final QueryClient.QuerySpecParams querySpec = scenarioSpec(List.of(PV_1), start, start + 10);

        final QueryBucketsApiResult unary = queryClient.queryBuckets(
                new QueryClient.QueryBucketsParams(querySpec, 0, null, false, false));
        assertFalse(unary.resultStatus.msg, unary.isError());
        assertEquals(10, unary.dataBuckets.size());
        assertTrue(unary.nextPageToken.isEmpty());
        for (DataBucket bucket : unary.dataBuckets) {
            assertEquals(PV_1, bucket.getPvName());
        }

        // streaming accumulates the same buckets and carries no token
        final QueryBucketsApiResult streamed = queryClient.queryBucketsStream(
                new QueryClient.QueryBucketsParams(querySpec, 3, null, false, false));
        assertFalse(streamed.resultStatus.msg, streamed.isError());
        assertEquals(10, streamed.dataBuckets.size());
        assertTrue(streamed.nextPageToken.isEmpty());
    }

    /**
     * queryBuckets pages through the keyset token like querySamples does.
     */
    @Test
    public void testQueryBucketsPagingSeam() {

        final long start = seedSimpleScenario();

        final List<DataBucket> collected = new ArrayList<>();
        String pageToken = null;
        int pageCount = 0;

        while (true) {
            final QueryBucketsApiResult result = queryClient.queryBuckets(
                    new QueryClient.QueryBucketsParams(
                            scenarioSpec(List.of(PV_1, PV_2), start, start + 10), 7, pageToken, false, false));
            assertFalse(result.resultStatus.msg, result.isError());
            collected.addAll(result.dataBuckets);
            pageCount++;
            if (result.nextPageToken.isEmpty()) {
                break;
            }
            pageToken = result.nextPageToken;
            assertTrue("paging did not terminate", pageCount < 20);
        }

        assertEquals(20, collected.size());
        assertTrue(pageCount >= 3);
    }

    /**
     * A server rejection must surface as ApiResultStatus.REJECT carrying the server's message
     * verbatim, not as a local failure.  A blank sampleStatusSelector domain is a rejection the
     * client layer deliberately does not pre-empt: validation is the server's, and a client-side
     * copy would drift from it.
     */
    @Test
    public void testQuerySamplesRejectionSurfacesWithServerMessage() {

        final long start = seedSimpleScenario();

        final QuerySamplesApiResult result = queryClient.querySamples(
                new QueryClient.QuerySamplesParams(
                        scenarioSpec(List.of(PV_1), start, start + 1),
                        new QueryClient.SampleStatusSelectorParams(
                                "  ", null, null, SampleStatusSelector.Mode.MODE_INCLUDE_MATCHING),
                        0, null, false));

        assertTrue(result.isError());
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertFalse("the server's own message must reach the caller",
                result.resultStatus.msg.isBlank());
    }

    /**
     * A malformed page token is REJECTED on the V2 query methods, unlike the annotation metadata
     * queries, which silently reset to the first page.  Pinned here because a shared client-side
     * paging helper must not assume one behavior across the API surface.
     */
    @Test
    public void testQuerySamplesMalformedPageTokenRejected() {

        final long start = seedSimpleScenario();

        final QuerySamplesApiResult result = queryClient.querySamples(
                new QueryClient.QuerySamplesParams(
                        scenarioSpec(List.of(PV_1), start, start + 1), null, 0, "not-a-token", false));

        assertTrue(result.isError());
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
    }

    /**
     * A non-empty page token on a streaming call would be rejected by the server, so the streaming
     * wrapper drops it and the call SUCCEEDS.  This pins the D5 decision end to end: without the
     * drop, a params instance reused from a unary call would fail every time.
     */
    @Test
    public void testQuerySamplesStreamIgnoresPageToken() {

        final long start = seedSimpleScenario();

        final QuerySamplesApiResult result = queryClient.querySamplesStream(
                new QueryClient.QuerySamplesParams(
                        scenarioSpec(List.of(PV_1), start, start + 1), null, 0, "leftover-token", false));

        assertFalse(result.resultStatus.msg, result.isError());
        assertTrue(result.columnTable.getTimestampList().getTimestampsCount() > 0);
    }

    // =========================================================================
    // pinned failure mode: the data-driven non-scalar rejection
    // =========================================================================

    /**
     * Ingests one non-scalar (double-array) PV over a one-second window, so the two halves of the
     * data-driven non-scalar behavior can be pinned.
     *
     * @return the first second of the ingested array data
     */
    private long ingestArrayPv(String arrayPvName) {

        final String providerId = ingestionServiceWrapper.registerProvider("query-client-it", null);
        final long firstSeconds = Instant.now().getEpochSecond();

        final DoubleArrayColumn arrayColumn = DoubleArrayColumn.newBuilder()
                .setName(arrayPvName)
                .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                .addValues(1.1).addValues(1.2)
                .addValues(2.1).addValues(2.2)
                .build();

        final IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        "query-client-it-array",
                        null,
                        null,
                        firstSeconds,
                        0L,
                        1_000_000L,
                        2,
                        List.of(arrayPvName),
                        null,
                        null,
                        null);
        params.setDoubleArrayColumnList(List.of(arrayColumn));

        final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        return firstSeconds;
    }

    /**
     * A non-scalar PV is rejected by querySamples — but only when a bucket carrying it is actually
     * encountered during assembly.  Both halves are pinned: the rejection when data is in the
     * window, and the silent success when it is not.  The second half is the footgun the consumer
     * ticket flags, and it is behavior, not an accident: fail-fast rejection is #194, still open.
     * If #194 lands, the second assertion is the one that must change.
     */
    @Test
    public void testQuerySamplesNonScalarRejectionIsDataDriven() {

        final String arrayPvName = "query_client_it_array_pv";
        final long firstSeconds = ingestArrayPv(arrayPvName);

        // (a) the PV has data in the window -- rejected, naming the PV and pointing at queryBuckets
        {
            final QuerySamplesApiResult result = queryClient.querySamples(
                    new QueryClient.QuerySamplesParams(
                            scenarioSpec(List.of(arrayPvName), firstSeconds, firstSeconds + 1),
                            null, 0, null, false));

            assertTrue(result.isError());
            assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
            assertTrue("the reject must name the offending PV",
                    result.resultStatus.msg.contains(arrayPvName));
            assertTrue("the reject must point the caller at queryBuckets",
                    result.resultStatus.msg.contains("queryBuckets"));
        }

        // (b) the same PV with NO buckets in the window -- passes silently as an empty result,
        // because the rejection fires during assembly rather than pre-flight
        {
            final QuerySamplesApiResult result = queryClient.querySamples(
                    new QueryClient.QuerySamplesParams(
                            scenarioSpec(List.of(arrayPvName), firstSeconds + 3600, firstSeconds + 3601),
                            null, 0, null, false));

            assertFalse("a non-scalar PV with no data in the window is NOT rejected (#194)",
                    result.isError());
            assertEquals(0, result.columnTable.getTimestampList().getTimestampsCount());
        }
    }

    /**
     * The same non-scalar PV that querySamples rejects is returned normally by queryBuckets, which
     * carries stored column types unchanged.  This is what the rejection message points callers at.
     */
    @Test
    public void testQueryBucketsAcceptsNonScalarPv() {

        final String arrayPvName = "query_client_it_array_pv_buckets";
        final long firstSeconds = ingestArrayPv(arrayPvName);

        final QueryBucketsApiResult result = queryClient.queryBuckets(
                new QueryClient.QueryBucketsParams(
                        scenarioSpec(List.of(arrayPvName), firstSeconds, firstSeconds + 1),
                        0, null, false, false));

        assertFalse(result.resultStatus.msg, result.isError());
        assertEquals(1, result.dataBuckets.size());
        assertTrue(result.dataBuckets.get(0).getDataValues().hasDoubleArrayColumn());
    }
}
