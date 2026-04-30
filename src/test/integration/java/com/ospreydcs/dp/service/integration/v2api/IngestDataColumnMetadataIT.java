package com.ospreydcs.dp.service.integration.v2api;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests verifying end-to-end persistence of ColumnMetadata through the ingestion pipeline.
 * Tests that metadata supplied in IngestDataRequest columns is correctly stored in MongoDB BucketDocuments.
 */
public class IngestDataColumnMetadataIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * Helper to build a minimal IngestionRequestParams for a single DoubleColumn list.
     */
    private IngestionTestBase.IngestionRequestParams buildParams(
            String providerId,
            String requestId,
            long startSeconds,
            long startNanos,
            int numSamples,
            List<String> columnNames,
            List<DoubleColumn> doubleColumns) {

        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null, null,
                        startSeconds, startNanos,
                        1_000_000L,   // 1ms sample interval
                        numSamples,
                        columnNames,
                        null, null, null, null);
        params.setDoubleColumnList(doubleColumns);
        return params;
    }

    // -----------------------------------------------------------------------
    // Test 1: full metadata (provenance + tags + attributes) is persisted
    // -----------------------------------------------------------------------

    @Test
    public void testIngestDoubleColumnWithFullMetadata() {
        final String providerName = "metadata-provider-1";
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);

        final String pvName = "pv:meta:full";
        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;
        final int numSamples = 2;

        ColumnMetadata metadata = IngestionTestBase.buildColumnMetadata(
                "archiver",
                "epics-bridge",
                Arrays.asList("fast", "critical"),
                Map.of("units", "mm", "site", "SLAC"));

        DoubleColumn column = IngestionTestBase.buildDoubleColumnWithMetadata(
                pvName, Arrays.asList(1.1, 2.2), metadata);

        IngestionTestBase.IngestionRequestParams params = buildParams(
                providerId, "req-full-meta", startSeconds, startNanos, numSamples,
                Collections.singletonList(pvName),
                Collections.singletonList(column));

        IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        List<BucketDocument> buckets = ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        assertEquals(1, buckets.size());
        GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(buckets.get(0), metadata);

        // verify metadata survives the gRPC query API round-trip (applyMetadataToProto() path)
        {
            QueryTestBase.QueryDataRequestParams queryParams = new QueryTestBase.QueryDataRequestParams(
                    Collections.singletonList(pvName),
                    startSeconds, startNanos,
                    startSeconds + 1L, 0L);
            List<DataBucket> queryBuckets = queryServiceWrapper.queryData(queryParams, false, "");
            assertEquals(1, queryBuckets.size());
            DataBucket bucket = queryBuckets.get(0);
            DoubleColumn queriedColumn = bucket.getDataValues().getDoubleColumn();
            assertTrue("metadata must be present in query result column", queriedColumn.hasMetadata());
            assertEquals("metadata returned by query must equal originally ingested metadata",
                    metadata, queriedColumn.getMetadata());
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: column without metadata — columnMetadata must be null
    // -----------------------------------------------------------------------

    @Test
    public void testIngestDoubleColumnWithoutMetadata() {
        final String providerId = ingestionServiceWrapper.registerProvider("metadata-provider-2", null);

        final String pvName = "pv:meta:none";
        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;
        final int numSamples = 2;

        DoubleColumn column = IngestionTestBase.buildDoubleColumnWithMetadata(
                pvName, Arrays.asList(3.3, 4.4), null);

        IngestionTestBase.IngestionRequestParams params = buildParams(
                providerId, "req-no-meta", startSeconds, startNanos, numSamples,
                Collections.singletonList(pvName),
                Collections.singletonList(column));

        IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        List<BucketDocument> buckets = ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        assertEquals(1, buckets.size());
        GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(buckets.get(0), null);
    }

    // -----------------------------------------------------------------------
    // Test 3: provenance-only metadata — tags and attributes must be null
    // -----------------------------------------------------------------------

    @Test
    public void testIngestDoubleColumnWithProvenanceOnly() {
        final String providerId = ingestionServiceWrapper.registerProvider("metadata-provider-3", null);

        final String pvName = "pv:meta:prov";
        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;
        final int numSamples = 2;

        ColumnMetadata metadata = IngestionTestBase.buildColumnMetadata(
                "data-store", "normalizer", null, null);

        DoubleColumn column = IngestionTestBase.buildDoubleColumnWithMetadata(
                pvName, Arrays.asList(5.5, 6.6), metadata);

        IngestionTestBase.IngestionRequestParams params = buildParams(
                providerId, "req-prov-only", startSeconds, startNanos, numSamples,
                Collections.singletonList(pvName),
                Collections.singletonList(column));

        IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        List<BucketDocument> buckets = ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        assertEquals(1, buckets.size());
        GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(buckets.get(0), metadata);

        // additionally verify that tags and attributes are null
        assertNull(buckets.get(0).getDataColumn().getColumnMetadata().getTags());
        assertNull(buckets.get(0).getDataColumn().getColumnMetadata().getAttributes());
    }

    // -----------------------------------------------------------------------
    // Test 4: two columns in the same request — only one has metadata
    // -----------------------------------------------------------------------

    @Test
    public void testIngestMultipleColumnsOnlyOneWithMetadata() {
        final String providerId = ingestionServiceWrapper.registerProvider("metadata-provider-4", null);

        final String pvWithMeta = "pv:meta:yes";
        final String pvNoMeta   = "pv:meta:no";
        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;
        final int numSamples = 2;

        ColumnMetadata metadata = IngestionTestBase.buildColumnMetadata(
                "src", "proc", Collections.singletonList("tagged"), null);

        DoubleColumn colWithMeta = IngestionTestBase.buildDoubleColumnWithMetadata(
                pvWithMeta, Arrays.asList(7.7, 8.8), metadata);
        DoubleColumn colNoMeta = IngestionTestBase.buildDoubleColumnWithMetadata(
                pvNoMeta, Arrays.asList(9.9, 0.1), null);

        List<String> columnNames = Arrays.asList(pvWithMeta, pvNoMeta);
        List<DoubleColumn> columns = Arrays.asList(colWithMeta, colNoMeta);

        IngestionTestBase.IngestionRequestParams params = buildParams(
                providerId, "req-mixed-meta", startSeconds, startNanos, numSamples,
                columnNames, columns);

        IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        List<BucketDocument> buckets = ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        assertEquals(2, buckets.size());

        // find the two buckets by PV name and verify each independently
        BucketDocument bucketWithMeta = null;
        BucketDocument bucketNoMeta = null;
        for (BucketDocument bucket : buckets) {
            if (bucket.getPvName().equals(pvWithMeta)) bucketWithMeta = bucket;
            else if (bucket.getPvName().equals(pvNoMeta)) bucketNoMeta = bucket;
        }
        assertNotNull("expected bucket for " + pvWithMeta, bucketWithMeta);
        assertNotNull("expected bucket for " + pvNoMeta, bucketNoMeta);

        GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(bucketWithMeta, metadata);
        GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(bucketNoMeta, null);
    }

    // -----------------------------------------------------------------------
    // Test 5: one scalar, one array, and one binary column — all with metadata
    // -----------------------------------------------------------------------

    @Test
    public void testIngestAllColumnCategoriesWithMetadata() {
        final String providerId = ingestionServiceWrapper.registerProvider("metadata-provider-5", null);

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;
        final int numSamples = 2;

        ColumnMetadata metadata = IngestionTestBase.buildColumnMetadata(
                "src", "proc",
                Collections.singletonList("test-tag"),
                Map.of("env", "test"));

        // --- scalar: FloatColumn ---
        final String floatPv = "pv:meta:float";
        FloatColumn floatColumn = FloatColumn.newBuilder()
                .setName(floatPv)
                .addValues(1.0f).addValues(2.0f)
                .setMetadata(metadata)
                .build();

        // --- array: DoubleArrayColumn (1D, 3 elements) ---
        final String arrayPv = "pv:meta:doubleArray";
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(3).build();
        DoubleArrayColumn arrayColumn = DoubleArrayColumn.newBuilder()
                .setName(arrayPv)
                .setDimensions(dims)
                // 2 samples * 3 elements = 6 values
                .addAllValues(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
                .setMetadata(metadata)
                .build();

        // --- binary: StructColumn ---
        final String structPv = "pv:meta:struct";
        StructColumn structColumn = StructColumn.newBuilder()
                .setName(structPv)
                .setSchemaId("schema-test-1")
                .addValues(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .addValues(com.google.protobuf.ByteString.copyFrom(new byte[]{4, 5, 6}))
                .setMetadata(metadata)
                .build();

        // Build a single request with all three column types
        List<String> columnNames = Arrays.asList(floatPv, arrayPv, structPv);

        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        "req-all-categories",
                        null, null,
                        startSeconds, startNanos,
                        1_000_000L,
                        numSamples,
                        columnNames,
                        null, null, null, null);
        params.setFloatColumnList(Collections.singletonList(floatColumn));
        params.setDoubleArrayColumnList(Collections.singletonList(arrayColumn));
        params.setStructColumnList(Collections.singletonList(structColumn));

        IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
        List<BucketDocument> buckets = ingestionServiceWrapper.sendAndVerifyIngestData(params, request);

        assertEquals(3, buckets.size());
        for (BucketDocument bucket : buckets) {
            GrpcIntegrationIngestionServiceWrapper.verifyBucketColumnMetadata(bucket, metadata);
        }

        // verify metadata survives the gRPC query API round-trip for the binary (struct) column
        {
            QueryTestBase.QueryDataRequestParams queryParams = new QueryTestBase.QueryDataRequestParams(
                    Collections.singletonList(structPv),
                    startSeconds, startNanos,
                    startSeconds + 1L, 0L);
            List<DataBucket> queryBuckets = queryServiceWrapper.queryData(queryParams, false, "");
            assertEquals(1, queryBuckets.size());
            DataBucket bucket = queryBuckets.get(0);
            StructColumn queriedColumn = bucket.getDataValues().getStructColumn();
            assertTrue("metadata must be present in binary column query result", queriedColumn.hasMetadata());
            assertEquals("metadata returned by query must equal originally ingested metadata",
                    metadata, queriedColumn.getMetadata());
        }
    }
}
