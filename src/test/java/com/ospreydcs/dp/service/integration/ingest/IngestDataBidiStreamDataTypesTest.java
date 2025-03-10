package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class IngestDataBidiStreamDataTypesTest extends IngestDataTypesTestBase {

    @Override
    protected List<BucketDocument> sendAndVerifyIngestionRpc_(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest
    ) {
        return sendAndVerifyIngestDataBidiStream(params, ingestionRequest);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void ingestionDataTypesTest() {
        super.ingestionDataTypesTest();
    }
}
