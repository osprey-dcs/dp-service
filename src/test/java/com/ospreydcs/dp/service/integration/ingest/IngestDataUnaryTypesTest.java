package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class IngestDataUnaryTypesTest extends IngestDataTypesTestBase {

    @Override
    protected List<BucketDocument> sendAndVerifyIngestionRpc_(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest
    ) {
        return sendAndVerifyIngestData(params, ingestionRequest);
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
