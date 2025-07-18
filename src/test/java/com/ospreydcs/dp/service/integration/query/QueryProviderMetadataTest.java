package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;

import static com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper.GCC_INGESTION_PROVIDER;

@RunWith(JUnit4.class)
public class QueryProviderMetadataTest extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryProviderMetadata() {

        // ingest some data
        GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult;
        {
            ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(Instant.now().getEpochSecond(), false);
        }

        // queryProviderMetadata() positive test for empty query result.
        {
            final String undefinedProviderId = "undefined-provider-id";
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            queryServiceWrapper.sendAndVerifyQueryProviderMetadata(
                    undefinedProviderId,
                    null,
                    expectReject,
                    expectedRejectMessage,
                    0);
        }

        // queryProviderMetadata() positive test for GCC_INGESTION_PROVIDER using result of simpleIngestionScenario.
        {
            final GrpcIntegrationIngestionServiceWrapper.IngestionProviderInfo gccProviderInfo =
                    ingestionScenarioResult.providerInfoMap().get(GCC_INGESTION_PROVIDER);
            queryServiceWrapper.sendAndVerifyQueryProviderMetadata(
                    gccProviderInfo.providerId(),
                    gccProviderInfo,
                    false,
                    null,
                    1);
        }
    }

    @Test
    public void testQueryProviderMetadataReject() {
        // queryProviderMetadata() negative test, rejected because providerId is blank
        {
            final String blankProviderId = "";
            final boolean expectReject = true;
            final String expectedRejectMessage = "QueryProviderMetadataRequest.providerId must be specified";
            queryServiceWrapper.sendAndVerifyQueryProviderMetadata(
                    blankProviderId,
                    null,
                    expectReject,
                    expectedRejectMessage,
                    1);
        }
    }

}