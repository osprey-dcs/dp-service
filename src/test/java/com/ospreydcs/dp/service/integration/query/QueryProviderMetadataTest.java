package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataResponse;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class QueryProviderMetadataTest extends GrpcIntegrationTestBase {

    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void testQueryProviderMetadata() {

        // ingest some data
        IngestionScenarioResult ingestionScenarioResult;
        {
            ingestionScenarioResult = simpleIngestionScenario();
        }

        // queryProviderMetadata() positive test for GCC_INGESTION_PROVIDER using result of simpleIngestionScenario.
        {
            final IngestionProviderInfo gccProviderInfo =
                    ingestionScenarioResult.providerInfoMap.get(GCC_INGESTION_PROVIDER);
            sendAndVerifyQueryProviderMetadata(
                    gccProviderInfo.providerId,
                    gccProviderInfo,
                    false,
                    null);
        }
    }

    @Test
    public void testQueryProviderMetadataReject() {
        // queryProviderMetadata() negative test, rejected because providerId is blank
        {
            final String blankProviderId = "";
            final boolean expectReject = true;
            final String expectedRejectMessage = "QueryProviderMetadataRequest.providerId must be specified";
            sendAndVerifyQueryProviderMetadata(
                    blankProviderId,
                    null,
                    expectReject,
                    expectedRejectMessage);
        }
    }

    @Test
    public void testQueryProviderMetadataNoData() {
        // queryProviderMetadata() negative test, ExceptionalResult because query returns no data
        {
            final String undefinedProviderId = "undefined-provider-id";
            final boolean expectReject = true;
            final String expectedRejectMessage = "query returned no data";
            sendAndVerifyQueryProviderMetadata(
                    undefinedProviderId,
                    null,
                    expectReject,
                    expectedRejectMessage);
        }
    }

}