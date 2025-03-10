package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IngestDataBidiStreamSimpleIngestionScenarioTest extends GrpcIntegrationTestBase {

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
    }

}
