package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;

@RunWith(JUnit4.class)
public class IngestDataBidiStreamSimpleIngestionScenarioTest extends GrpcIntegrationTestBase {

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
            ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(Instant.now().getEpochSecond());
        }
    }

}
