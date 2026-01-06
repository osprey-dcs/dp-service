package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.client.IngestionClient;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class RegisterProviderIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void registerProviderTest() {

        {
            // negative registerProvider() test, name not specified

            // create params, using empty provider name
            final String providerName = "";
            final Map<String, String> attributeMap = null;
            final IngestionClient.RegisterProviderRequestParams params
                    = new IngestionClient.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = true;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus =
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT;
            final String expectedExceptionMessage = "RegisterProviderRequest.providerName must be specified";
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    params,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
        }

        String providerId = null;
        {
            // positive registerProvider() test with all fields

            final String providerName = "Provider-1";
            final String description = "Provides data for IOC-1 vacuum subsystem";
            final List<String> tags = List.of("gauges");
            final Map<String, String> attributeMap = Map.of("IOC", "IOC-1", "subsystem", "vacuum");
            final IngestionClient.RegisterProviderRequestParams params
                    = new IngestionClient.RegisterProviderRequestParams(
                            providerName, description, tags, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            providerId = ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    params,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
            Objects.requireNonNull(providerId);
        }

        String providerIdUpdate = null;
        {
            // positive registerProvider() test, call method again for same provider name
            // pass different field values in params, check that same ProviderDocument is updated correctly

            final String providerName = "Provider-1";
            final String description = "Provides data for IOC-1 vacuum subsystem in sector 1";
            final List<String> tags = List.of("gauges", "pumps");
            final Map<String, String> attributeMap =
                    Map.of("IOC", "IOC-1", "subsystem", "vacuum", "sector", "01");
            final IngestionClient.RegisterProviderRequestParams params
                    = new IngestionClient.RegisterProviderRequestParams(
                    providerName, description, tags, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = false;
            final String expectedProviderId = providerId;
            providerIdUpdate = ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    params,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
            Objects.requireNonNull(providerIdUpdate);
            assertEquals(providerId, providerIdUpdate);
        }

        {
            // positive registerProvider() test for different provider name

            final String providerName = "Provider-2";
            final Map<String, String> attributeMap = Map.of("IOC", "IOC-2", "subsystem", "power");
            final IngestionClient.RegisterProviderRequestParams params
                    = new IngestionClient.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            providerId = ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    params,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
            Objects.requireNonNull(providerId);
        }
    }

}
