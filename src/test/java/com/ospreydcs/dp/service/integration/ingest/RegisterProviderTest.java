package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class RegisterProviderTest extends GrpcIntegrationTestBase {

    // static variables
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
    public void registerProviderTest() {

        {
            // negative registerProvider() test, name not specified

            // create params, using empty provider name
            final String providerName = "";
            final Map<String, String> attributeMap = null;
            final IngestionTestBase.RegisterProviderRequestParams params
                    = new IngestionTestBase.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = true;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus =
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT;
            final String expectedExceptionMessage = "RegisterProviderRequest.providerName must be specified";
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            sendAndVerifyRegisterProvider(
                    params,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
        }

        String providerId = null;
        {
            // positive registerProvider() test

            // create params, using empty provider name
            final String providerName = "Provider-1";
            final Map<String, String> attributeMap = Map.of("IOC", "IOC-1", "subsystem", "vacuum");
            final IngestionTestBase.RegisterProviderRequestParams params
                    = new IngestionTestBase.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            providerId = sendAndVerifyRegisterProvider(
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
            // pass different attributeMap contents, check that same providerId is updated with new map

            // create params, using empty provider name
            final String providerName = "Provider-1";
            final Map<String, String> attributeMap =
                    Map.of("IOC", "IOC-1", "subsystem", "vacuum", "sector", "01");
            final IngestionTestBase.RegisterProviderRequestParams params
                    = new IngestionTestBase.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = false;
            final String expectedProviderId = providerId;
            providerIdUpdate = sendAndVerifyRegisterProvider(
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

            // create params, using empty provider name
            final String providerName = "Provider-2";
            final Map<String, String> attributeMap = Map.of("IOC", "IOC-2", "subsystem", "power");
            final IngestionTestBase.RegisterProviderRequestParams params
                    = new IngestionTestBase.RegisterProviderRequestParams(providerName, attributeMap);

            // send and verify API request
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            providerId = sendAndVerifyRegisterProvider(
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
