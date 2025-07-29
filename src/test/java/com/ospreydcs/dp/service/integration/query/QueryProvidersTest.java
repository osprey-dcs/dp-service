package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.client.IngestionClient;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

import static com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper.BPM_INGESTION_PROVIDER;
import static com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper.GCC_INGESTION_PROVIDER;

@RunWith(JUnit4.class)
public class QueryProvidersTest extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryProviders() {

        // register gcc provider
        IngestionClient.RegisterProviderRequestParams gccProviderParams;
        String gccProviderId;
        {
            final String providerName = GCC_INGESTION_PROVIDER;
            final String description = "Provider for GCC instruments";
            final List<String> tags = List.of("vacuum", "gauges");
            final Map<String, String> attributes = Map.of(
                    "sector", "01",
                    "subsystem", "vacuum"
            );
            gccProviderParams =
                    new IngestionClient.RegisterProviderRequestParams(
                            providerName,
                            description,
                            tags,
                            attributes
                    );
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            gccProviderId = ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    gccProviderParams,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
        }

        // register bpm provider
        IngestionClient.RegisterProviderRequestParams bpmProviderParams;
        String bpmProviderId;
        {
            final String providerName = BPM_INGESTION_PROVIDER;
            final String description = "Provider for BPM instruments";
            final List<String> tags = List.of("diagnostics", "monitors");
            final Map<String, String> attributes = Map.of(
                    "sector", "01",
                    "subsystem", "diagnostics"
            );
            bpmProviderParams =
                    new IngestionClient.RegisterProviderRequestParams(
                            providerName,
                            description,
                            tags,
                            attributes
                    );
            final boolean expectExceptionalResponse = false;
            final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
            final String expectedExceptionMessage = null;
            boolean expectedIsNew = true;
            final String expectedProviderId = null;
            bpmProviderId = ingestionServiceWrapper.sendAndVerifyRegisterProvider(
                    bpmProviderParams,
                    expectExceptionalResponse,
                    expectedExceptionStatus,
                    expectedExceptionMessage,
                    expectedIsNew,
                    expectedProviderId);
        }

        // queryProviders() positive test: empty query result
        {
            final String textCriterion = "garbage";
            final QueryTestBase.QueryProvidersRequestParams requestParams = new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setTextCriterion(textCriterion);

            final int numMatchesExpected = 0;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    numMatchesExpected,
                    expectReject,
                    expectedRejectMessage);
        }

        // queryProviders() positive test: query by IdCriterion
        {
            final String idCriterion = gccProviderId;
            final QueryTestBase.QueryProvidersRequestParams requestParams = new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setIdCriterion(idCriterion);
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    1,
                    false,
                    null);
        }

        // queryProviders() positive test: query by TextCriterion
        {
            final String textCriterion = "BPM";
            final QueryTestBase.QueryProvidersRequestParams requestParams = new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setTextCriterion(textCriterion);
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    1,
                    false,
                    null);
        }

        // queryProviders() positive test: query by TagsCriterion
        {
            final String tagsCriterion = "diagnostics";
            final QueryTestBase.QueryProvidersRequestParams requestParams = new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setTagsCriterion(tagsCriterion);
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    1,
                    false,
                    null);
        }

        // queryProviders() positive test: query by AttributesCriterion
        {
            final String attributesCriterionKey = "sector";
            final String attributesCriterionValue = "01";
            QueryTestBase.QueryProvidersRequestParams requestParams = new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setAttributesCriterion(attributesCriterionKey, attributesCriterionValue);
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    2,
                    false,
                    null);
        }

    }

    @Test
    public void testQueryProvidersNegative() {

        // queryProviders() negative test: rejected because idCriterion is blank
        {
            final String idCriterion = "";
            final QueryTestBase.QueryProvidersRequestParams requestParams =
                    new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setIdCriterion(idCriterion);

            final int numMatchesExpected = 0;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryProvidersRequest.criteria.IdCriterion id must be specified";
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    numMatchesExpected,
                    expectReject,
                    expectedRejectMessage);
        }

        // queryProviders() negative test: rejected because TextCriterion is blank
        {
            final String textCriterion = "";
            final QueryTestBase.QueryProvidersRequestParams requestParams =
                    new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setTextCriterion(textCriterion);

            final int numMatchesExpected = 0;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryProvidersRequest.criteria.TextCriterion text must be specified";
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    numMatchesExpected,
                    expectReject,
                    expectedRejectMessage);
        }

        // queryProviders() negative test: rejected because TagsCriterion is blank
        {
            final String tagsCriterion = "";
            final QueryTestBase.QueryProvidersRequestParams requestParams =
                    new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setTagsCriterion(tagsCriterion);

            final int numMatchesExpected = 0;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryProvidersRequest.criteria.TagsCriterion tagValue must be specified";
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    numMatchesExpected,
                    expectReject,
                    expectedRejectMessage);
        }

        // queryProviders() negative test: rejected because attributes key is blank
        {
            final String attributesCriterionKey = "";
            final String attributesCriterionValue = "01";
            final QueryTestBase.QueryProvidersRequestParams requestParams =
                    new QueryTestBase.QueryProvidersRequestParams();
            requestParams.setAttributesCriterion(attributesCriterionKey, attributesCriterionValue);

            final int numMatchesExpected = 0;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryProvidersRequest.criteria.AttributesCriterion key must be specified";
            queryServiceWrapper.sendAndVerifyQueryProviders(
                    requestParams,
                    numMatchesExpected,
                    expectReject,
                    expectedRejectMessage);
        }

    }
}