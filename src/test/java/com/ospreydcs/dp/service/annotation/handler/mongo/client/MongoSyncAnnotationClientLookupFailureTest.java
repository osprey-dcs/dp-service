package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * A failed Configuration lookup inside saveConfigurationActivation must come back as an error
 * result, not escape the method.
 *
 * <p>findConfigurationByName used to wrap query failures in a bare RuntimeException, which is not a
 * MongoException and so slipped past saveConfigurationActivation's catch clause and out of the job.
 * QueueHandlerBase's worker logged it and moved on, meaning SaveConfigurationActivationJob never
 * reached dispatcher.handleResult() — the caller's response stream stayed open until it timed out,
 * with no error ever sent. These tests pin the two halves of the fix: the failure is classified as
 * an error, and it is not classified as the "no such Configuration" rejection that sits on the
 * adjacent branch.
 *
 * <p>deletePvMetadata had the same defect through findPvMetadataByNameOrAlias, and worse: it called
 * the helper with no try/catch at all, so the unchecked exception escaped straight through.
 */
@RunWith(JUnit4.class)
public class MongoSyncAnnotationClientLookupFailureTest {

    private static ConfigurationActivationDocument activationDocument() {
        final ConfigurationActivationDocument document = new ConfigurationActivationDocument();
        document.setConfigurationName("beamline-a");
        document.setStartTime(Instant.ofEpochSecond(1_000_000L));
        document.setEndTime(Instant.ofEpochSecond(1_000_100L));
        return document;
    }

    @Test
    public void testSaveActivationLookupFailureReturnsErrorResult() throws Exception {

        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.findConfigurationByName(anyString()))
                .thenThrow(new DpException("error querying ConfigurationDocument by name: connection refused"));
        when(client.saveConfigurationActivation(any())).thenCallRealMethod();

        final MongoSaveResult result = client.saveConfigurationActivation(activationDocument());

        // the failure must be reported, not thrown out of the method
        assertNotNull("saveConfigurationActivation returned null instead of a result", result);
        assertTrue("a failed lookup must be flagged as a failure", result.isError);

        // and it must be an error, not the business-rule rejection on the adjacent branch:
        // a database outage is retryable, "no such Configuration" is not
        assertFalse(
                "a failed lookup must not be reported as a rejection - that inverts the retry decision",
                result.isReject);

        assertTrue(
                "error message should identify the failing lookup, was: " + result.message,
                result.message.contains("error looking up Configuration"));
        assertTrue(
                "error message should carry the underlying cause, was: " + result.message,
                result.message.contains("connection refused"));
    }

    @Test
    public void testSaveActivationAbsentConfigurationStillRejects() throws Exception {

        // the contrast case: a lookup that succeeds and finds nothing is still a rejection
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.findConfigurationByName(anyString())).thenReturn(null);
        when(client.saveConfigurationActivation(any())).thenCallRealMethod();

        final MongoSaveResult result = client.saveConfigurationActivation(activationDocument());

        assertNotNull(result);
        assertTrue("an absent Configuration is a failure", result.isError);
        assertTrue("an absent Configuration is a business-rule rejection", result.isReject);
        assertTrue(
                "reject message should name the missing Configuration, was: " + result.message,
                result.message.contains("no Configuration found for configurationName"));
    }

    @Test
    public void testDeletePvMetadataLookupFailureReturnsErrorResult() throws Exception {

        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.findPvMetadataByNameOrAlias(anyString()))
                .thenThrow(new DpException("error querying PvMetadataDocument by name or alias: connection refused"));
        when(client.deletePvMetadata(anyString())).thenCallRealMethod();

        final MongoDeleteResult result = client.deletePvMetadata("DEL:PV:001");

        assertNotNull("deletePvMetadata returned null instead of a result", result);
        assertTrue("a failed lookup must be flagged as a failure", result.isError);

        // must not be confused with the not-found path, which returns isError=false and a null
        // identifier and is turned into a rejection by the dispatcher
        assertFalse(
                "a failed lookup must not be reported as a rejection - that inverts the retry decision",
                result.isReject);
        assertTrue(
                "error message should identify the failing lookup, was: " + result.message,
                result.message.contains("error looking up PvMetadata"));
    }

    @Test
    public void testDeletePvMetadataAbsentRecordIsNotAnError() throws Exception {

        // the contrast case: a lookup that succeeds and finds nothing is the not-found signal,
        // which the dispatcher converts to a rejection - not an error
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.findPvMetadataByNameOrAlias(anyString())).thenReturn(null);
        when(client.deletePvMetadata(anyString())).thenCallRealMethod();

        final MongoDeleteResult result = client.deletePvMetadata("DEL:PV:404");

        assertNotNull(result);
        assertFalse("an absent record is not an error at this layer", result.isError);
        assertNull("an absent record is signalled by a null identifier", result.deletedIdentifier);
    }

    @Test
    public void testDeleteAnnotationLookupFailureReturnsErrorResult() throws Exception {

        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.lookupAnnotation(anyString()))
                .thenThrow(new DpException("error querying AnnotationDocument by id: connection refused"));
        when(client.deleteAnnotation(anyString())).thenCallRealMethod();

        final MongoDeleteResult result = client.deleteAnnotation("66a1b2c3d4e5f60718293a4b");

        assertNotNull("deleteAnnotation returned null instead of a result", result);
        assertTrue("a failed lookup must be flagged as a failure", result.isError);
        assertFalse(
                "a failed lookup must not be reported as a rejection - that inverts the retry decision",
                result.isReject);
        assertTrue(
                "error message should identify the failing lookup, was: " + result.message,
                result.message.contains("error looking up AnnotationDocument"));
    }

    @Test
    public void testDeleteAnnotationAbsentRecordIsNotAnError() throws Exception {

        // the contrast case: a lookup that succeeds and finds nothing is the not-found signal,
        // which the dispatcher converts to a rejection - not an error
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.lookupAnnotation(anyString())).thenReturn(null);
        when(client.deleteAnnotation(anyString())).thenCallRealMethod();

        final MongoDeleteResult result = client.deleteAnnotation("66a1b2c3d4e5f60718293a4b");

        assertNotNull(result);
        assertFalse("an absent record is not an error at this layer", result.isError);
        assertNull("an absent record is signalled by a null identifier", result.deletedIdentifier);
    }

    // ------------------------------------------------------------------
    // save-validation lookups (MongoAnnotationHandler.validateSaveAnnotationRequest)
    // ------------------------------------------------------------------

    private static SaveAnnotationRequest saveAnnotationRequest() {
        return SaveAnnotationRequest.newBuilder()
                .setOwnerId("craigmcc")
                .setName("lookup failure test")
                .addAllDataSetIds(List.of("66a1b2c3d4e5f60718293a4b"))
                .build();
    }

    @Test
    public void testSaveAnnotationValidationLookupFailureReportsErrorNotAbsence() throws Exception {

        // A Mongo outage during save validation must not read as "your dataSetId does not exist" —
        // that message asserts a fact the lookup never established, and inverts the retry decision.
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.lookupDataSet(anyString()))
                .thenThrow(new DpException("error querying DataSetDocument by id: connection refused"));
        final MongoAnnotationHandler handler = new MongoAnnotationHandler(client, null);

        final ResultStatus resultStatus = handler.validateSaveAnnotationRequest(saveAnnotationRequest());

        assertTrue("a failed lookup must fail validation", resultStatus.isError);
        assertTrue(
                "message should identify the failing lookup, was: " + resultStatus.msg,
                resultStatus.msg.contains("error looking up DataSetDocument"));
        assertFalse(
                "a failed lookup must not be reported as an absent document",
                resultStatus.msg.contains("no DataSetDocument found"));
    }

    @Test
    public void testSaveAnnotationValidationAbsentDataSetStillRejects() throws Exception {

        // the contrast case: a lookup that succeeds and finds nothing is a genuine absence
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.lookupDataSet(anyString())).thenReturn(null);
        final MongoAnnotationHandler handler = new MongoAnnotationHandler(client, null);

        final ResultStatus resultStatus = handler.validateSaveAnnotationRequest(saveAnnotationRequest());

        assertTrue(resultStatus.isError);
        assertTrue(
                "message should name the missing document, was: " + resultStatus.msg,
                resultStatus.msg.contains("no DataSetDocument found"));
    }

    // ------------------------------------------------------------------
    // lookupCalculations (findCalculations previously collapsed absent / failed / malformed)
    // ------------------------------------------------------------------

    @Test
    public void testFindCalculationsCollapsesLookupFailureToNull() throws Exception {

        // findCalculations keeps the legacy swallow-to-null contract for callers that cannot act
        // on the distinction; lookupCalculations is the checked variant for those that must
        final MongoSyncAnnotationClient client = mock(MongoSyncAnnotationClient.class);
        when(client.lookupCalculations(anyString()))
                .thenThrow(new DpException("error querying CalculationsDocument by id: connection refused"));
        when(client.findCalculations(anyString())).thenCallRealMethod();

        assertNull(client.findCalculations("66a1b2c3d4e5f60718293a4b"));
    }
}
