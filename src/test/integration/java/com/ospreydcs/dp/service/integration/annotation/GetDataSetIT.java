package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GetDataSetIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testGetDataSet() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);

        {
            // getDataSet() positive test - retrieve a dataset saved without optional fields

            final DataSet dataSet = annotationServiceWrapper.sendAndVerifyGetDataSet(
                    scenarioResult.firstHalfDataSetId(), false, null);

            final AnnotationTestBase.AnnotationDataSet expectedDataSet =
                    scenarioResult.firstHalfDataSetParams().dataSet();
            assertEquals(expectedDataSet.name(), dataSet.getName());
            assertEquals(expectedDataSet.ownerId(), dataSet.getOwnerId());
            assertEquals(expectedDataSet.description(), dataSet.getDescription());
            assertEquals(expectedDataSet.dataBlocks().size(), dataSet.getDataBlocksCount());

            // audit fields: createdTime is server-set; updatedTime stays unset until the first
            // update (#248 plan D13, matching the other entity types in this service)
            assertTrue(dataSet.hasCreatedTime());
            assertFalse(dataSet.hasUpdatedTime());
            assertTrue(dataSet.getModifiedBy().isEmpty());
            assertTrue(dataSet.getTagsList().isEmpty());
            assertTrue(dataSet.getAttributesList().isEmpty());
        }

        {
            // getDataSet() positive test - tags/attributes/modifiedBy round-trip, tags normalized

            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = List.of(
                    new AnnotationTestBase.AnnotationDataBlock(
                            startSeconds, 0L, startSeconds + 1, 0L, List.of("S01-GCC01", "S01-BPM01")));
            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(
                            null, "tagged dataset", "craigmcc", "tags round-trip", dataBlocks,
                            List.of("Beam Loss", "OUTAGE", "beam loss"),
                            Map.of("sector", "01", "subsystem", "vacuum"),
                            "operator-1");
            final String dataSetId = annotationServiceWrapper.sendAndVerifySaveDataSet(
                    new AnnotationTestBase.SaveDataSetParams(dataSet), false, false, "");

            final DataSet taggedDataSet = annotationServiceWrapper.sendAndVerifyGetDataSet(
                    dataSetId, false, null);

            // tags are normalized on save: lowercase, deduplicated, sorted
            assertEquals(List.of("beam loss", "outage"), taggedDataSet.getTagsList());
            assertEquals(2, taggedDataSet.getAttributesCount());
            assertEquals("operator-1", taggedDataSet.getModifiedBy());
            assertTrue(taggedDataSet.hasCreatedTime());
        }
    }

    @Test
    public void testGetDataSetRejects() {

        final long startSeconds = Instant.now().getEpochSecond();

        // no data needed: every case below is rejected before any dataset is consulted, except the
        // not-found case, which needs only an id that matches nothing
        {
            // getDataSet() negative test - id that matches no record
            final String missingId = new ObjectId().toHexString();
            annotationServiceWrapper.sendAndVerifyGetDataSet(
                    missingId, true, "no DataSet record found for id: " + missingId);
        }

        {
            // getDataSet() negative test - blank id
            annotationServiceWrapper.sendAndVerifyGetDataSet(
                    "", true, "GetDataSetRequest.dataSetId must be specified");
        }

        {
            // getDataSet() negative test - malformed id is a client mistake, rejected before the
            // ObjectId constructor can throw inside the worker thread (#248 plan D11)
            annotationServiceWrapper.sendAndVerifyGetDataSet(
                    "not-an-object-id", true,
                    "GetDataSetRequest.dataSetId is not a valid id: not-an-object-id");
        }
    }
}
