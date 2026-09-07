package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SaveDataSetIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testSaveDataSetReject() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        annotationIngestionScenario(startSeconds);

        {
            // createDataSet() negative test - request should be rejected because name not specified

            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();

            // create data block with pvNames that do exist in archive
            final List<String> pvNamesValid = List.of("S01-GCC01", "S02-GCC01");
            final AnnotationTestBase.AnnotationDataBlock dataBlockValid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesValid);
            dataBlocks.add(dataBlockValid);

            final String ownerId = "craigmcc";
            final String unspecifiedName = "";
            final String description = "reject test";
            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(null, unspecifiedName, ownerId, description, dataBlocks);

            final AnnotationTestBase.SaveDataSetParams params =
                    new AnnotationTestBase.SaveDataSetParams(dataSet);

            annotationServiceWrapper.sendAndVerifySaveDataSet(
                    params, false, true, "SaveDataSetRequest.name must be specified");
        }

        {
            // createDataSet() negative test - request should be rejected because some PVs don't exist in the archive

            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();

            // create data block with pvNames that don't exist in archive
            final List<String> pvNamesInvalid = List.of("pv1", "pv2");
            final AnnotationTestBase.AnnotationDataBlock dataBlockInvalid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesInvalid);
            dataBlocks.add(dataBlockInvalid);

            // create data block with pvNames that do exist in archive
            final List<String> pvNamesValid = List.of("S01-GCC01", "S02-GCC01");
            final AnnotationTestBase.AnnotationDataBlock dataBlockValid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesValid);
            dataBlocks.add(dataBlockValid);

            // create data block with both pvNames that do and do not exist in archive
            final List<String> pvNamesMixed = List.of("S01-BPM01", "pv3");
            final AnnotationTestBase.AnnotationDataBlock dataBlockMixed
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesMixed);
            dataBlocks.add(dataBlockMixed);

            final String ownerId = "craigmcc";
            final String name = "missing PV test";
            final String description = "negative test, PVs don't exist in archive";
            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(null, name, ownerId, description, dataBlocks);

            final AnnotationTestBase.SaveDataSetParams params =
                    new AnnotationTestBase.SaveDataSetParams(dataSet);

            annotationServiceWrapper.sendAndVerifySaveDataSet(
                    params, false, true, "no PV metadata found for names: [pv1, pv2, pv3]");
        }

        // positive test case defined in super class so it can be used to generate datasets for other tests
        createDataSetScenario(startSeconds);
    }

    @Test
    public void testSaveDataSetAuditFields() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data
        annotationIngestionScenario(startSeconds);

        final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = List.of(
                new AnnotationTestBase.AnnotationDataBlock(
                        startSeconds, 0L, startSeconds + 1, 0L, List.of("S01-GCC01", "S01-BPM01")));

        String dataSetId;
        Instant createdAt;
        {
            // saveDataSet() positive test - create with tags/attributes/modifiedBy; tags are
            // normalized on save, and updatedTime stays unset on create (#248 plan D12, D13)

            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(
                            null, "audit fields dataset", "craigmcc", "audit field test", dataBlocks,
                            List.of("Beam Loss", "OUTAGE", "beam loss"),
                            Map.of("sector", "01"),
                            "operator-1");
            dataSetId = annotationServiceWrapper.sendAndVerifySaveDataSet(
                    new AnnotationTestBase.SaveDataSetParams(dataSet), false, false, "");

            final DataSetDocument document = mongoClient.findDataSet(dataSetId);
            assertEquals(List.of("beam loss", "outage"), document.getTags());
            assertEquals(Map.of("sector", "01"), document.getAttributes());
            assertEquals("operator-1", document.getModifiedBy());
            assertNotNull(document.getCreatedAt());
            assertNull(document.getUpdatedAt());
            createdAt = document.getCreatedAt();
        }

        {
            // saveDataSet() positive test - full-replace update preserves createdTime and sets
            // updatedTime; modifiedBy is replaced with the new writer

            final AnnotationTestBase.AnnotationDataSet updatedDataSet =
                    new AnnotationTestBase.AnnotationDataSet(
                            dataSetId, "audit fields dataset", "craigmcc", "audit field test", dataBlocks,
                            List.of("recalibrated"),
                            Map.of("sector", "02"),
                            "operator-2");
            annotationServiceWrapper.sendAndVerifySaveDataSet(
                    new AnnotationTestBase.SaveDataSetParams(updatedDataSet), true, false, "");

            final DataSetDocument document = mongoClient.findDataSet(dataSetId);
            assertEquals(List.of("recalibrated"), document.getTags());
            assertEquals("operator-2", document.getModifiedBy());
            assertEquals(createdAt, document.getCreatedAt());
            assertNotNull(document.getUpdatedAt());
        }
    }
}
