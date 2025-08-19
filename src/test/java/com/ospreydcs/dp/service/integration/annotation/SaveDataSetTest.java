package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SaveDataSetTest extends AnnotationIntegrationTestIntermediate {

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
                    params, false, true, "DataSet name must be specified");
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
}
