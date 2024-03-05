package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnnotationTest extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void annotationTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

//        final int providerId = INGESTION_PROVIDER_ID;

//        List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();
//
//        // create data for 10 sectors, each containing 3 gauges and 3 bpms
//        for (int sectorIndex = 1 ; sectorIndex <= 10 ; ++sectorIndex) {
//            final String sectorName = String.format("S%02d", sectorIndex);
//
//            // create columns for 3 gccs in each sector
//            for (int gccIndex = 1 ; gccIndex <= 3 ; ++ gccIndex) {
//                final String gccName = sectorName + "-" + String.format("GCC%02d", gccIndex);
//                final String requestIdBase = gccName + "-";
//                final long interval = 100_000_000L;
//                final int numBuckets = 10;
//                final int numSecondsPerBucket = 1;
//                final IngestionColumnInfo columnInfoTenths =
//                        new IngestionColumnInfo(
//                                gccName,
//                                requestIdBase,
//                                interval,
//                                numBuckets,
//                                numSecondsPerBucket);
//                ingestionColumnInfoList.add(columnInfoTenths);
//            }
//
//            // create columns for 3 bpms in each sector
//            for (int bpmIndex = 1 ; bpmIndex <= 3 ; ++ bpmIndex) {
//                final String bpmName = sectorName + "-" + String.format("BPM%02d", bpmIndex);
//                final String requestIdBase = bpmName + "-";
//                final long interval = 100_000_000L;
//                final int numBuckets = 10;
//                final int numSecondsPerBucket = 1;
//                final IngestionColumnInfo columnInfoTenths =
//                        new IngestionColumnInfo(
//                                bpmName,
//                                requestIdBase,
//                                interval,
//                                numBuckets,
//                                numSecondsPerBucket);
//                ingestionColumnInfoList.add(columnInfoTenths);
//            }
//        }
//
//        Map<String, IngestionStreamInfo> validationMap = null;
//        {
//            // perform ingestion for specified list of columns
//            validationMap = ingestDataStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, providerId);
//        }
//
//        {
//            // validate database artifacts from ingestion
//            verifyIngestionDbArtifacts(validationMap);
//        }

        {
            // create annotation
            List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();
            List<String> pvNames = List.of("pv1", "pv2");
            AnnotationTestBase.AnnotationDataBlock dataBlock
                    = new AnnotationTestBase.AnnotationDataBlock(
                            startSeconds, startNanos, startSeconds+1, 0, pvNames);
            dataBlocks.add(dataBlock);

            AnnotationTestBase.AnnotationDataSet dataSet = new AnnotationTestBase.AnnotationDataSet(dataBlocks);

            final int authorId = 1;
            String comment = "this is a test comment";
            AnnotationTestBase.CreateCommentAnnotationParams params =
                    new AnnotationTestBase.CreateCommentAnnotationParams(authorId, dataSet, comment);

            sendAndVerifyCreateCommentAnnotation(params, false, "");
        }

    }

}
