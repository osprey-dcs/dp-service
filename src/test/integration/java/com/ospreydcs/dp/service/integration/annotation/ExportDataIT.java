package com.ospreydcs.dp.service.integration.annotation;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.bson.types.ObjectId;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ExportDataIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testExportData() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        final Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap =
                annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        {
            // export to hdf5, negative test, unspecified dataset id
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            "",
                            null,
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "ExportDataRequest must specify at least one of dataSetId, dataBlocks, or calculationsSpec");
        }

        {
            // export to hdf5, negative test, invalid dataset id
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            "1234abcd1234abcd1234abcd",
                            null,
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "DatasetDocument with id 1234abcd1234abcd1234abcd not found");
        }

        {
            // export to hdf5, negative test, unspecified output format
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            createDataSetScenarioResult.firstHalfDataSetId(),
                            null,
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "valid ExportDataRequest.outputFormat must be specified");
        }

        {
            // export to hdf5, positive test
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            createDataSetScenarioResult.firstHalfDataSetId(),
                            createDataSetScenarioResult.firstHalfDataSetParams(),
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0, // expectedNumRows ignored for bucketed export
                            validationMap,
                            false,
                            "");
        }

        {
            // export to csv, positive test
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            createDataSetScenarioResult.firstHalfDataSetId(),
                            createDataSetScenarioResult.firstHalfDataSetParams(),
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            25, // 2.5 seconds of data with 10 values per second
                            validationMap,
                            false,
                            "");
        }

        {
            // export to xlsx, positive test
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            createDataSetScenarioResult.firstHalfDataSetId(),
                            createDataSetScenarioResult.firstHalfDataSetParams(),
                            null,
                            null,
                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            25, // 2.5 seconds of data with 10 values per second
                            validationMap,
                            false,
                            "");
        }


        // ------------------------------------------------------------------
        // #248 Phase 4 (D30/D31): reject classification and inline dataBlocks
        // ------------------------------------------------------------------

        {
            // negative test: malformed dataSetId is a REJECT naming the malformation,
            // not "not found" (D30)
            annotationServiceWrapper.sendAndVerifyExportData(
                    "junk-id",
                    null,
                    null,
                    null,
                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                    0,
                    0,
                    null,
                    true,
                    "ExportDataRequest.dataSetId is not a valid id: junk-id");
        }

        {
            // negative test: calculations id that matches no record is a REJECT (D30)
            final String missingCalculationsId = new ObjectId().toHexString();
            annotationServiceWrapper.sendAndVerifyExportData(
                    null,
                    null,
                    CalculationsSpec.newBuilder().setCalculationsId(missingCalculationsId).build(),
                    null,
                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                    0,
                    0,
                    null,
                    true,
                    "CalculationsDocument with id " + missingCalculationsId + " not found");
        }

        {
            // negative test: inline dataBlock with no pvNames is rejected in validation (D31)
            final ExportDataRequest request = AnnotationTestBase.buildExportDataRequest(
                    null,
                    List.of(DataBlock.newBuilder()
                            .setBeginTime(Timestamp.newBuilder().setEpochSeconds(startSeconds))
                            .setEndTime(Timestamp.newBuilder().setEpochSeconds(startSeconds + 1))
                            .build()),
                    null,
                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV);
            annotationServiceWrapper.sendExportData(
                    request, true, "ExportDataRequest.dataBlocks.pvNames must not be empty");
        }

        {
            // negative test: inline dataBlock with zero beginTime is rejected in validation (D31)
            final ExportDataRequest request = AnnotationTestBase.buildExportDataRequest(
                    null,
                    List.of(DataBlock.newBuilder()
                            .setEndTime(Timestamp.newBuilder().setEpochSeconds(startSeconds + 1))
                            .addPvNames("S01-GCC01")
                            .build()),
                    null,
                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV);
            annotationServiceWrapper.sendExportData(
                    request, true, "ExportDataRequest.dataBlocks.beginTime must be non-zero");
        }

        final List<DataBlock> firstHalfBlocks =
                protoDataBlocks(createDataSetScenarioResult.firstHalfDataSetParams());
        final List<DataBlock> secondHalfBlocks =
                protoDataBlocks(createDataSetScenarioResult.secondHalfDataSetParams());

        {
            // positive test: inline-dataBlocks-only CSV export produces content identical to the
            // saved-dataset export over the same blocks (D31 — the inline path is a transient
            // DataSet, so the same blocks must yield the same file), keyed by a generated
            // ObjectId filename rather than a stored id
            final ExportDataResponse.ExportDataResult datasetCsvResult =
                    annotationServiceWrapper.sendExportData(
                            AnnotationTestBase.buildExportDataRequest(
                                    createDataSetScenarioResult.firstHalfDataSetId(),
                                    null,
                                    null,
                                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV),
                            false, "");
            final ExportDataResponse.ExportDataResult inlineCsvResult =
                    annotationServiceWrapper.sendExportData(
                            AnnotationTestBase.buildExportDataRequest(
                                    null,
                                    firstHalfBlocks,
                                    null,
                                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV),
                            false, "");
            assertArrayEquals(fileBytes(datasetCsvResult), fileBytes(inlineCsvResult));

            final String inlineStem = exportFilenameStem(inlineCsvResult);
            assertTrue("inline export filename stem must be a generated ObjectId: " + inlineStem,
                    ObjectId.isValid(inlineStem));
            assertNotEquals(createDataSetScenarioResult.firstHalfDataSetId(), inlineStem);
        }

        {
            // positive test: inline-dataBlocks-only HDF5 export writes the transient dataset's
            // blocks (D31); content verified against a transient document built from the same blocks
            final ExportDataResponse.ExportDataResult inlineHdf5Result =
                    annotationServiceWrapper.sendExportData(
                            AnnotationTestBase.buildExportDataRequest(
                                    null,
                                    firstHalfBlocks,
                                    null,
                                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5),
                            false, "");
            final DataSetDocument transientDataSet = new DataSetDocument();
            transientDataSet.setDataBlocks(
                    firstHalfBlocks.stream().map(DataBlockDocument::fromDataBlock).toList());
            final IHDF5Reader reader = HDF5Factory.openForReading(inlineHdf5Result.getFilePath());
            AnnotationTestBase.verifyDatasetHdf5Content(reader, transientDataSet);
            reader.close();
        }

        {
            // positive test: dataSetId and inline dataBlocks combine — the effective block list
            // is the stored dataset's blocks plus the inline ones (D31), so combining the stored
            // first-half dataset with inline second-half blocks must equal exporting the union
            // of both halves inline
            final ExportDataResponse.ExportDataResult combinedCsvResult =
                    annotationServiceWrapper.sendExportData(
                            AnnotationTestBase.buildExportDataRequest(
                                    createDataSetScenarioResult.firstHalfDataSetId(),
                                    secondHalfBlocks,
                                    null,
                                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV),
                            false, "");
            final List<DataBlock> unionBlocks = new ArrayList<>(firstHalfBlocks);
            unionBlocks.addAll(secondHalfBlocks);
            final ExportDataResponse.ExportDataResult unionCsvResult =
                    annotationServiceWrapper.sendExportData(
                            AnnotationTestBase.buildExportDataRequest(
                                    null,
                                    unionBlocks,
                                    null,
                                    ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV),
                            false, "");
            assertArrayEquals(fileBytes(unionCsvResult), fileBytes(combinedCsvResult));

            // the combined export is keyed by the stored dataset's id
            assertEquals(createDataSetScenarioResult.firstHalfDataSetId(),
                    exportFilenameStem(combinedCsvResult));
        }

    }

    /** Converts a saveDataSet params block list to the proto DataBlocks an ExportDataRequest carries. */
    private static List<DataBlock> protoDataBlocks(AnnotationTestBase.SaveDataSetParams params) {
        final List<DataBlock> dataBlocks = new ArrayList<>();
        for (AnnotationTestBase.AnnotationDataBlock block : params.dataSet().dataBlocks()) {
            dataBlocks.add(DataBlock.newBuilder()
                    .setBeginTime(Timestamp.newBuilder()
                            .setEpochSeconds(block.beginSeconds()).setNanoseconds(block.beginNanos()))
                    .setEndTime(Timestamp.newBuilder()
                            .setEpochSeconds(block.endSeconds()).setNanoseconds(block.endNanos()))
                    .addAllPvNames(block.pvNames())
                    .build());
        }
        return dataBlocks;
    }

    private static byte[] fileBytes(ExportDataResponse.ExportDataResult exportResult) {
        try {
            return Files.readAllBytes(Path.of(exportResult.getFilePath()));
        } catch (IOException ex) {
            fail("IOException reading export file " + exportResult.getFilePath() + ": " + ex.getMessage());
            return null; // unreachable
        }
    }

    /** Filename stem of the export file, which is the id the export was keyed by. */
    private static String exportFilenameStem(ExportDataResponse.ExportDataResult exportResult) {
        final String filename = Path.of(exportResult.getFilePath()).getFileName().toString();
        return filename.substring(0, filename.lastIndexOf('.'));
    }
}
