package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.*;

import java.time.Instant;
import java.util.Map;

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
                            "ExportDataRequest either dataSetId or calculationsSpec must be specified");
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

    }
}
