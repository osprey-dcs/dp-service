package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class ExportDataSetTest extends AnnotationIntegrationTestIntermediate {

    @BeforeClass
    public static void setUp() throws Exception {
        AnnotationIntegrationTestIntermediate.setUp();
    }

    @AfterClass
    public static void tearDown() {
        AnnotationIntegrationTestIntermediate.tearDown();
    }

    @Test
    public void testExportDataSet() {

        // ingest some data
        final Map<String, IngestionStreamInfo> validationMap =
                AnnotationIntegrationTestIntermediate.annotationIngestionScenario();

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult =
                AnnotationIntegrationTestIntermediate.createDataSetScenario();

        {
            // export to hdf5, negative test, unspecified dataset id
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            "",
                            null,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "ExportDataSetRequest.dataSetId must be specified");
        }

        {
            // export to hdf5, negative test, invalid dataset id
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            "1234abcd1234abcd1234abcd",
                            null,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "DatasetDocument with id 1234abcd1234abcd1234abcd not found");
        }

        {
            // export to hdf5, negative test, unspecified output format
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            createDataSetScenarioResult.firstHalfDataSetId,
                            null,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0,
                            null,
                            true,
                            "valid ExportDataSetRequest.outputFormat must be specified");
        }

        {
            // export to hdf5, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            createDataSetScenarioResult.firstHalfDataSetId,
                            createDataSetScenarioResult.firstHalfDataSetParams,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            0, // expectedNumRows ignored for bucketed export
                            validationMap,
                            false,
                            "");
        }

        {
            // export to csv, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            createDataSetScenarioResult.firstHalfDataSetId,
                            createDataSetScenarioResult.firstHalfDataSetParams,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            25, // 2.5 seconds of data with 10 values per second
                            validationMap,
                            false,
                            "");
        }

        {
            // export to xlsx, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            createDataSetScenarioResult.firstHalfDataSetId,
                            createDataSetScenarioResult.firstHalfDataSetParams,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            25, // 2.5 seconds of data with 10 values per second
                            validationMap,
                            false,
                            "");
        }

    }
}
