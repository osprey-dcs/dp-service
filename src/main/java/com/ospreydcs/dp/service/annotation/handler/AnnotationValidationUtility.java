package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ValidationResult;

import java.util.List;

public class AnnotationValidationUtility {

    public static ValidationResult validateDataSet(DataSet dataSet) {

        // DataSet must include name
        if (dataSet.getName() == null || dataSet.getName().isBlank()) {
            final String errorMsg = "DataSet name must be specified";
            return new ValidationResult(true, errorMsg);
        }
        
        // DataSet must include ownerId
        if (dataSet.getOwnerId() == null || dataSet.getOwnerId().isBlank()) {
            final String errorMsg = "DataSet ownerId must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // DataSet must contain one or more DataBlocks
        final List<DataBlock> requestDataBlocks = dataSet.getDataBlocksList();
        if (requestDataBlocks.isEmpty()) {
            final String errorMsg = "DataSet must include one or more data blocks";
            return new ValidationResult(true, errorMsg);
        }

        // validate each DataBlock
        for (DataBlock dataBlock : requestDataBlocks) {

            // validate beginTime
            final Timestamp blockBeginTime = dataBlock.getBeginTime();
            if (blockBeginTime.getEpochSeconds() < 1) {
                final String errorMsg = "DataSet.DataBlock.beginTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate endTime
            final Timestamp blockEndTime = dataBlock.getEndTime();
            if (blockEndTime.getEpochSeconds() < 1) {
                final String errorMsg = "DataSet.DataBlock.endTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate pvNames list not empty
            final List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames.isEmpty()) {
                final String errorMsg = "DataSet.DataBlock.pvNames must not be empty";
                return new ValidationResult(true, errorMsg);
            }
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateCreateAnnotationRequest(CreateAnnotationRequest request) {

        // owner must be specified
        final String requestOwnerId = request.getOwnerId();
        if (requestOwnerId.isBlank()) {
            final String errorMsg = "CreateAnnotationRequest.ownerId must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // check that list of datasetIds is not empty but don't validate corresponding datasets exist,
        // that will be done by the handler job
        if (request.getDataSetIdsList().isEmpty()) {
            final String errorMsg = "CreateAnnotationRequest.dataSetIds must not be empty";
            return new ValidationResult(true, errorMsg);
        }

        // name must be specified
        final String name = request.getName();
        if (name.isBlank()) {
            final String errorMsg = "CreateAnnotationRequest.name must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // if supplied in request, validate calculations
        if (request.hasCalculations()) {

            // check that list of frames is non-empty
            if ( ! request.getCalculations().getCalculationDataFramesList().isEmpty()) {
                final String errorMsg = "CreateAnnotationRequest.calculations.calculationDataFrames must not be empty";
                return new ValidationResult(true, errorMsg);
            }

            // validate each frame
            for (Calculations.CalculationsDataFrame frame : request.getCalculations().getCalculationDataFramesList()) {

                if (! frame.hasDataTimestamps()) {
                    final String errorMsg =
                            "CalculationDataFrame.dataTimestamps must be specified";
                    return new ValidationResult(true, errorMsg);
                }

                if (! frame.getDataColumnsList().isEmpty()) {
                    final String errorMsg =
                            "CalculationDataFrame.dataColumns must not be empty";
                    return new ValidationResult(true, errorMsg);
                }
            }
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateExportDataSetRequest(ExportDataSetRequest request) {

        final String dataSetId = request.getDataSetId();
        if (dataSetId == null || dataSetId.isBlank()) {
            final String errorMsg = "ExportDataSetRequest.dataSetId must be specified";
            return new ValidationResult(true, errorMsg);
        }

        final ExportDataSetRequest.ExportOutputFormat outputFormat = request.getOutputFormat();
        if (outputFormat == ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED ||
                outputFormat == ExportDataSetRequest.ExportOutputFormat.UNRECOGNIZED) {
            final String errorMsg = "valid ExportDataSetRequest.outputFormat must be specified";
            return new ValidationResult(true, errorMsg);
        }

        return new ValidationResult(false, "");
    }
}
