package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
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
            if (request.getCalculations().getCalculationDataFramesList().isEmpty()) {
                final String errorMsg = "CreateAnnotationRequest.calculations.calculationDataFrames must not be empty";
                return new ValidationResult(true, errorMsg);
            }

            // validate each frame
            for (Calculations.CalculationsDataFrame frame : request.getCalculations().getCalculationDataFramesList()) {

                // name field is required
                if (frame.getName().isBlank()) {
                    final String errorMsg =
                            "CalculationDataFrame.name must be specified";
                    return new ValidationResult(true, errorMsg);
                }

                // check that request includes DataTimestamps
                if (! frame.hasDataTimestamps()) {
                    final String errorMsg =
                            "CalculationDataFrame.dataTimestamps must be specified";
                    return new ValidationResult(true, errorMsg);
                }

                // check that DataTimestamps include either SamplingClock or TimestampList
                if ((!frame.getDataTimestamps().hasSamplingClock())
                        && (!frame.getDataTimestamps().hasTimestampList())
                ) {
                    final String errorMsg =
                            "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
                    return new ValidationResult(true, errorMsg);
                }

                // check that SamplingClock is valid, if specified
                if (frame.getDataTimestamps().hasSamplingClock()) {
                    final SamplingClock samplingClock = frame.getDataTimestamps().getSamplingClock();
                    if ((!samplingClock.hasStartTime())
                            || (samplingClock.getStartTime().getEpochSeconds() == 0)
                            || (samplingClock.getPeriodNanos() == 0)
                            || (samplingClock.getCount() == 0)
                    ) {
                        final String errorMsg =
                                "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
                        return new ValidationResult(true, errorMsg);
                    }
                }

                // check that TimestampList is valid, if specified
                if (frame.getDataTimestamps().hasTimestampList()) {
                    // check that TimestampList is not empty
                    if (frame.getDataTimestamps().getTimestampList().getTimestampsList().isEmpty()) {
                        final String errorMsg =
                                "CalculationDataFrame.dataTimestamps.timestampList must not be empty";
                        return new ValidationResult(true, errorMsg);
                    }
                }

                // check that list of DataColumns is not empty
                if (frame.getDataColumnsList().isEmpty()) {
                    final String errorMsg =
                            "CalculationDataFrame.dataColumns must not be empty";
                    return new ValidationResult(true, errorMsg);
                }

                // check that each DataColumn is valid
                for (DataColumn dataColumn : frame.getDataColumnsList()) {

                    // check that DataColumn name is specified
                    if (dataColumn.getName().isBlank()) {
                        final String errorMsg =
                                "CalculationDataFrame.dataColumns name must be specified for each DataColumn";
                        return new ValidationResult(true, errorMsg);
                    }

                    // check that DataColumn is not empty
                    if (dataColumn.getDataValuesList().isEmpty()) {
                        final String errorMsg =
                                "CalculationDataFrame.dataColumns contains a DataColumn with no values: "
                                        + dataColumn.getName();
                        return new ValidationResult(true, errorMsg);
                    }
                }
            }
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateExportDataRequest(ExportDataRequest request) {

        // either dataSetId or calculationsSpec is required
        final String dataSetId = request.getDataSetId();
        if ((dataSetId == null || dataSetId.isBlank()) && ( ! request.hasCalculationsSpec())) {
            final String errorMsg = "ExportDataRequest either dataSetId or calculationsSpec must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // calculationsSpec is optional, but validate content if specified
        if (request.hasCalculationsSpec()) {

            final CalculationsSpec calculationsSpec = request.getCalculationsSpec();
            if (calculationsSpec.getCalculationsId().isBlank()) {
                final String errorMsg = "ExportDataRequest.calculationsSpec.calculationsId must be specified";
                return new ValidationResult(true, errorMsg);
            }

            for (var mapEntries : calculationsSpec.getDataFrameColumnsMap().entrySet()) {
                final String frameName = mapEntries.getKey();
                final CalculationsSpec.ColumnNameList frameColumnNameList = mapEntries.getValue();
                if (frameColumnNameList.getColumnNamesList().isEmpty()) {
                    final String errorMsg =
                            "ExportDataRequest.calculationsSpec.dataFrameColumns list must not be empty";
                    return new ValidationResult(true, errorMsg);
                }
                // list can be empty, but check contents if not
                for (String frameColumnName : frameColumnNameList.getColumnNamesList()) {
                    if (frameColumnName.isBlank()) {
                        final String errorMsg =
                                "ExportDataRequest.calculationsSpec.dataFrameColumns includes blank column name";
                        return new ValidationResult(true, errorMsg);
                    }
                }
            }
        }

        final ExportDataRequest.ExportOutputFormat outputFormat = request.getOutputFormat();
        if (outputFormat == ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED ||
                outputFormat == ExportDataRequest.ExportOutputFormat.UNRECOGNIZED) {
            final String errorMsg = "valid ExportDataRequest.outputFormat must be specified";
            return new ValidationResult(true, errorMsg);
        }

        return new ValidationResult(false, "");
    }
}
