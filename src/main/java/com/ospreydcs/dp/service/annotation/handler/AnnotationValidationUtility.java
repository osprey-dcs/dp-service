package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.BoolArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.BoolColumn;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.EnumColumn;
import com.ospreydcs.dp.grpc.v1.common.FloatArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.FloatColumn;
import com.ospreydcs.dp.grpc.v1.common.ImageColumn;
import com.ospreydcs.dp.grpc.v1.common.Int32ArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.Int32Column;
import com.ospreydcs.dp.grpc.v1.common.Int64ArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.Int64Column;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.StringColumn;
import com.ospreydcs.dp.grpc.v1.common.StructColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.handler.ColumnMetadataValidationUtility;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.bson.types.ObjectId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public class AnnotationValidationUtility {

    /**
     * Validates a required ObjectId-typed request field: present (non-blank) and parseable as an
     * ObjectId. A malformed id is a client mistake, rejected with a precise message (#248 plan
     * D11); unvalidated, it would throw IllegalArgumentException from the ObjectId constructor
     * inside the worker thread, where QueueHandlerBase swallows it and the caller's stream hangs
     * with no response. Every get/delete/patch job over an id-keyed entity must call this before
     * touching the mongo client.
     *
     * @param fieldName the request-qualified field name for messages, e.g. "GetDataSetRequest.dataSetId"
     */
    public static ResultStatus validateRequiredObjectId(String fieldName, String value) {
        if (value.isBlank()) {
            return new ResultStatus(true, fieldName + " must be specified");
        }
        if (!ObjectId.isValid(value)) {
            return new ResultStatus(true, fieldName + " is not a valid id: " + value);
        }
        return new ResultStatus(false, "");
    }

    public static ResultStatus validateSaveDataSetRequest(SaveDataSetRequest request) {

        // request must include name
        if (request.getName().isBlank()) {
            final String errorMsg = "SaveDataSetRequest.name must be specified";
            return new ResultStatus(true, errorMsg);
        }

        // request must include ownerId
        if (request.getOwnerId().isBlank()) {
            final String errorMsg = "SaveDataSetRequest.ownerId must be specified";
            return new ResultStatus(true, errorMsg);
        }

        // request must contain one or more DataBlocks
        final List<DataBlock> requestDataBlocks = request.getDataBlocksList();
        if (requestDataBlocks.isEmpty()) {
            final String errorMsg = "SaveDataSetRequest must include one or more data blocks";
            return new ResultStatus(true, errorMsg);
        }

        // validate each DataBlock
        for (DataBlock dataBlock : requestDataBlocks) {
            final ResultStatus blockStatus = validateDataBlock("SaveDataSetRequest.DataBlock", dataBlock);
            if (blockStatus.isError) {
                return blockStatus;
            }
        }

        // validation successful
        return new ResultStatus(false, "");
    }

    /**
     * Validates one DataBlock: non-zero begin/end times and a non-empty pvNames list. Shared by
     * validateSaveDataSetRequest and the exportData inline-dataBlocks path (#248 plan D31); the
     * fieldPath names the block's position in the caller's request.
     */
    public static ResultStatus validateDataBlock(String fieldPath, DataBlock dataBlock) {

        // validate beginTime
        final Timestamp blockBeginTime = dataBlock.getBeginTime();
        if (blockBeginTime.getEpochSeconds() < 1) {
            final String errorMsg = fieldPath + ".beginTime must be non-zero";
            return new ResultStatus(true, errorMsg);
        }

        // validate endTime
        final Timestamp blockEndTime = dataBlock.getEndTime();
        if (blockEndTime.getEpochSeconds() < 1) {
            final String errorMsg = fieldPath + ".endTime must be non-zero";
            return new ResultStatus(true, errorMsg);
        }

        // validate pvNames list not empty
        final List<String> blockPvNames = dataBlock.getPvNamesList();
        if (blockPvNames.isEmpty()) {
            final String errorMsg = fieldPath + ".pvNames must not be empty";
            return new ResultStatus(true, errorMsg);
        }

        return new ResultStatus(false, "");
    }

    public static ResultStatus validateSaveAnnotationRequest(SaveAnnotationRequest request) {

        // owner must be specified
        final String requestOwnerId = request.getOwnerId();
        if (requestOwnerId.isBlank()) {
            final String errorMsg = "SaveAnnotationRequest.ownerId must be specified";
            return new ResultStatus(true, errorMsg);
        }

        // check that list of datasetIds is not empty but don't validate corresponding datasets exist,
        // that will be done by the handler job
        if (request.getDataSetIdsList().isEmpty()) {
            final String errorMsg = "SaveAnnotationRequest.dataSetIds must not be empty";
            return new ResultStatus(true, errorMsg);
        }

        // name must be specified
        final String name = request.getName();
        if (name.isBlank()) {
            final String errorMsg = "SaveAnnotationRequest.name must be specified";
            return new ResultStatus(true, errorMsg);
        }

        // if supplied in request, validate calculations content (#248 plan D28)
        if (request.hasCalculations()) {
            final ResultStatus calculationsStatus = validateCalculations(request.getCalculations());
            if (calculationsStatus.isError) {
                return calculationsStatus;
            }
        }

        // validation successful
        return new ResultStatus(false, "");
    }


    /**
     * Validates a Calculations payload's full shape (#248 plan D28): non-empty frame list, unique
     * frame names, and per frame — well-formed timestamps, at least one column of any type,
     * column names unique across all column types, per-column name/values/count checks, and the
     * shared column-metadata limits. Frame and column names are addressing keys (they key
     * CalculationsSpec.dataFrameColumns and provenance derivedFrom links), which is why
     * duplicates are unaddressable and rejected.
     */
    private static ResultStatus validateCalculations(Calculations calculations) {

        // check that list of frames is non-empty
        final List<Calculations.CalculationsDataFrame> frames = calculations.getCalculationDataFramesList();
        if (frames.isEmpty()) {
            final String errorMsg = "SaveAnnotationRequest.calculations.calculationDataFrames must not be empty";
            return new ResultStatus(true, errorMsg);
        }

        final Set<String> frameNames = new HashSet<>();
        for (int frameIndex = 0; frameIndex < frames.size(); frameIndex++) {
            final Calculations.CalculationsDataFrame frame = frames.get(frameIndex);

            // name field is required
            if (frame.getName().isBlank()) {
                final String errorMsg =
                        "CalculationDataFrame.name must be specified";
                return new ResultStatus(true, errorMsg);
            }

            // frame names must be unique across the Calculations object
            if ( ! frameNames.add(frame.getName())) {
                final String errorMsg =
                        "SaveAnnotationRequest.calculations.calculationDataFrames contains duplicate frame name: "
                                + frame.getName();
                return new ResultStatus(true, errorMsg);
            }

            final ResultStatus frameStatus = validateCalculationsDataFrame(frame, frameIndex);
            if (frameStatus.isError) {
                return frameStatus;
            }
        }

        // validation successful
        return new ResultStatus(false, "");
    }

    private static ResultStatus validateCalculationsDataFrame(
            Calculations.CalculationsDataFrame frame, int frameIndex
    ) {
        // check that request includes DataTimestamps
        if (! frame.getFrame().hasDataTimestamps()) {
            final String errorMsg =
                    "CalculationDataFrame.dataTimestamps must be specified";
            return new ResultStatus(true, errorMsg);
        }

        // check that DataTimestamps include either SamplingClock or TimestampList
        if ((!frame.getFrame().getDataTimestamps().hasSamplingClock())
                && (!frame.getFrame().getDataTimestamps().hasTimestampList())
        ) {
            final String errorMsg =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            return new ResultStatus(true, errorMsg);
        }

        // check that SamplingClock is valid, if specified
        if (frame.getFrame().getDataTimestamps().hasSamplingClock()) {
            final SamplingClock samplingClock = frame.getFrame().getDataTimestamps().getSamplingClock();
            if ((!samplingClock.hasStartTime())
                    || (samplingClock.getStartTime().getEpochSeconds() == 0)
                    || (samplingClock.getPeriodNanos() == 0)
                    || (samplingClock.getCount() == 0)
            ) {
                final String errorMsg =
                        "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
                return new ResultStatus(true, errorMsg);
            }
        }

        // check that TimestampList is valid, if specified
        if (frame.getFrame().getDataTimestamps().hasTimestampList()) {
            // check that TimestampList is not empty
            if (frame.getFrame().getDataTimestamps().getTimestampList().getTimestampsList().isEmpty()) {
                final String errorMsg =
                        "CalculationDataFrame.dataTimestamps.timestampList must not be empty";
                return new ResultStatus(true, errorMsg);
            }
        }

        final DataFrame dataFrame = frame.getFrame();

        // The frame's timestamp count is the row axis every column must match. A shorter column
        // otherwise fails during tabular export assembly with an unchecked
        // IndexOutOfBoundsException that QueueHandlerBase swallows, hanging the caller's stream
        // (#248 plan finding 5) — so the count check is mandatory for every column type, legacy
        // included. Guaranteed >= 1 by the timestamp checks above.
        final int sampleCount = new DataTimestampsUtility.DataTimestampsModel(
                dataFrame.getDataTimestamps()).getSampleCount();

        // at least one column of any type — replaces the legacy-list-only emptiness check, which
        // wrongly rejected a frame carrying only typed columns
        final int totalColumnCount = dataFrame.getDataColumnsCount()
                + dataFrame.getSerializedDataColumnsCount()
                + dataFrame.getDoubleColumnsCount()
                + dataFrame.getFloatColumnsCount()
                + dataFrame.getInt64ColumnsCount()
                + dataFrame.getInt32ColumnsCount()
                + dataFrame.getBoolColumnsCount()
                + dataFrame.getStringColumnsCount()
                + dataFrame.getEnumColumnsCount()
                + dataFrame.getImageColumnsCount()
                + dataFrame.getStructColumnsCount()
                + dataFrame.getDoubleArrayColumnsCount()
                + dataFrame.getFloatArrayColumnsCount()
                + dataFrame.getInt32ArrayColumnsCount()
                + dataFrame.getInt64ArrayColumnsCount()
                + dataFrame.getBoolArrayColumnsCount();
        if (totalColumnCount == 0) {
            final String errorMsg =
                    "CalculationDataFrame must include at least one column of any type: " + frame.getName();
            return new ResultStatus(true, errorMsg);
        }

        // column names must be unique across all column types in the frame, mirroring
        // ingestion's unique-PV-names cross-check
        final Set<String> columnNames = new HashSet<>();

        // check that each legacy DataColumn is valid
        for (DataColumn dataColumn : dataFrame.getDataColumnsList()) {

            // check that DataColumn name is specified
            if (dataColumn.getName().isBlank()) {
                final String errorMsg =
                        "CalculationDataFrame.dataColumns name must be specified for each DataColumn";
                return new ResultStatus(true, errorMsg);
            }

            final ResultStatus duplicateStatus =
                    registerColumnName(columnNames, dataColumn.getName(), frame.getName());
            if (duplicateStatus.isError) {
                return duplicateStatus;
            }

            // check that DataColumn is not empty
            if (dataColumn.getDataValuesList().isEmpty()) {
                final String errorMsg =
                        "CalculationDataFrame.dataColumns contains a DataColumn with no values: "
                                + dataColumn.getName();
                return new ResultStatus(true, errorMsg);
            }

            // A DataValue entry with no value arm set still occupies its position, so sparse
            // legacy columns remain expressible — the check is on entry count, not set arms.
            if (dataColumn.getDataValuesCount() != sampleCount) {
                final String errorMsg =
                        "CalculationDataFrame.dataColumns values count mismatch: expected " + sampleCount
                                + ", got: " + dataColumn.getDataValuesCount()
                                + " for column: " + dataColumn.getName();
                return new ResultStatus(true, errorMsg);
            }
        }

        // serialized columns carry no countable values; name checks only
        for (SerializedDataColumn column : dataFrame.getSerializedDataColumnsList()) {
            if (column.getName().isBlank()) {
                final String errorMsg =
                        "CalculationDataFrame.serializedDataColumns name must be specified for each column";
                return new ResultStatus(true, errorMsg);
            }
            final ResultStatus duplicateStatus =
                    registerColumnName(columnNames, column.getName(), frame.getName());
            if (duplicateStatus.isError) {
                return duplicateStatus;
            }
        }

        ResultStatus s;
        s = validateCountedColumns(dataFrame.getDoubleColumnsList(),
                DoubleColumn::getName, DoubleColumn::getValuesCount,
                "CalculationDataFrame.doubleColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getFloatColumnsList(),
                FloatColumn::getName, FloatColumn::getValuesCount,
                "CalculationDataFrame.floatColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getInt64ColumnsList(),
                Int64Column::getName, Int64Column::getValuesCount,
                "CalculationDataFrame.int64Columns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getInt32ColumnsList(),
                Int32Column::getName, Int32Column::getValuesCount,
                "CalculationDataFrame.int32Columns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getBoolColumnsList(),
                BoolColumn::getName, BoolColumn::getValuesCount,
                "CalculationDataFrame.boolColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getStringColumnsList(),
                StringColumn::getName, StringColumn::getValuesCount,
                "CalculationDataFrame.stringColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getEnumColumnsList(),
                EnumColumn::getName, EnumColumn::getValuesCount,
                "CalculationDataFrame.enumColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getImageColumnsList(),
                ImageColumn::getName, ImageColumn::getImagesCount,
                "CalculationDataFrame.imageColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateCountedColumns(dataFrame.getStructColumnsList(),
                StructColumn::getName, StructColumn::getValuesCount,
                "CalculationDataFrame.structColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateArrayColumns(dataFrame.getDoubleArrayColumnsList(),
                DoubleArrayColumn::getName, DoubleArrayColumn::getDimensions, DoubleArrayColumn::getValuesCount,
                "CalculationDataFrame.doubleArrayColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateArrayColumns(dataFrame.getFloatArrayColumnsList(),
                FloatArrayColumn::getName, FloatArrayColumn::getDimensions, FloatArrayColumn::getValuesCount,
                "CalculationDataFrame.floatArrayColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateArrayColumns(dataFrame.getInt32ArrayColumnsList(),
                Int32ArrayColumn::getName, Int32ArrayColumn::getDimensions, Int32ArrayColumn::getValuesCount,
                "CalculationDataFrame.int32ArrayColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateArrayColumns(dataFrame.getInt64ArrayColumnsList(),
                Int64ArrayColumn::getName, Int64ArrayColumn::getDimensions, Int64ArrayColumn::getValuesCount,
                "CalculationDataFrame.int64ArrayColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;
        s = validateArrayColumns(dataFrame.getBoolArrayColumnsList(),
                BoolArrayColumn::getName, BoolArrayColumn::getDimensions, BoolArrayColumn::getValuesCount,
                "CalculationDataFrame.boolArrayColumns", sampleCount, columnNames, frame.getName());
        if (s.isError) return s;

        // column metadata limits are the shared platform contract (ColumnMetadataValidationUtility)
        return ColumnMetadataValidationUtility.validateAllColumnMetadata(
                dataFrame, "SaveAnnotationRequest.calculations.calculationDataFrames[" + frameIndex + "]");
    }

    private static ResultStatus registerColumnName(
            Set<String> frameColumnNames, String columnName, String frameName
    ) {
        if ( ! frameColumnNames.add(columnName)) {
            final String errorMsg = "CalculationDataFrame contains duplicate column name: " + columnName
                    + " in frame: " + frameName;
            return new ResultStatus(true, errorMsg);
        }
        return new ResultStatus(false, "");
    }

    private static <T> ResultStatus validateCountedColumns(
            List<T> columns,
            Function<T, String> nameGetter,
            ToIntFunction<T> valueCounter,
            String listPath,
            int sampleCount,
            Set<String> frameColumnNames,
            String frameName
    ) {
        for (T column : columns) {
            final String name = nameGetter.apply(column);
            if (name.isBlank()) {
                return new ResultStatus(true, listPath + " name must be specified for each column");
            }
            final ResultStatus duplicateStatus = registerColumnName(frameColumnNames, name, frameName);
            if (duplicateStatus.isError) {
                return duplicateStatus;
            }
            final int valuesCount = valueCounter.applyAsInt(column);
            if (valuesCount == 0) {
                return new ResultStatus(true, listPath + " contains a column with no values: " + name);
            }
            if (valuesCount != sampleCount) {
                return new ResultStatus(true, listPath + " values count mismatch: expected " + sampleCount
                        + ", got: " + valuesCount + " for column: " + name);
            }
        }
        return new ResultStatus(false, "");
    }

    private static <T> ResultStatus validateArrayColumns(
            List<T> columns,
            Function<T, String> nameGetter,
            Function<T, ArrayDimensions> dimensionsGetter,
            ToIntFunction<T> valueCounter,
            String listPath,
            int sampleCount,
            Set<String> frameColumnNames,
            String frameName
    ) {
        for (T column : columns) {
            final String name = nameGetter.apply(column);
            if (name.isBlank()) {
                return new ResultStatus(true, listPath + " name must be specified for each column");
            }
            final ResultStatus duplicateStatus = registerColumnName(frameColumnNames, name, frameName);
            if (duplicateStatus.isError) {
                return duplicateStatus;
            }
            final int valuesCount = valueCounter.applyAsInt(column);
            if (valuesCount == 0) {
                return new ResultStatus(true, listPath + " contains a column with no values: " + name);
            }

            // array values are flattened, so the expected count is per-sample element count times
            // the frame's timestamp count — the dims checks make that product well-defined
            final List<Integer> dims = dimensionsGetter.apply(column).getDimsList();
            if (dims.isEmpty() || dims.size() > 3) {
                return new ResultStatus(true, listPath + " dimensions.dims.size must be in {1, 2, 3}, got: "
                        + dims.size() + " for column: " + name);
            }
            long elementCount = 1;
            final long expectedValuesCount;
            try {
                for (int j = 0; j < dims.size(); j++) {
                    final int dim = dims.get(j);
                    if (dim <= 0) {
                        return new ResultStatus(true, listPath + " dimensions.dims[" + j + "] must be > 0, got: "
                                + dim + " for column: " + name);
                    }
                    elementCount = Math.multiplyExact(elementCount, dim);
                }
                expectedValuesCount = Math.multiplyExact((long) sampleCount, elementCount);
            } catch (ArithmeticException ex) {
                return new ResultStatus(true, listPath + " dimensions element count overflows for column: " + name);
            }
            if (valuesCount != expectedValuesCount) {
                return new ResultStatus(true, listPath + " values count mismatch: expected " + expectedValuesCount
                        + " (sampleCount=" + sampleCount + " * elementCount=" + elementCount + "), got: "
                        + valuesCount + " for column: " + name);
            }
        }
        return new ResultStatus(false, "");
    }

    public static ResultStatus validateExportDataRequest(ExportDataRequest request) {

        // at least one data source is required: a saved dataset, inline dataBlocks, or
        // calculations (#248 plan D31)
        final String dataSetId = request.getDataSetId();
        if (dataSetId.isBlank()
                && request.getDataBlocksList().isEmpty()
                && ( ! request.hasCalculationsSpec())) {
            final String errorMsg =
                    "ExportDataRequest must specify at least one of dataSetId, dataBlocks, or calculationsSpec";
            return new ResultStatus(true, errorMsg);
        }

        // a non-blank dataSetId must be a well-formed ObjectId, so a malformed id reads as
        // malformed rather than "not found" (#248 plan D30)
        if ( ! dataSetId.isBlank()) {
            final ResultStatus dataSetIdStatus =
                    validateRequiredObjectId("ExportDataRequest.dataSetId", dataSetId);
            if (dataSetIdStatus.isError) {
                return dataSetIdStatus;
            }
        }

        // validate each inline DataBlock, same rules as saveDataSet blocks (#248 plan D31)
        for (DataBlock dataBlock : request.getDataBlocksList()) {
            final ResultStatus blockStatus = validateDataBlock("ExportDataRequest.dataBlocks", dataBlock);
            if (blockStatus.isError) {
                return blockStatus;
            }
        }

        // calculationsSpec is optional, but validate content if specified
        if (request.hasCalculationsSpec()) {

            final CalculationsSpec calculationsSpec = request.getCalculationsSpec();
            final ResultStatus calculationsIdStatus = validateRequiredObjectId(
                    "ExportDataRequest.calculationsSpec.calculationsId", calculationsSpec.getCalculationsId());
            if (calculationsIdStatus.isError) {
                return calculationsIdStatus;
            }

            for (var mapEntries : calculationsSpec.getDataFrameColumnsMap().entrySet()) {
                final String frameName = mapEntries.getKey();
                final CalculationsSpec.ColumnNameList frameColumnNameList = mapEntries.getValue();
                if (frameColumnNameList.getColumnNamesList().isEmpty()) {
                    final String errorMsg =
                            "ExportDataRequest.calculationsSpec.dataFrameColumns list must not be empty";
                    return new ResultStatus(true, errorMsg);
                }
                // list can be empty, but check contents if not
                for (String frameColumnName : frameColumnNameList.getColumnNamesList()) {
                    if (frameColumnName.isBlank()) {
                        final String errorMsg =
                                "ExportDataRequest.calculationsSpec.dataFrameColumns includes blank column name";
                        return new ResultStatus(true, errorMsg);
                    }
                }
            }
        }

        final ExportDataRequest.ExportOutputFormat outputFormat = request.getOutputFormat();
        if (outputFormat == ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED ||
                outputFormat == ExportDataRequest.ExportOutputFormat.UNRECOGNIZED) {
            final String errorMsg = "valid ExportDataRequest.outputFormat must be specified";
            return new ResultStatus(true, errorMsg);
        }

        return new ResultStatus(false, "");
    }
}
