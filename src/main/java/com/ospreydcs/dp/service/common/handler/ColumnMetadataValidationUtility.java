package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.model.ResultStatus;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Shared validation of column-level ColumnMetadata against the platform limits (provenance field
 * lengths, tag/attribute counts and lengths), parameterized on the request field path so the
 * rejection message names the caller's field. Consumed by IngestionValidationUtility
 * (ingestionDataFrame) and by the annotation save-calculations validation (#248 Phase 4) — the
 * limits are one contract, so they live in one place.
 *
 * derivedFrom provenance links get length checks only: links are stored as supplied and may
 * dangle (no existence checks, no ObjectId parse of calculationsId — a link may point at records
 * not yet created).
 */
public class ColumnMetadataValidationUtility {

    // Shared platform limits for column metadata content.
    public static final int MAX_STRING_LENGTH = 256;
    public static final int MAX_METADATA_TAGS_COUNT = 20;
    public static final int MAX_METADATA_ATTRIBUTES_COUNT = 20;

    public static ResultStatus validateColumnMetadata(ColumnMetadata metadata, String fieldPath) {
        if (metadata.hasProvenance()) {
            ColumnProvenance provenance = metadata.getProvenance();
            if (provenance.getSource().length() > MAX_STRING_LENGTH) {
                return new ResultStatus(true, fieldPath + ".metadata.provenance.source length exceeds maximum: " +
                        "got: " + provenance.getSource().length() + ", max: " + MAX_STRING_LENGTH);
            }
            if (provenance.getProcess().length() > MAX_STRING_LENGTH) {
                return new ResultStatus(true, fieldPath + ".metadata.provenance.process length exceeds maximum: " +
                        "got: " + provenance.getProcess().length() + ", max: " + MAX_STRING_LENGTH);
            }

            List<ColumnProvenance.ColumnSource> derivedFrom = provenance.getDerivedFromList();
            for (int i = 0; i < derivedFrom.size(); i++) {
                ColumnProvenance.ColumnSource source = derivedFrom.get(i);
                final String sourcePath = fieldPath + ".metadata.provenance.derivedFrom[" + i + "]";
                switch (source.getOriginCase()) {
                    case PVNAME -> {
                        if (source.getPvName().length() > MAX_STRING_LENGTH) {
                            return new ResultStatus(true, sourcePath + ".pvName length exceeds maximum: " +
                                    "got: " + source.getPvName().length() + ", max: " + MAX_STRING_LENGTH);
                        }
                    }
                    case CALCULATIONSCOLUMN -> {
                        ColumnProvenance.CalculationsColumn calculationsColumn = source.getCalculationsColumn();
                        if (calculationsColumn.getCalculationsId().length() > MAX_STRING_LENGTH) {
                            return new ResultStatus(true, sourcePath + ".calculationsColumn.calculationsId length exceeds maximum: " +
                                    "got: " + calculationsColumn.getCalculationsId().length() + ", max: " + MAX_STRING_LENGTH);
                        }
                        if (calculationsColumn.getFrameName().length() > MAX_STRING_LENGTH) {
                            return new ResultStatus(true, sourcePath + ".calculationsColumn.frameName length exceeds maximum: " +
                                    "got: " + calculationsColumn.getFrameName().length() + ", max: " + MAX_STRING_LENGTH);
                        }
                        if (calculationsColumn.getColumnName().length() > MAX_STRING_LENGTH) {
                            return new ResultStatus(true, sourcePath + ".calculationsColumn.columnName length exceeds maximum: " +
                                    "got: " + calculationsColumn.getColumnName().length() + ", max: " + MAX_STRING_LENGTH);
                        }
                    }
                    case ORIGIN_NOT_SET -> {
                        // an unset origin carries nothing to bound; stored as supplied
                    }
                }
            }
        }

        List<String> tags = metadata.getTagsList();
        if (tags.size() > MAX_METADATA_TAGS_COUNT) {
            return new ResultStatus(true, fieldPath + ".metadata.tags count exceeds maximum: " +
                    "got: " + tags.size() + ", max: " + MAX_METADATA_TAGS_COUNT);
        }
        for (int i = 0; i < tags.size(); i++) {
            if (tags.get(i).length() > MAX_STRING_LENGTH) {
                return new ResultStatus(true, fieldPath + ".metadata.tags[" + i + "] length exceeds maximum: " +
                        "got: " + tags.get(i).length() + ", max: " + MAX_STRING_LENGTH);
            }
        }

        List<Attribute> attributes = metadata.getAttributesList();
        if (attributes.size() > MAX_METADATA_ATTRIBUTES_COUNT) {
            return new ResultStatus(true, fieldPath + ".metadata.attributes count exceeds maximum: " +
                    "got: " + attributes.size() + ", max: " + MAX_METADATA_ATTRIBUTES_COUNT);
        }
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.getName().length() > MAX_STRING_LENGTH) {
                return new ResultStatus(true, fieldPath + ".metadata.attributes[" + i + "].name length exceeds maximum: " +
                        "got: " + attr.getName().length() + ", max: " + MAX_STRING_LENGTH);
            }
            if (attr.getValue().length() > MAX_STRING_LENGTH) {
                return new ResultStatus(true, fieldPath + ".metadata.attributes[" + i + "].value length exceeds maximum: " +
                        "got: " + attr.getValue().length() + ", max: " + MAX_STRING_LENGTH);
            }
        }

        return new ResultStatus(false, "");
    }

    /**
     * Validates the metadata of every column of every type in the frame. The fieldPathPrefix names
     * the frame's position in the caller's request (e.g. "ingestionDataFrame"); rejection messages
     * extend it with the column list and index.
     */
    public static ResultStatus validateAllColumnMetadata(DataFrame frame, String fieldPathPrefix) {
        ResultStatus s;
        s = validateColumnListMetadata(frame.getDataColumnsList(),
                DataColumn::hasMetadata, DataColumn::getMetadata, fieldPathPrefix + ".dataColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getSerializedDataColumnsList(),
                SerializedDataColumn::hasMetadata, SerializedDataColumn::getMetadata, fieldPathPrefix + ".serializedDataColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getDoubleColumnsList(),
                DoubleColumn::hasMetadata, DoubleColumn::getMetadata, fieldPathPrefix + ".doubleColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getFloatColumnsList(),
                FloatColumn::hasMetadata, FloatColumn::getMetadata, fieldPathPrefix + ".floatColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getInt64ColumnsList(),
                Int64Column::hasMetadata, Int64Column::getMetadata, fieldPathPrefix + ".int64Columns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getInt32ColumnsList(),
                Int32Column::hasMetadata, Int32Column::getMetadata, fieldPathPrefix + ".int32Columns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getBoolColumnsList(),
                BoolColumn::hasMetadata, BoolColumn::getMetadata, fieldPathPrefix + ".boolColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getStringColumnsList(),
                StringColumn::hasMetadata, StringColumn::getMetadata, fieldPathPrefix + ".stringColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getEnumColumnsList(),
                EnumColumn::hasMetadata, EnumColumn::getMetadata, fieldPathPrefix + ".enumColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getImageColumnsList(),
                ImageColumn::hasMetadata, ImageColumn::getMetadata, fieldPathPrefix + ".imageColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getStructColumnsList(),
                StructColumn::hasMetadata, StructColumn::getMetadata, fieldPathPrefix + ".structColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getDoubleArrayColumnsList(),
                DoubleArrayColumn::hasMetadata, DoubleArrayColumn::getMetadata, fieldPathPrefix + ".doubleArrayColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getFloatArrayColumnsList(),
                FloatArrayColumn::hasMetadata, FloatArrayColumn::getMetadata, fieldPathPrefix + ".floatArrayColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getInt32ArrayColumnsList(),
                Int32ArrayColumn::hasMetadata, Int32ArrayColumn::getMetadata, fieldPathPrefix + ".int32ArrayColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getInt64ArrayColumnsList(),
                Int64ArrayColumn::hasMetadata, Int64ArrayColumn::getMetadata, fieldPathPrefix + ".int64ArrayColumns");
        if (s.isError) return s;
        s = validateColumnListMetadata(frame.getBoolArrayColumnsList(),
                BoolArrayColumn::hasMetadata, BoolArrayColumn::getMetadata, fieldPathPrefix + ".boolArrayColumns");
        if (s.isError) return s;
        return new ResultStatus(false, "");
    }

    private static <T> ResultStatus validateColumnListMetadata(
            List<T> columns,
            Predicate<T> hasMetadata,
            Function<T, ColumnMetadata> metadataGetter,
            String listPath
    ) {
        for (int i = 0; i < columns.size(); i++) {
            T column = columns.get(i);
            if (hasMetadata.test(column)) {
                ResultStatus s = validateColumnMetadata(metadataGetter.apply(column), listPath + "[" + i + "]");
                if (s.isError) return s;
            }
        }
        return new ResultStatus(false, "");
    }
}
