package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared dispatch from a common.DataFrame's typed column lists to the corresponding MongoDB
 * column document classes.
 *
 * Both storage paths that persist DataFrame columns consume this helper: ingestion buckets
 * (BucketDocument.generateBucketsFromRequest()) and annotation calculations
 * (CalculationsDataFrameDocument.fromCalculationsDataFrame()). Keep the dispatch here — a second
 * copy lets a new column type land on one path and silently drop on the other.
 */
public class ColumnDocumentUtility {

    /**
     * Creates a column document for each column of the supplied DataFrame, covering all 16 column
     * types (legacy DataColumn, SerializedDataColumn, and the typed scalar/array/binary families).
     * Adding a new protobuf column type requires a branch here; see "Systematic Process for Adding
     * New Protobuf Column Types" in CLAUDE.md.
     */
    public static List<ColumnDocumentBase> fromDataFrame(DataFrame dataFrame) throws DpException {

        final List<ColumnDocumentBase> columnDocuments = new ArrayList<>();

        for (DataColumn column : dataFrame.getDataColumnsList()) {
            columnDocuments.add(DataColumnDocument.fromDataColumn(column));
        }

        for (SerializedDataColumn column : dataFrame.getSerializedDataColumnsList()) {
            columnDocuments.add(SerializedDataColumnDocument.fromSerializedDataColumn(column));
        }

        for (DoubleColumn column : dataFrame.getDoubleColumnsList()) {
            columnDocuments.add(DoubleColumnDocument.fromDoubleColumn(column));
        }

        for (FloatColumn column : dataFrame.getFloatColumnsList()) {
            columnDocuments.add(FloatColumnDocument.fromFloatColumn(column));
        }

        for (Int64Column column : dataFrame.getInt64ColumnsList()) {
            columnDocuments.add(Int64ColumnDocument.fromInt64Column(column));
        }

        for (Int32Column column : dataFrame.getInt32ColumnsList()) {
            columnDocuments.add(Int32ColumnDocument.fromInt32Column(column));
        }

        for (BoolColumn column : dataFrame.getBoolColumnsList()) {
            columnDocuments.add(BoolColumnDocument.fromBoolColumn(column));
        }

        for (StringColumn column : dataFrame.getStringColumnsList()) {
            columnDocuments.add(StringColumnDocument.fromStringColumn(column));
        }

        for (EnumColumn column : dataFrame.getEnumColumnsList()) {
            columnDocuments.add(EnumColumnDocument.fromEnumColumn(column));
        }

        for (DoubleArrayColumn column : dataFrame.getDoubleArrayColumnsList()) {
            columnDocuments.add(DoubleArrayColumnDocument.fromDoubleArrayColumn(column));
        }

        for (FloatArrayColumn column : dataFrame.getFloatArrayColumnsList()) {
            columnDocuments.add(FloatArrayColumnDocument.fromFloatArrayColumn(column));
        }

        for (Int32ArrayColumn column : dataFrame.getInt32ArrayColumnsList()) {
            columnDocuments.add(Int32ArrayColumnDocument.fromInt32ArrayColumn(column));
        }

        for (Int64ArrayColumn column : dataFrame.getInt64ArrayColumnsList()) {
            columnDocuments.add(Int64ArrayColumnDocument.fromInt64ArrayColumn(column));
        }

        for (BoolArrayColumn column : dataFrame.getBoolArrayColumnsList()) {
            columnDocuments.add(BoolArrayColumnDocument.fromBoolArrayColumn(column));
        }

        for (StructColumn column : dataFrame.getStructColumnsList()) {
            columnDocuments.add(StructColumnDocument.fromStructColumn(column));
        }

        for (ImageColumn column : dataFrame.getImageColumnsList()) {
            columnDocuments.add(ImageColumnDocument.fromImageColumn(column));
        }

        return columnDocuments;
    }

    /**
     * Adds the protobuf column represented by the supplied document to the DataFrame builder's
     * repeated field for its concrete type — the reverse of {@link #fromDataFrame(DataFrame)}.
     * Consumed by CalculationsDataFrameDocument.toCalculationsDataFrame(); buckets do not need the
     * reverse direction (query results are assembled per bucket via addColumnToBucket()). Adding a
     * new protobuf column type requires a branch here too; see "Systematic Process for Adding New
     * Protobuf Column Types" in CLAUDE.md.
     */
    public static void addColumnToDataFrame(
            DataFrame.Builder frameBuilder, ColumnDocumentBase columnDocument) throws DpException {

        // The legacy document converts through toDataColumn(), which throws (checked) on a
        // malformed stored byte array; toProtobufColumn() would instead silently substitute an
        // empty column — a wrong answer rather than an error.
        if (columnDocument instanceof DataColumnDocument dataColumnDocument) {
            frameBuilder.addDataColumns(dataColumnDocument.toDataColumn());
            return;
        }

        final Message column;
        try {
            column = columnDocument.toProtobufColumn();
        } catch (RuntimeException ex) {
            // binary column documents wrap deserialization failures unchecked; surface them as
            // DpException so a malformed stored column is a reportable error, never an escaped throw
            throw new DpException("error converting stored column '" + columnDocument.getName()
                    + "' (" + columnDocument.getClass().getSimpleName() + "): " + ex.getMessage());
        }

        switch (column) {
            case SerializedDataColumn c -> frameBuilder.addSerializedDataColumns(c);
            case DoubleColumn c -> frameBuilder.addDoubleColumns(c);
            case FloatColumn c -> frameBuilder.addFloatColumns(c);
            case Int64Column c -> frameBuilder.addInt64Columns(c);
            case Int32Column c -> frameBuilder.addInt32Columns(c);
            case BoolColumn c -> frameBuilder.addBoolColumns(c);
            case StringColumn c -> frameBuilder.addStringColumns(c);
            case EnumColumn c -> frameBuilder.addEnumColumns(c);
            case DoubleArrayColumn c -> frameBuilder.addDoubleArrayColumns(c);
            case FloatArrayColumn c -> frameBuilder.addFloatArrayColumns(c);
            case Int32ArrayColumn c -> frameBuilder.addInt32ArrayColumns(c);
            case Int64ArrayColumn c -> frameBuilder.addInt64ArrayColumns(c);
            case BoolArrayColumn c -> frameBuilder.addBoolArrayColumns(c);
            case StructColumn c -> frameBuilder.addStructColumns(c);
            case ImageColumn c -> frameBuilder.addImageColumns(c);
            default -> throw new DpException("stored column '" + columnDocument.getName()
                    + "' has unsupported document type " + columnDocument.getClass().getSimpleName());
        }
    }
}
