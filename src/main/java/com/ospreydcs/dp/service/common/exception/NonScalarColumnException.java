package com.ospreydcs.dp.service.common.exception;

/**
 * Thrown when a non-scalar column (array, image, struct, serialized) is encountered where only
 * scalar columns can be represented in a tabular (row/column) form. Carries the offending PV name
 * and stored column type so each caller can phrase context-appropriate guidance: the export
 * framework points at specialized export formats; the Query API V2 {@code querySamples} path points
 * the caller at {@code queryBuckets} (Q4). The message itself is neutral (no caller-specific wording)
 * so it reads correctly wherever it surfaces un-translated.
 */
public class NonScalarColumnException extends DpException {

    private final String pvName;
    private final String columnType;

    public NonScalarColumnException(String pvName, String columnType) {
        super("PV '" + pvName + "' has non-scalar column type " + columnType
                + ", which has no tabular (row/column) representation");
        this.pvName = pvName;
        this.columnType = columnType;
    }

    public String getPvName() {
        return pvName;
    }

    public String getColumnType() {
        return columnType;
    }
}
