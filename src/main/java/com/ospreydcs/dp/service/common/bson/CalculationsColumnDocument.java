package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;

/**
 * BSON document class for storing ColumnProvenance.CalculationsColumn as an embedded subdocument
 * within ColumnSourceDocument. Addresses a single column of a Calculations object by
 * calculationsId/frameName/columnName. Stored as supplied: the calculationsId is not parsed or
 * resolved — provenance links are soft associations and may dangle (see common.proto).
 */
public class CalculationsColumnDocument {

    private String calculationsId;
    private String frameName;
    private String columnName;

    public String getCalculationsId() {
        return calculationsId;
    }

    public void setCalculationsId(String calculationsId) {
        this.calculationsId = calculationsId;
    }

    public String getFrameName() {
        return frameName;
    }

    public void setFrameName(String frameName) {
        this.frameName = frameName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public static CalculationsColumnDocument fromCalculationsColumn(ColumnProvenance.CalculationsColumn proto) {
        CalculationsColumnDocument document = new CalculationsColumnDocument();
        // Store only non-empty strings so unset proto fields (which default to "") are stored as
        // null rather than ""; toCalculationsColumn() restores null as "".
        if (!proto.getCalculationsId().isEmpty()) {
            document.setCalculationsId(proto.getCalculationsId());
        }
        if (!proto.getFrameName().isEmpty()) {
            document.setFrameName(proto.getFrameName());
        }
        if (!proto.getColumnName().isEmpty()) {
            document.setColumnName(proto.getColumnName());
        }
        return document;
    }

    public ColumnProvenance.CalculationsColumn toCalculationsColumn() {
        return ColumnProvenance.CalculationsColumn.newBuilder()
                .setCalculationsId(calculationsId != null ? calculationsId : "")
                .setFrameName(frameName != null ? frameName : "")
                .setColumnName(columnName != null ? columnName : "")
                .build();
    }
}
