package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;

/**
 * BSON document class for storing ColumnProvenance.ColumnSource as an embedded subdocument within
 * ColumnProvenanceDocument's derivedFrom list. One of pvName / calculationsColumn is set,
 * mirroring the proto's origin oneof; the optional timeRange is stored as begin/end
 * TimestampDocuments.
 */
public class ColumnSourceDocument {

    private String pvName;
    private CalculationsColumnDocument calculationsColumn;
    private TimestampDocument timeRangeBegin;
    private TimestampDocument timeRangeEnd;

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public CalculationsColumnDocument getCalculationsColumn() {
        return calculationsColumn;
    }

    public void setCalculationsColumn(CalculationsColumnDocument calculationsColumn) {
        this.calculationsColumn = calculationsColumn;
    }

    public TimestampDocument getTimeRangeBegin() {
        return timeRangeBegin;
    }

    public void setTimeRangeBegin(TimestampDocument timeRangeBegin) {
        this.timeRangeBegin = timeRangeBegin;
    }

    public TimestampDocument getTimeRangeEnd() {
        return timeRangeEnd;
    }

    public void setTimeRangeEnd(TimestampDocument timeRangeEnd) {
        this.timeRangeEnd = timeRangeEnd;
    }

    public static ColumnSourceDocument fromColumnSource(ColumnProvenance.ColumnSource proto) {
        ColumnSourceDocument document = new ColumnSourceDocument();
        // Dispatch on the oneof case rather than field emptiness: a pvName arm set to "" still
        // selects the case, and round-trip must restore the case the caller supplied.
        switch (proto.getOriginCase()) {
            case PVNAME -> document.setPvName(proto.getPvName());
            case CALCULATIONSCOLUMN -> document.setCalculationsColumn(
                    CalculationsColumnDocument.fromCalculationsColumn(proto.getCalculationsColumn()));
            case ORIGIN_NOT_SET -> { }
        }
        if (proto.hasTimeRange()) {
            if (proto.getTimeRange().hasBeginTime()) {
                document.setTimeRangeBegin(TimestampDocument.fromTimestamp(proto.getTimeRange().getBeginTime()));
            }
            if (proto.getTimeRange().hasEndTime()) {
                document.setTimeRangeEnd(TimestampDocument.fromTimestamp(proto.getTimeRange().getEndTime()));
            }
        }
        return document;
    }

    public ColumnProvenance.ColumnSource toColumnSource() {
        ColumnProvenance.ColumnSource.Builder builder = ColumnProvenance.ColumnSource.newBuilder();
        if (pvName != null) {
            builder.setPvName(pvName);
        } else if (calculationsColumn != null) {
            builder.setCalculationsColumn(calculationsColumn.toCalculationsColumn());
        }
        if (timeRangeBegin != null || timeRangeEnd != null) {
            TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
            if (timeRangeBegin != null) {
                timeRangeBuilder.setBeginTime(timeRangeBegin.toTimestamp());
            }
            if (timeRangeEnd != null) {
                timeRangeBuilder.setEndTime(timeRangeEnd.toTimestamp());
            }
            builder.setTimeRange(timeRangeBuilder.build());
        }
        return builder.build();
    }
}
