package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;

/**
 * BSON document class for storing ColumnProvenance as an embedded subdocument within ColumnMetadataDocument.
 */
public class ColumnProvenanceDocument {

    private String source;
    private String process;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getProcess() {
        return process;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public static ColumnProvenanceDocument fromColumnProvenance(ColumnProvenance proto) {
        ColumnProvenanceDocument document = new ColumnProvenanceDocument();
        document.setSource(proto.getSource());
        document.setProcess(proto.getProcess());
        return document;
    }

    public ColumnProvenance toColumnProvenance() {
        return ColumnProvenance.newBuilder()
                .setSource(source != null ? source : "")
                .setProcess(process != null ? process : "")
                .build();
    }
}
