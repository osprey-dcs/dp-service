package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;

import java.util.ArrayList;
import java.util.List;

/**
 * BSON document class for storing ColumnProvenance as an embedded subdocument within ColumnMetadataDocument.
 */
public class ColumnProvenanceDocument {

    private String source;
    private String process;
    private List<ColumnSourceDocument> derivedFrom;

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

    public List<ColumnSourceDocument> getDerivedFrom() {
        return derivedFrom;
    }

    public void setDerivedFrom(List<ColumnSourceDocument> derivedFrom) {
        this.derivedFrom = derivedFrom;
    }

    public static ColumnProvenanceDocument fromColumnProvenance(ColumnProvenance proto) {
        ColumnProvenanceDocument document = new ColumnProvenanceDocument();
        // Only store non-empty strings so that unset proto fields (which default to "") are
        // stored as null in MongoDB rather than "".  toColumnProvenance() already handles null
        // by converting back to "", preserving correct protobuf round-trip semantics.
        if (!proto.getSource().isEmpty()) {
            document.setSource(proto.getSource());
        }
        if (!proto.getProcess().isEmpty()) {
            document.setProcess(proto.getProcess());
        }
        if (proto.getDerivedFromCount() > 0) {
            List<ColumnSourceDocument> sourceDocuments = new ArrayList<>();
            for (ColumnProvenance.ColumnSource columnSource : proto.getDerivedFromList()) {
                sourceDocuments.add(ColumnSourceDocument.fromColumnSource(columnSource));
            }
            document.setDerivedFrom(sourceDocuments);
        }
        return document;
    }

    public ColumnProvenance toColumnProvenance() {
        ColumnProvenance.Builder builder = ColumnProvenance.newBuilder()
                .setSource(source != null ? source : "")
                .setProcess(process != null ? process : "");
        if (derivedFrom != null) {
            for (ColumnSourceDocument sourceDocument : derivedFrom) {
                builder.addDerivedFrom(sourceDocument.toColumnSource());
            }
        }
        return builder.build();
    }
}
