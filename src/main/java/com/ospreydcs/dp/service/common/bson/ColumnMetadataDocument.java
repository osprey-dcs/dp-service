package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;

import java.util.List;
import java.util.Map;

/**
 * BSON document class for storing ColumnMetadata as an embedded subdocument within ColumnDocumentBase.
 * Persists optional column-level provenance, tags, and attributes supplied with an ingestion request.
 */
public class ColumnMetadataDocument {

    private ColumnProvenanceDocument provenance;
    private List<String> tags;
    private Map<String, String> attributes;

    public ColumnProvenanceDocument getProvenance() {
        return provenance;
    }

    public void setProvenance(ColumnProvenanceDocument provenance) {
        this.provenance = provenance;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public static ColumnMetadataDocument fromColumnMetadata(ColumnMetadata proto) {
        ColumnMetadataDocument document = new ColumnMetadataDocument();
        if (proto.hasProvenance()) {
            document.setProvenance(ColumnProvenanceDocument.fromColumnProvenance(proto.getProvenance()));
        }
        if (proto.getTagsCount() > 0) {
            document.setTags(proto.getTagsList());
        }
        if (proto.getAttributesCount() > 0) {
            document.setAttributes(AttributesUtility.attributeMapFromList(proto.getAttributesList()));
        }
        return document;
    }

    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata.Builder builder = ColumnMetadata.newBuilder();
        if (provenance != null) {
            builder.setProvenance(provenance.toColumnProvenance());
        }
        if (tags != null) {
            builder.addAllTags(tags);
        }
        if (attributes != null) {
            List<Attribute> attributeList = AttributesUtility.attributeListFromMap(attributes);
            builder.addAllAttributes(attributeList);
        }
        return builder.build();
    }
}
