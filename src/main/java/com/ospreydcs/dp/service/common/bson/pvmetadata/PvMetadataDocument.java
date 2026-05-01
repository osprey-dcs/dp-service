package com.ospreydcs.dp.service.common.bson.pvmetadata;

import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.common.PvMetadata;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class PvMetadataDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String pvName;
    private List<String> aliases;
    private String description;
    private String modifiedBy;

    public PvMetadataDocument() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public List<String> getAliases() {
        return aliases;
    }

    public void setAliases(List<String> aliases) {
        this.aliases = aliases;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public static PvMetadataDocument fromSavePvMetadataRequest(SavePvMetadataRequest request) {

        PvMetadataDocument document = new PvMetadataDocument();

        document.setPvName(request.getPvName());

        if (!request.getAliasesList().isEmpty()) {
            document.setAliases(new ArrayList<>(request.getAliasesList()));
        }

        if (!request.getDescription().isBlank()) {
            document.setDescription(request.getDescription());
        }

        if (!request.getModifiedBy().isBlank()) {
            document.setModifiedBy(request.getModifiedBy());
        }

        // normalize tags: lowercase, unique, sorted
        if (!request.getTagsList().isEmpty()) {
            final TreeSet<String> normalizedTags = new TreeSet<>();
            for (String tag : request.getTagsList()) {
                normalizedTags.add(tag.toLowerCase());
            }
            document.setTags(new ArrayList<>(normalizedTags));
        }

        if (!request.getAttributesList().isEmpty()) {
            document.setAttributes(AttributesUtility.attributeMapFromList(request.getAttributesList()));
        }

        return document;
    }

    public PvMetadata toPvMetadata() {

        PvMetadata.Builder builder = PvMetadata.newBuilder();

        if (this.pvName != null) {
            builder.setPvName(this.pvName);
        }

        if (this.aliases != null) {
            builder.addAllAliases(this.aliases);
        }

        if (this.description != null) {
            builder.setDescription(this.description);
        }

        if (this.modifiedBy != null) {
            builder.setModifiedBy(this.modifiedBy);
        }

        if (this.getTags() != null) {
            builder.addAllTags(this.getTags());
        }

        if (this.getAttributes() != null) {
            builder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        if (this.getCreatedAt() != null) {
            builder.setCreatedTime(TimestampUtility.getTimestampFromInstant(this.getCreatedAt()));
        }

        if (this.getUpdatedAt() != null) {
            builder.setUpdatedTime(TimestampUtility.getTimestampFromInstant(this.getUpdatedAt()));
        }

        return builder.build();
    }
}
