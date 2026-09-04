package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;

public class DataSetDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String name;
    private String ownerId;
    private String description;
    private List<DataBlockDocument> dataBlocks;
    private String modifiedBy;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<DataBlockDocument> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DataBlockDocument> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public static DataSetDocument fromSaveRequest(SaveDataSetRequest request) {

        DataSetDocument document = new DataSetDocument();

        final List<DataBlockDocument> dataBlocks = new ArrayList<>();
        for (DataBlock dataBlock : request.getDataBlocksList()) {
            DataBlockDocument documentBlock = DataBlockDocument.fromDataBlock(dataBlock);
            dataBlocks.add(documentBlock);
        }
        document.setDataBlocks(dataBlocks);

        document.setName(request.getName());
        document.setOwnerId(request.getOwnerId());
        document.setDescription(request.getDescription());

        if (!request.getModifiedBy().isBlank()) {
            document.setModifiedBy(request.getModifiedBy());
        }

        // normalize tags: lowercase, unique, sorted
        if (!request.getTagsList().isEmpty()) {
            document.setTags(normalizedTags(request.getTagsList()));
        }

        if (!request.getAttributesList().isEmpty()) {
            document.setAttributes(AttributesUtility.attributeMapFromList(request.getAttributesList()));
        }

        return document;
    }

    public DataSet toDataSet() {

        final DataSet.Builder dataSetBuilder = DataSet.newBuilder();

        // add base dataset fields to response object
        dataSetBuilder.setId(this.getId().toString());
        dataSetBuilder.setName(this.getName());
        dataSetBuilder.setOwnerId(this.getOwnerId());

        // description is optional; a document saved without one leaves the proto field at its default
        if (this.getDescription() != null) {
            dataSetBuilder.setDescription(this.getDescription());
        }

        // add dataset content to response object
        for (DataBlockDocument dataBlockDocument : this.getDataBlocks()) {
            dataSetBuilder.addDataBlocks(dataBlockDocument.toDataBlock());
        }

        if (this.getModifiedBy() != null) {
            dataSetBuilder.setModifiedBy(this.getModifiedBy());
        }

        if (this.getTags() != null) {
            dataSetBuilder.addAllTags(this.getTags());
        }

        if (this.getAttributes() != null) {
            dataSetBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        if (this.getCreatedAt() != null) {
            dataSetBuilder.setCreatedTime(TimestampUtility.getTimestampFromInstant(this.getCreatedAt()));
        }

        if (this.getUpdatedAt() != null) {
            dataSetBuilder.setUpdatedTime(TimestampUtility.getTimestampFromInstant(this.getUpdatedAt()));
        }

        return dataSetBuilder.build();
    }

    public List<String> diffRequest(SaveDataSetRequest request) {

        final List<String> diffs = new ArrayList<>();
        
        // diff name
        if (! Objects.equals(request.getName(), this.getName())) {
            final String msg =
                    "name: " + request.getName() + " mismatch: " + this.getName();
            diffs.add(msg);
        }

        // diff description
        if (! Objects.equals(request.getDescription(), this.getDescription())) {
            final String msg =
                    "description: " + request.getDescription() + " mismatch: " + this.getDescription();
            diffs.add(msg);
        }

        // diff DataSet
        if (request.getDataBlocksList().size() != getDataBlocks().size()) {
            final String msg = "DataSet DataBlocks list size mismatch: " + getDataBlocks().size()
                    + " expected: " + request.getDataBlocksList().size();
            diffs.add(msg);
        }
        for (int blockIndex = 0 ; blockIndex < request.getDataBlocksList().size() ; ++blockIndex) {
            final com.ospreydcs.dp.grpc.v1.annotation.DataBlock requestDataBlock =
                    request.getDataBlocksList().get(blockIndex);
            final DataBlockDocument dataBlockDocument = this.getDataBlocks().get(blockIndex);
            diffs.addAll(dataBlockDocument.diffDataBlock(requestDataBlock));
        }

        // diff modifiedBy (blank in request is stored as null)
        final String requestModifiedBy = request.getModifiedBy().isBlank() ? null : request.getModifiedBy();
        if (! Objects.equals(requestModifiedBy, this.getModifiedBy())) {
            final String msg =
                    "modifiedBy: " + request.getModifiedBy() + " mismatch: " + this.getModifiedBy();
            diffs.add(msg);
        }

        // diff tags list against the normalized form the save path stores
        final List<String> expectedTags =
                request.getTagsList().isEmpty() ? null : normalizedTags(request.getTagsList());
        if (expectedTags != null && this.getTags() != null) {
            final Collection<String> tagsDisjunction =
                    CollectionUtils.disjunction(expectedTags, this.getTags());
            if (!tagsDisjunction.isEmpty()) {
                diffs.add("tags mismatch: " + this.getTags() + " disjunction: " + tagsDisjunction);
            }
        } else if (expectedTags != null || this.getTags() != null) {
            diffs.add("tags mismatch: " + this.getTags() + " expected: " + expectedTags);
        }

        // diff attributes
        final Map<String, String> expectedAttributes = request.getAttributesList().isEmpty()
                ? null : AttributesUtility.attributeMapFromList(request.getAttributesList());
        if (! Objects.equals(expectedAttributes, this.getAttributes())) {
            diffs.add("attributes mismatch: " + this.getAttributes() + " expected: " + expectedAttributes);
        }

        return diffs;
    }

}
