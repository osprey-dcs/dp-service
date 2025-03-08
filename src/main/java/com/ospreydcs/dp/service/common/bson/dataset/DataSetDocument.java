package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import org.bson.types.ObjectId;

import java.util.*;

public class DataSetDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String name;
    private String ownerId;
    private String description;
    private List<DataBlockDocument> dataBlocks;

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

    public static DataSetDocument fromCreateRequest(CreateDataSetRequest request) {

        DataSetDocument document = new DataSetDocument();

        final List<DataBlockDocument> dataBlocks = new ArrayList<>();
        for (DataBlock dataBlock : request.getDataSet().getDataBlocksList()) {
            DataBlockDocument documentBlock = DataBlockDocument.fromDataBlock(dataBlock);
            dataBlocks.add(documentBlock);
        }
        document.setDataBlocks(dataBlocks);

        document.setName(request.getDataSet().getName());
        document.setOwnerId(request.getDataSet().getOwnerId());
        document.setDescription(request.getDataSet().getDescription());

        return document;
    }

    public DataSet toDataSet() {

        final DataSet.Builder dataSetBuilder = DataSet.newBuilder();

        // add base dataset fields to response object
        dataSetBuilder.setId(this.getId().toString());
        dataSetBuilder.setName(this.getName());
        dataSetBuilder.setOwnerId(this.getOwnerId());
        dataSetBuilder.setDescription(this.getDescription());

        // add dataset content to response object
        for (DataBlockDocument dataBlockDocument : this.getDataBlocks()) {
            dataSetBuilder.addDataBlocks(dataBlockDocument.toDataBlock());
        }

        return dataSetBuilder.build();
    }

    public List<String> diffRequest(CreateDataSetRequest request) {

        final List<String> diffs = new ArrayList<>();
        
        // diff name
        if (! Objects.equals(request.getDataSet().getName(), this.getName())) {
            final String msg =
                    "name: " + request.getDataSet().getName() + " mismatch: " + this.getName();
            diffs.add(msg);
        }

        // diff description
        if (! Objects.equals(request.getDataSet().getDescription(), this.getDescription())) {
            final String msg =
                    "description: " + request.getDataSet().getDescription() + " mismatch: " + this.getDescription();
            diffs.add(msg);
        }

        // diff DataSet
        if (request.getDataSet().getDataBlocksList().size() != getDataBlocks().size()) {
            final String msg = "DataSet DataBlocks list size mismatch: " + getDataBlocks().size()
                    + " expected: " + request.getDataSet().getDataBlocksList().size();
            diffs.add(msg);
        }
        for (int blockIndex = 0 ; blockIndex < request.getDataSet().getDataBlocksList().size() ; ++blockIndex) {
            final com.ospreydcs.dp.grpc.v1.annotation.DataBlock requestDataBlock =
                    request.getDataSet().getDataBlocksList().get(blockIndex);
            final DataBlockDocument dataBlockDocument = this.getDataBlocks().get(blockIndex);
            diffs.addAll(dataBlockDocument.diffDataBlock(requestDataBlock));
        }

        return diffs;
    }

}
