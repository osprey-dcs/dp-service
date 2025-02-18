package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.bson.types.ObjectId;

import java.util.*;

public class DataSetDocument {

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
        document.applyRequest(request);
        return document;
    }

    public void applyRequest(CreateDataSetRequest request) {

        final List<DataBlockDocument> dataBlocks = new ArrayList<>();
        for (DataBlock dataBlock : request.getDataSet().getDataBlocksList()) {
            final Timestamp blockBeginTime =  dataBlock.getBeginTime();
            final Timestamp blockEndtime = dataBlock.getEndTime();
            final long blockBeginSeconds = blockBeginTime.getEpochSeconds();
            final long blockBeginNanos = blockBeginTime.getNanoseconds();
            final long blockEndSeconds = blockEndtime.getEpochSeconds();
            final long blockEndNanos = blockEndtime.getNanoseconds();
            List<String> blockPvNames = dataBlock.getPvNamesList();
            DataBlockDocument documentBlock = new DataBlockDocument(
                    blockBeginSeconds,
                    blockBeginNanos,
                    blockEndSeconds,
                    blockEndNanos,
                    blockPvNames);
            dataBlocks.add(documentBlock);
        }
        this.setDataBlocks(dataBlocks);

        this.setName(request.getDataSet().getName());
        this.setOwnerId(request.getDataSet().getOwnerId());
        this.setDescription(request.getDataSet().getDescription());
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
            final Timestamp requestBlockBeginTime =  requestDataBlock.getBeginTime();
            final Timestamp requestBlockEndTime = requestDataBlock.getEndTime();
            final long requestBlockBeginSeconds = requestBlockBeginTime.getEpochSeconds();
            final long requestBlockBeginNanos = requestBlockBeginTime.getNanoseconds();
            final long requestBlockEndSeconds = requestBlockEndTime.getEpochSeconds();
            final long requestBlockEndNanos = requestBlockEndTime.getNanoseconds();
            Set<String> requestPvNames = new TreeSet<>(requestDataBlock.getPvNamesList());

            final DataBlockDocument dataBlockDocument = this.getDataBlocks().get(blockIndex);
            if (requestBlockBeginSeconds != dataBlockDocument.getBeginTimeSeconds()) {
                final String msg = "block beginTime seconds mistmatch: " + dataBlockDocument.getBeginTimeSeconds()
                        + " expected: " + requestBlockBeginSeconds;
                diffs.add(msg);
            }
            if (requestBlockBeginNanos != dataBlockDocument.getBeginTimeNanos()) {
                final String msg = "block beginTime nanos mistmatch: " + dataBlockDocument.getBeginTimeNanos()
                        + " expected: " + requestBlockBeginNanos;
                diffs.add(msg);
            }
            if (requestBlockEndSeconds != dataBlockDocument.getEndTimeSeconds()) {
                final String msg = "block endTime seconds mistmatch: " + dataBlockDocument.getEndTimeSeconds()
                        + " expected: " + requestBlockEndSeconds;
                diffs.add(msg);
            }
            if (requestBlockEndNanos != dataBlockDocument.getEndTimeNanos()) {
                final String msg = "block endTime nanos mistmatch: " + dataBlockDocument.getEndTimeNanos()
                        + " expected: " + requestBlockEndNanos;
                diffs.add(msg);
            }
            if (requestPvNames.size() != dataBlockDocument.getPvNames().size()) {
                final String msg = "block pvNames list size mismatch: " + dataBlockDocument.getPvNames().size()
                        + " expected: " + requestPvNames.size();
                diffs.add(msg);
            }
            if (!Objects.equals(requestPvNames, dataBlockDocument.getPvNames())) {
                final String msg = "block pvNames list mismatch: " + dataBlockDocument.getPvNames().toString()
                        + " expected: " + requestPvNames.toString();
                diffs.add(msg);
            }
        }

        return diffs;
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
            Timestamp blockBeginTime = Timestamp.newBuilder()
                    .setEpochSeconds(dataBlockDocument.getBeginTimeSeconds())
                    .setNanoseconds(dataBlockDocument.getBeginTimeNanos())
                    .build();
            Timestamp blockEndTime = Timestamp.newBuilder()
                    .setEpochSeconds(dataBlockDocument.getEndTimeSeconds())
                    .setNanoseconds(dataBlockDocument.getEndTimeNanos())
                    .build();
            com.ospreydcs.dp.grpc.v1.annotation.DataBlock responseBlock =
                    com.ospreydcs.dp.grpc.v1.annotation.DataBlock.newBuilder()
                            .setBeginTime(blockBeginTime)
                            .setEndTime(blockEndTime)
                            .addAllPvNames(dataBlockDocument.getPvNames())
                            .build();
            dataSetBuilder.addDataBlocks(responseBlock);
        }

        return dataSetBuilder.build();
    }
}
