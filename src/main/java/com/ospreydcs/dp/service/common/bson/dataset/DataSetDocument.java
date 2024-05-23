package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;

import java.util.*;

public class DataSetDocument {

    // instance variables
    private String description;
    private List<DocumentDataBlock> dataBlocks;

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<DocumentDataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DocumentDataBlock> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }

    public static DataSetDocument fromCreateRequest(CreateDataSetRequest request) {
        DataSetDocument document = new DataSetDocument();
        document.applyRequest(request);
        return document;
    }

    public void applyRequest(CreateDataSetRequest request) {

        final List<DocumentDataBlock> dataBlocks = new ArrayList<>();
        for (DataBlock dataBlock : request.getDataSet().getDataBlocksList()) {
            final Timestamp blockBeginTime =  dataBlock.getBeginTime();
            final Timestamp blockEndtime = dataBlock.getEndTime();
            final long blockBeginSeconds = blockBeginTime.getEpochSeconds();
            final long blockBeginNanos = blockBeginTime.getNanoseconds();
            final long blockEndSeconds = blockEndtime.getEpochSeconds();
            final long blockEndNanos = blockEndtime.getNanoseconds();
            List<String> blockPvNames = dataBlock.getPvNamesList();
            DocumentDataBlock documentBlock = new DocumentDataBlock(
                    blockBeginSeconds,
                    blockBeginNanos,
                    blockEndSeconds,
                    blockEndNanos,
                    blockPvNames);
            dataBlocks.add(documentBlock);
        }
        this.setDataBlocks(dataBlocks);

        this.setDescription(request.getDataSet().getDescription());
    }

    public List<String> diffRequest(CreateDataSetRequest request) {

        final List<String> diffs = new ArrayList<>();

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

            final DocumentDataBlock documentDataBlock = this.getDataBlocks().get(blockIndex);
            if (requestBlockBeginSeconds != documentDataBlock.getBeginTimeSeconds()) {
                final String msg = "block beginTime seconds mistmatch: " + documentDataBlock.getBeginTimeSeconds()
                        + " expected: " + requestBlockBeginSeconds;
                diffs.add(msg);
            }
            if (requestBlockBeginNanos != documentDataBlock.getBeginTimeNanos()) {
                final String msg = "block beginTime nanos mistmatch: " + documentDataBlock.getBeginTimeNanos()
                        + " expected: " + requestBlockBeginNanos;
                diffs.add(msg);
            }
            if (requestBlockEndSeconds != documentDataBlock.getEndTimeSeconds()) {
                final String msg = "block endTime seconds mistmatch: " + documentDataBlock.getEndTimeSeconds()
                        + " expected: " + requestBlockEndSeconds;
                diffs.add(msg);
            }
            if (requestBlockEndNanos != documentDataBlock.getEndTimeNanos()) {
                final String msg = "block endTime nanos mistmatch: " + documentDataBlock.getEndTimeNanos()
                        + " expected: " + requestBlockEndNanos;
                diffs.add(msg);
            }
            if (requestPvNames.size() != documentDataBlock.getPvNames().size()) {
                final String msg = "block pvNames list size mismatch: " + documentDataBlock.getPvNames().size()
                        + " expected: " + requestPvNames.size();
                diffs.add(msg);
            }
            if (!Objects.equals(requestPvNames, documentDataBlock.getPvNames())) {
                final String msg = "block pvNames list mismatch: " + documentDataBlock.getPvNames().toString()
                        + " expected: " + requestPvNames.toString();
                diffs.add(msg);
            }
        }

        return diffs;
    }

}
