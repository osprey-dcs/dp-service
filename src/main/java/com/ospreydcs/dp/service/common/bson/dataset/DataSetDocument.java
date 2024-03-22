package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;

import java.util.ArrayList;
import java.util.List;

public class DataSetDocument {

    // instance variables
    private List<DocumentDataBlock> dataBlocks;

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
    }

    public List<DocumentDataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DocumentDataBlock> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }
}
