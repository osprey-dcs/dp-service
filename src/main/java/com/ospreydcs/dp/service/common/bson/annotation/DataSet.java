package com.ospreydcs.dp.service.common.bson.annotation;

import java.util.List;

public class DataSet {
    private List<DataBlock> dataBlocks;

    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DataBlock> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }
}
