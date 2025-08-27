package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ResultStatus;

import java.util.List;

public class DataImportResult {

    public final ResultStatus resultStatus;
    public final List<DataFrameResult> dataFrames;

    public DataImportResult(
            boolean isError,
            String errorMsg,
            List<DataFrameResult> dataFrames
    ) {
        final ResultStatus status = new ResultStatus(isError, errorMsg);
        this.resultStatus = status;
        this.dataFrames = dataFrames;
    }
    
    /**
     * Represents data from a single sheet/frame.
     */
    public static class DataFrameResult {
        public final String sheetName;
        public final List<Timestamp> timestamps;
        public final List<DataColumn> columns;
        
        public DataFrameResult(String sheetName, List<Timestamp> timestamps, List<DataColumn> columns) {
            this.sheetName = sheetName;
            this.timestamps = timestamps;
            this.columns = columns;
        }
    }
}
