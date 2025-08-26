package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ResultStatus;

import java.util.List;

public class DataImportResult {

    public final ResultStatus resultStatus;
    public final List<Timestamp> timestamps;
    public final List<DataColumn> columns;

    public DataImportResult(
            boolean isError,
            String errorMsg,
            List<Timestamp> timestamps,
            List<DataColumn> columns
    ) {
        final ResultStatus status = new ResultStatus(isError, errorMsg);
        this.resultStatus = status;
        this.timestamps = timestamps;
        this.columns = columns;
    }
}
