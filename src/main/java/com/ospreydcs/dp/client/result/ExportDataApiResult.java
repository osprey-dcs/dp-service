package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;

public class ExportDataApiResult extends ApiResultBase {
    
    public final ExportDataResponse.ExportDataResult exportDataResult;
    
    public ExportDataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.exportDataResult = null;
    }

    public ExportDataApiResult(ExportDataResponse.ExportDataResult exportDataResult) {
        super(false, "");
        this.exportDataResult = exportDataResult;
    }

}
