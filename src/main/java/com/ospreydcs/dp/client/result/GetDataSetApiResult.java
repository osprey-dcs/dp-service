package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;

public class GetDataSetApiResult extends ApiResultBase {

    // instance variables
    public final DataSet dataSet;

    public GetDataSetApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.dataSet = null;
    }

    public GetDataSetApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.dataSet = null;
    }

    public GetDataSetApiResult(DataSet dataSet) {
        super(false, "");
        this.dataSet = dataSet;
    }

}
