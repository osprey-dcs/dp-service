package com.ospreydcs.dp.client.result;

public class SaveDataSetApiResult extends ApiResultBase {

    // instance variables
    public final String datasetId;

    public SaveDataSetApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.datasetId = null;
    }

    public SaveDataSetApiResult(String datasetId) {
        super(false, "");
        this.datasetId = datasetId;
    }

}
