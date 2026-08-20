package com.ospreydcs.dp.client.result;

public class SavePvMetadataApiResult extends ApiResultBase {

    // instance variables
    public final String pvName;

    public SavePvMetadataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.pvName = null;
    }

    public SavePvMetadataApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.pvName = null;
    }

    public SavePvMetadataApiResult(String pvName) {
        super(false, "");
        this.pvName = pvName;
    }

}
