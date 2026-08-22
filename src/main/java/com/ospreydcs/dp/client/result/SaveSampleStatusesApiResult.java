package com.ospreydcs.dp.client.result;

public class SaveSampleStatusesApiResult extends ApiResultBase {

    // instance variables
    public final long savedCount;

    public SaveSampleStatusesApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.savedCount = 0;
    }

    public SaveSampleStatusesApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.savedCount = 0;
    }

    public SaveSampleStatusesApiResult(long savedCount) {
        super(false, "");
        this.savedCount = savedCount;
    }

}
