package com.ospreydcs.dp.client.result;

public class DeleteSampleStatusesApiResult extends ApiResultBase {

    // instance variables
    public final long deletedCount;

    public DeleteSampleStatusesApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.deletedCount = 0;
    }

    public DeleteSampleStatusesApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.deletedCount = 0;
    }

    public DeleteSampleStatusesApiResult(long deletedCount) {
        super(false, "");
        this.deletedCount = deletedCount;
    }

}
