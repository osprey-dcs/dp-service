package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.PvMetadata;

/**
 * Result of getPvMetadata().  A PV name or alias with no matching record is reported as a rejection
 * (isReject() true), not as a successful result with a null pvMetadata.
 */
public class GetPvMetadataApiResult extends ApiResultBase {

    // instance variables
    public final PvMetadata pvMetadata;

    public GetPvMetadataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.pvMetadata = null;
    }

    public GetPvMetadataApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.pvMetadata = null;
    }

    public GetPvMetadataApiResult(PvMetadata pvMetadata) {
        super(false, "");
        this.pvMetadata = pvMetadata;
    }

}
