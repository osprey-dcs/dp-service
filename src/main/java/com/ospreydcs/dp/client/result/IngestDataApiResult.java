package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;

public class IngestDataApiResult extends ApiResultBase {

    // instance variables
    public final IngestDataResponse ingestDataResponse;

    public IngestDataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.ingestDataResponse = null;
    }

    public IngestDataApiResult(IngestDataResponse ingestDataResponse) {
        super(false, "");
        this.ingestDataResponse = ingestDataResponse;
    }

}
