package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.client.IngestionStreamClient;

public class SubscribeDataEventApiResult extends ApiResultBase {
    
    // instance variables
    public final IngestionStreamClient.SubscribeDataEventCall subscribeDataEventCall;

    public SubscribeDataEventApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.subscribeDataEventCall = null;
    }

    public SubscribeDataEventApiResult(IngestionStreamClient.SubscribeDataEventCall subscribeDataEventCall) {
        super(false, "");
        this.subscribeDataEventCall = subscribeDataEventCall;
    }

}
