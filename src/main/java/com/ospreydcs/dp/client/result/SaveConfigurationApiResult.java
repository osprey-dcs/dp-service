package com.ospreydcs.dp.client.result;

public class SaveConfigurationApiResult extends ApiResultBase {

    // instance variables
    public final String configurationName;

    public SaveConfigurationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.configurationName = null;
    }

    public SaveConfigurationApiResult(String configurationName) {
        super(false, "");
        this.configurationName = configurationName;
    }

}
