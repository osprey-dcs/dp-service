package com.ospreydcs.dp.client.result;

public class SaveAnnotationApiResult extends ApiResultBase {
    
    // instance variables
    public final String annotationId;

    public SaveAnnotationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotationId = null;
    }

    public SaveAnnotationApiResult(String annotationId) {
        super(false, "");
        this.annotationId = annotationId;
    }

}
