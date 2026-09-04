package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.Annotation;

public class GetAnnotationApiResult extends ApiResultBase {

    // instance variables
    public final Annotation annotation;

    public GetAnnotationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotation = null;
    }

    public GetAnnotationApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.annotation = null;
    }

    public GetAnnotationApiResult(Annotation annotation) {
        super(false, "");
        this.annotation = annotation;
    }

}
