package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.Annotation;

import java.util.List;

public class QueryAnnotationsApiResult extends ApiResultBase {
    
    // instance variables
    public final List<Annotation> annotations;

    public QueryAnnotationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotations = null;
    }

    public QueryAnnotationsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.annotations = null;
    }

    public QueryAnnotationsApiResult(List<Annotation> annotations) {
        super(false, "");
        this.annotations = annotations;
    }

}
