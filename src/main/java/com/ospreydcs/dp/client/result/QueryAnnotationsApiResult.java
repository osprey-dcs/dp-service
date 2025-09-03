package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;

import java.util.List;

public class QueryAnnotationsApiResult extends ApiResultBase {
    
    // instance variables
    public final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotations;

    public QueryAnnotationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotations = null;
    }

    public QueryAnnotationsApiResult(List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotations) {
        super(false, "");
        this.annotations = annotations;
    }

}
