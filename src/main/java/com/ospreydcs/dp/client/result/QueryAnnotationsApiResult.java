package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.Annotation;

import java.util.List;

/**
 * Result of queryAnnotations().  nextPageToken is non-empty when more pages are available; supply
 * it as the pageToken of the next request.  An empty nextPageToken indicates the last page.
 */
public class QueryAnnotationsApiResult extends ApiResultBase {
    
    // instance variables
    public final List<Annotation> annotations;
    public final String nextPageToken;

    public QueryAnnotationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotations = null;
        this.nextPageToken = "";
    }

    public QueryAnnotationsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.annotations = null;
        this.nextPageToken = "";
    }

    public QueryAnnotationsApiResult(List<Annotation> annotations, String nextPageToken) {
        super(false, "");
        this.annotations = annotations;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
