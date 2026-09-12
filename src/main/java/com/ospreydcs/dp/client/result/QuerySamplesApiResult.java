package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.query.ColumnTable;

/**
 * Result of {@code querySamples()} / {@code querySamplesStream()} (Query API V2).
 *
 * <p>For the unary method, {@code nextPageToken} is non-empty when more pages are available; for
 * the streaming method the table is the accumulated result of the whole stream and {@code
 * nextPageToken} is always empty.
 *
 * <p>An empty query result is a <em>success</em> carrying a table with an empty timestamp list, not
 * a failure.  Note that the table carries no column metadata on either method — the sample assembly
 * path does not produce any; use {@code queryBuckets()} or {@code queryPvMetadata()} when metadata
 * is needed.
 */
public class QuerySamplesApiResult extends ApiResultBase {

    // instance variables
    public final ColumnTable columnTable;
    public final String nextPageToken;

    public QuerySamplesApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.columnTable = null;
        this.nextPageToken = "";
    }

    public QuerySamplesApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.columnTable = null;
        this.nextPageToken = "";
    }

    public QuerySamplesApiResult(ColumnTable columnTable, String nextPageToken) {
        super(false, "");
        this.columnTable = columnTable;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
