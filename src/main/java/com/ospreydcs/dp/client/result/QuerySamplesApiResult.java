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
 *
 * <p><strong>{@code serializedColumnsFragmented} qualifies the table and must be checked before
 * consuming one built with {@code useSerializedColumns}.</strong>  It is true only for the
 * streaming method, and only when more than one streamed page carried serialized columns: those
 * cannot be merged without deserializing them, so {@code columnTable.serializedDataColumns} then
 * holds per-page column <em>fragments</em> (the same column name repeated once per page) against a
 * fully concatenated timestamp axis.  The table is structurally well formed but its columns do not
 * line up with its axis, so treating it as an assembled table is a wrong answer rather than an
 * error — which is why the condition is reported here rather than only documented.  It is always
 * false for the unary method, for the non-serialized representation, and for a single-page stream,
 * in all of which the table is directly consumable.
 */
public class QuerySamplesApiResult extends ApiResultBase {

    // instance variables
    public final ColumnTable columnTable;
    public final String nextPageToken;
    public final boolean serializedColumnsFragmented;

    public QuerySamplesApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.columnTable = null;
        this.nextPageToken = "";
        this.serializedColumnsFragmented = false;
    }

    public QuerySamplesApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.columnTable = null;
        this.nextPageToken = "";
        this.serializedColumnsFragmented = false;
    }

    public QuerySamplesApiResult(ColumnTable columnTable, String nextPageToken) {
        this(columnTable, nextPageToken, false);
    }

    public QuerySamplesApiResult(
            ColumnTable columnTable, String nextPageToken, boolean serializedColumnsFragmented
    ) {
        super(false, "");
        this.columnTable = columnTable;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
        this.serializedColumnsFragmented = serializedColumnsFragmented;
    }

}
