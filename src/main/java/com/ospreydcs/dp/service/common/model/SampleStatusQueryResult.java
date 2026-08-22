package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;

import java.util.List;

/**
 * Result of a paginated sample status query, bundling the current page of documents with the
 * next-page token (empty string when no further pages exist).
 */
public class SampleStatusQueryResult {

    private final List<SampleStatusBucketDocument> documents;
    private final String nextPageToken;

    public SampleStatusQueryResult(List<SampleStatusBucketDocument> documents, String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<SampleStatusBucketDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
