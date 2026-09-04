package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;

import java.util.List;

/**
 * Result of a paginated Annotation query, bundling the current page of documents
 * with the next-page token (empty string when no further pages exist).
 */
public class AnnotationQueryResult {

    private final List<AnnotationDocument> documents;
    private final String nextPageToken;

    public AnnotationQueryResult(List<AnnotationDocument> documents, String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<AnnotationDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
