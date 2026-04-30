package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;

import java.util.List;

/**
 * Result of a paginated PV metadata query, bundling the current page of documents
 * with the next-page token (empty string when no further pages exist).
 */
public class PvMetadataQueryResult {

    private final List<PvMetadataDocument> documents;
    private final String nextPageToken;

    public PvMetadataQueryResult(List<PvMetadataDocument> documents, String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<PvMetadataDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
