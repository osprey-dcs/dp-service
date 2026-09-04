package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;

import java.util.List;

/**
 * Result of a paginated DataSet query, bundling the current page of documents
 * with the next-page token (empty string when no further pages exist).
 */
public class DataSetQueryResult {

    private final List<DataSetDocument> documents;
    private final String nextPageToken;

    public DataSetQueryResult(List<DataSetDocument> documents, String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<DataSetDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
