package com.ospreydcs.dp.service.common.model;

public class MongoSaveResult {
    
    public final Boolean isError;
    public final String message;
    public final String documentId;
    public final boolean isNewDocument;

    public MongoSaveResult(Boolean isError, String message, String documentId, boolean isNewDocument) {
        this.isError = isError;
        this.message = message;
        this.documentId = documentId;
        this.isNewDocument = isNewDocument;
    }
}
