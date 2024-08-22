package com.ospreydcs.dp.service.ingest.handler.model;

import com.ospreydcs.dp.service.common.bson.ProviderDocument;

public class FindProviderResult {

    // instance variables
    public final boolean isException;
    public final String errorMessage;
    public final ProviderDocument providerDocument;

    public FindProviderResult(boolean isException, String errorMessage, ProviderDocument providerDocument) {
        this.isException = isException;
        this.errorMessage = errorMessage;
        this.providerDocument = providerDocument;
    }

    public static FindProviderResult findProviderError(String errorMessage) {
        return new FindProviderResult(true, errorMessage, null);
    }

    public static FindProviderResult findProviderSuccess(ProviderDocument providerDocument) {
        return new FindProviderResult(false, null, providerDocument);
    }
}
