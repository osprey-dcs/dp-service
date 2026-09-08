package com.ospreydcs.dp.service.annotation.handler.model;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Keyset page token for queryDataSets and queryAnnotations: the id of the last record returned.
 * Both queries sort by id ascending (unique, per the proto ordering contract), so resuming filters
 * strictly greater and page boundaries remain stable while documents are inserted or deleted
 * mid-pagination — a skip-offset token would drift (#248 Phase 3, plan D18).
 *
 * <p>The query discriminator exists because the two queries' tokens are otherwise structurally
 * identical: without it, a queryDataSets token pasted into queryAnnotations would decode cleanly
 * and silently skip an arbitrary prefix of results — a wrong answer rather than an error (D19).
 *
 * <p>Unparseable and wrong-query tokens decode to null and are rejected by the caller, per the API
 * contract (unlike the metadata APIs' skip-offset tokens, which silently reset to the first page).
 */
public record AnnotationQueryPageToken(String query, String lastId) {

    public static final String QUERY_DATA_SETS = "dataSets";
    public static final String QUERY_ANNOTATIONS = "annotations";

    private static final String KEY_QUERY = "query";
    private static final String KEY_LAST_ID = "lastId";

    public String encode() {
        final Document document = new Document()
                .append(KEY_QUERY, query)
                .append(KEY_LAST_ID, lastId);
        return Base64.getEncoder().encodeToString(document.toJson().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes a token issued by {@link #encode()} for the given query, returning null for anything
     * unparseable, any lastId that is not ObjectId hex, and any token issued for a different query.
     */
    public static AnnotationQueryPageToken decode(String token, String expectedQuery) {
        try {
            final Document document = Document.parse(
                    new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8));
            final String query = document.getString(KEY_QUERY);
            final String lastId = document.getString(KEY_LAST_ID);
            if (!expectedQuery.equals(query) || lastId == null || !ObjectId.isValid(lastId)) {
                return null;
            }
            return new AnnotationQueryPageToken(query, lastId);
        } catch (RuntimeException ex) {
            return null;
        }
    }
}
