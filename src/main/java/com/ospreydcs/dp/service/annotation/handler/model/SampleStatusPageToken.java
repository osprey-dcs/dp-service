package com.ospreydcs.dp.service.annotation.handler.model;

import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Keyset page token for querySampleStatuses: the sort position of the last bucket returned, in
 * result order (pvName, domain, layer, firstTimeNanos). Resuming filters strictly greater in
 * tuple order, so page boundaries always fall between complete buckets and remain stable while
 * the collection is rewritten in place (carves, splits) — a skip-offset token would drift.
 *
 * <p>The position is unique: two documents for the same (pvName, domain, layer) sharing
 * firstTimeNanos would both assert the identity key of that first timestamp, which the storage
 * invariant forbids.
 *
 * <p>Unparseable tokens decode to null and are rejected by the caller, per the API contract
 * (unlike the metadata APIs' skip-offset tokens, which silently reset to the first page).
 */
public record SampleStatusPageToken(String pvName, String domain, String layer, long firstTimeNanos) {

    private static final String KEY_PV_NAME = "pvName";
    private static final String KEY_DOMAIN = "domain";
    private static final String KEY_LAYER = "layer";
    private static final String KEY_FIRST_TIME_NANOS = "firstTimeNanos";

    public String encode() {
        final Document document = new Document()
                .append(KEY_PV_NAME, pvName)
                .append(KEY_DOMAIN, domain)
                .append(KEY_LAYER, layer)
                .append(KEY_FIRST_TIME_NANOS, firstTimeNanos);
        return Base64.getEncoder().encodeToString(document.toJson().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes a token issued by {@link #encode()}, returning null for anything unparseable.
     */
    public static SampleStatusPageToken decode(String token) {
        try {
            final Document document = Document.parse(
                    new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8));
            final String pvName = document.getString(KEY_PV_NAME);
            final String domain = document.getString(KEY_DOMAIN);
            final String layer = document.getString(KEY_LAYER);
            final Number firstTimeNanos = (Number) document.get(KEY_FIRST_TIME_NANOS);
            if (pvName == null || domain == null || layer == null || firstTimeNanos == null) {
                return null;
            }
            return new SampleStatusPageToken(pvName, domain, layer, firstTimeNanos.longValue());
        } catch (RuntimeException ex) {
            return null;
        }
    }
}
