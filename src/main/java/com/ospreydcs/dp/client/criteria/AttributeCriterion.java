package com.ospreydcs.dp.client.criteria;

import java.util.List;

/**
 * Matches records by attribute key and optional value(s).  Mirrors the proto
 * AttributesCriterion message, which has the same shape on the annotation queries and on the
 * Query API V2 PV metadata selector.
 *
 * <p>key is required.  values is optional: when non-empty, the record must carry the key with one
 * of the specified values (ORed); when null or empty, any record possessing the key matches
 * regardless of its value (key-only existence search).
 *
 * <p>Note that this cannot be expressed as a Map&lt;String,String&gt; — hence the absence of
 * AttributesUtility here — because a map can represent neither multiple values for one key nor
 * a key-only search.
 */
public record AttributeCriterion(
        String key,
        List<String> values
) {
}
