package com.ospreydcs.dp.client.criteria;

import java.util.List;

/**
 * Guards shared by every client-side criterion builder.
 *
 * <p>These live here rather than on one client because the invariant they enforce is not specific
 * to one service.  {@link #nonBlank} is the single source for the issue #243 rule, and that rule
 * binds every criterion that reaches {@code MongoQueryFilterBuilder.nameMatchFilter()} — the
 * annotation queries and, since issue #244, the Query API V2 {@code PvSelector.MetadataQuery}
 * selector alike.  A second copy of a four-line guard is exactly the drift a shared helper exists
 * to prevent: a fix applied to one copy leaves the other silently wrong.
 *
 * <p>There is deliberately no weaker helper here to reach for.
 */
public final class ClientCriteria {

    private ClientCriteria() {
    }

    /**
     * Returns the non-blank entries of the supplied list, never null.
     *
     * <p>Blank entries must never reach the server.  A blank prefix or contains value becomes the
     * regex {@code "^" + Pattern.quote("")} (respectively {@code ".*" + Pattern.quote("") + ".*"})
     * in {@code MongoQueryFilterBuilder.nameMatchFilter()}, and both match EVERY value — so a
     * caller binding an unfilled optional UI field would silently retrieve the entire collection
     * instead of applying no filter.  That is a silent wrong answer rather than an error, so the
     * blanks are dropped here rather than being sent and rejected.
     *
     * <p>Null entries are dropped for the same reason they cannot be forwarded: protobuf's addAll
     * throws NullPointerException on a null element.
     */
    public static List<String> nonBlank(List<String> list) {
        if (list == null) {
            return List.of();
        }
        return list.stream()
                .filter(value -> value != null && !value.isBlank())
                .toList();
    }

    /**
     * True when the supplied attribute key is unusable as a criterion key.  The server validates
     * AttributesCriterion.key with isBlank(), so a blank key is an avoidable rejection rather than
     * an omitted filter.
     */
    public static boolean isBlankKey(String key) {
        return key == null || key.isBlank();
    }
}
