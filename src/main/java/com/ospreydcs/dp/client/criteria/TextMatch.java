package com.ospreydcs.dp.client.criteria;

import java.util.List;

/**
 * Matches a text field by any combination of exact, prefix and substring values.  Mirrors the
 * proto PvNameCriterion / AliasesCriterion / configuration NameCriterion messages, all of which
 * have the same shape — including {@code PvSelector.MetadataQuery.Criterion.PvNameCriterion} on
 * the Query API V2, whose proto type is deliberately distinct from the annotation query's but
 * whose client-side shape and semantics are identical.
 *
 * <p>All values across all three lists are ORed: a record matches if it satisfies any supplied
 * value from any list.  A null list is treated as empty.  A TextMatch with all three lists
 * null or empty contributes no criterion to the request.
 *
 * <p>To require that a name satisfy two matches simultaneously (AND), build the request directly
 * with two separate criterion entries and pass it to the corresponding sendXxx() method.
 */
public record TextMatch(
        List<String> exact,
        List<String> prefix,
        List<String> contains
) {
    /**
     * True when no usable match value is present.  Blank and null entries do not count: a
     * TextMatch holding only blanks is empty, because those entries are dropped when the
     * criterion is built (see {@link ClientCriteria#nonBlank}).  Were this to report false for
     * such a TextMatch, the builder would emit a criterion with all three lists empty, which the
     * server rejects.
     */
    public boolean isEmpty() {
        return ClientCriteria.nonBlank(exact).isEmpty()
                && ClientCriteria.nonBlank(prefix).isEmpty()
                && ClientCriteria.nonBlank(contains).isEmpty();
    }
}
