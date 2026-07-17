package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ConfigurationSelector;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.ResultRepresentation;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.mongo.MongoQueryFilterBuilder;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.paging.PageToken;
import com.ospreydcs.dp.service.query.handler.paging.PageTokenException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.PatternSyntaxException;

/**
 * Query API V2 request resolver — turns a validated {@code QuerySpec + ExecutionOptions +
 * ResultRepresentation} into an internal {@link ResolvedQuery} (the "planner + ExecutionPlan").
 * Validates the §6 invariants proto3 cannot enforce, resolves the {@link PvSelector} to a concrete
 * PV name list (Q9), resolves the {@link ConfigurationSelector} to effective retrieval intervals
 * (Q3), normalizes paging (Q7), and decodes the continuation token (Q1/Q2).
 *
 * <p>Resolution steps that touch MongoDB go through {@link MongoQueryClientInterface}; the criterion
 * → {@code Bson} mapping reuses the shared {@link MongoQueryFilterBuilder} helpers (Q6), so V1 and
 * V2 build identical filters.
 */
public class QueryV2Resolver {

    private static final Logger logger = LogManager.getLogger();

    private final MongoQueryClientInterface mongoClient;
    private final int defaultPageSize;
    private final int maxPageSize;
    private final int maxResolvedPvCount;

    public QueryV2Resolver(
            MongoQueryClientInterface mongoClient,
            int defaultPageSize,
            int maxPageSize,
            int maxResolvedPvCount) {
        this.mongoClient = mongoClient;
        this.defaultPageSize = defaultPageSize;
        this.maxPageSize = maxPageSize;
        this.maxResolvedPvCount = maxResolvedPvCount;
    }

    /**
     * Validates and resolves a V2 request.
     *
     * @param querySpec the request's QuerySpec (required)
     * @param executionOptions paging options (may be default/empty)
     * @param resultRepresentation representation flags (may be default/empty)
     * @param mode BUCKET or SAMPLE result mode (set by the calling handler)
     * @param streaming true for the *Stream variants (set by the calling handler)
     * @return a ResolutionResult carrying either the ResolvedQuery or a reject/error status
     */
    public ResolutionResult resolve(
            QuerySpec querySpec,
            ExecutionOptions executionOptions,
            ResultRepresentation resultRepresentation,
            ResolvedQuery.ResultMode mode,
            boolean streaming) {

        // ---- §6: TimeRange presence + ordering ----
        if (!querySpec.hasTimeRange()) {
            return ResolutionResult.reject("querySpec.timeRange must be specified");
        }
        final TimeRange timeRange = querySpec.getTimeRange();
        if (!timeRange.hasBeginTime()) {
            return ResolutionResult.reject("querySpec.timeRange.beginTime must be specified");
        }
        if (!timeRange.hasEndTime()) {
            return ResolutionResult.reject("querySpec.timeRange.endTime must be specified");
        }
        final Timestamp begin = timeRange.getBeginTime();
        final Timestamp end = timeRange.getEndTime();
        if (TimeInterval.compareInstant(
                end.getEpochSeconds(), end.getNanoseconds(),
                begin.getEpochSeconds(), begin.getNanoseconds()) <= 0) {
            return ResolutionResult.reject("querySpec.timeRange endTime must be > beginTime");
        }
        final TimeInterval queryRange = new TimeInterval(
                begin.getEpochSeconds(), begin.getNanoseconds(),
                end.getEpochSeconds(), end.getNanoseconds());

        // ---- §6: PvSelector presence + exactly-one arm ----
        if (!querySpec.hasPvSelector()) {
            return ResolutionResult.reject("querySpec.pvSelector must be specified");
        }
        final PvSelector pvSelector = querySpec.getPvSelector();
        if (pvSelector.getSelectorCase() == PvSelector.SelectorCase.SELECTOR_NOT_SET) {
            return ResolutionResult.reject("querySpec.pvSelector must set one of pvNameList, pvNamePattern, metadataQuery");
        }

        // ---- §6/§Q7: paging normalization + streaming token rule ----
        final String pageToken = executionOptions.getPageToken();
        final boolean hasToken = pageToken != null && !pageToken.isBlank();
        if (streaming && hasToken) {
            return ResolutionResult.reject("pageToken must not be set on a streaming query");
        }

        KeysetPosition pageStart = null;
        if (hasToken) {
            try {
                pageStart = PageToken.decode(pageToken);
            } catch (PageTokenException ex) {
                return ResolutionResult.reject("invalid pageToken: " + ex.getMessage());
            }
            // guard against a token from the wrong result mode (bucket token on a sample query, etc.)
            final KeysetPosition.Kind expected = (mode == ResolvedQuery.ResultMode.BUCKET)
                    ? KeysetPosition.Kind.BUCKET : KeysetPosition.Kind.SAMPLE;
            if (pageStart.getKind() != expected) {
                return ResolutionResult.reject("pageToken is not valid for this query type");
            }
        }

        int pageSize = executionOptions.getLimit();
        if (pageSize <= 0) {
            pageSize = defaultPageSize;
        } else if (pageSize > maxPageSize) {
            pageSize = maxPageSize; // Q7: silent clamp
        }

        // ---- Q9: resolve PvSelector to a concrete, sorted PV name list ----
        final ResolvedNames resolvedNames = resolvePvNames(pvSelector);
        if (resolvedNames.error != null) {
            return ResolutionResult.reject(resolvedNames.error);
        }
        // sort + dedup (Q9 column order derives from this)
        final List<String> pvNames = new ArrayList<>(new TreeSet<>(resolvedNames.names));
        if (pvNames.size() > maxResolvedPvCount) {
            return ResolutionResult.reject(
                    "selector resolved to " + pvNames.size() + " PVs, exceeding the maximum of "
                            + maxResolvedPvCount + "; narrow the selector");
        }

        // ---- Q3: resolve ConfigurationSelector to effective retrieval intervals ----
        final ResolvedIntervals resolvedIntervals = resolveIntervals(querySpec, queryRange);
        if (resolvedIntervals.error != null) {
            return ResolutionResult.reject(resolvedIntervals.error);
        }
        final List<TimeInterval> intervals = resolvedIntervals.intervals;

        final boolean useSerialized = resultRepresentation.getUseSerializedColumns();
        final boolean excludeMetadata = resultRepresentation.getExcludeColumnMetadata();

        final ResolvedQuery resolved = new ResolvedQuery(
                pvNames, intervals, pageSize, pageStart, useSerialized, excludeMetadata, mode, streaming);
        return ResolutionResult.of(resolved);
    }

    // -----------------------------------------------------------------------
    // PvSelector resolution
    // -----------------------------------------------------------------------

    private static final class ResolvedNames {
        final List<String> names;
        final String error;

        private ResolvedNames(List<String> names, String error) {
            this.names = names;
            this.error = error;
        }

        static ResolvedNames ok(List<String> names) {
            return new ResolvedNames(names, null);
        }

        static ResolvedNames err(String error) {
            return new ResolvedNames(null, error);
        }
    }

    private ResolvedNames resolvePvNames(PvSelector pvSelector) {
        switch (pvSelector.getSelectorCase()) {

            case PVNAMELIST -> {
                final List<String> names = new ArrayList<>(pvSelector.getPvNameList().getPvNamesList());
                if (names.isEmpty()) {
                    return ResolvedNames.err("pvSelector.pvNameList is empty");
                }
                return ResolvedNames.ok(names);
            }

            case PVNAMEPATTERN -> {
                final String pattern = pvSelector.getPvNamePattern().getPattern();
                if (pattern == null || pattern.isBlank()) {
                    return ResolvedNames.err("pvSelector.pvNamePattern.pattern is empty");
                }
                final List<String> names;
                try {
                    names = mongoClient.resolvePvNamesByPattern(pattern);
                } catch (PatternSyntaxException ex) {
                    return ResolvedNames.err("pvSelector.pvNamePattern is not a valid regex: " + ex.getMessage());
                }
                if (names == null) {
                    return ResolvedNames.err("pv name pattern resolution failed (database error)");
                }
                return ResolvedNames.ok(names);
            }

            case METADATAQUERY -> {
                final PvSelector.MetadataQuery metadataQuery = pvSelector.getMetadataQuery();
                final List<Bson> criteriaFilters = new ArrayList<>();
                for (PvSelector.MetadataQuery.Criterion criterion : metadataQuery.getCriteriaList()) {
                    final CriterionFilter cf = metadataCriterionToFilter(criterion);
                    if (cf.invalid) {
                        return ResolvedNames.err("metadataQuery criterion must set exactly one arm");
                    }
                    if (cf.filter != null) {
                        criteriaFilters.add(cf.filter);
                    }
                }
                final List<String> names = mongoClient.resolvePvNamesByMetadata(criteriaFilters);
                if (names == null) {
                    return ResolvedNames.err("metadata query resolution failed (database error)");
                }
                return ResolvedNames.ok(names);
            }

            default -> {
                return ResolvedNames.err("pvSelector has no selector set");
            }
        }
    }

    /**
     * Result of mapping one metadata criterion: a filter (possibly null when the criterion's value
     * lists are all empty, which is a valid no-op arm), or {@code invalid} when no arm is set
     * (exactly-one-criterion violation, §6).
     */
    private static final class CriterionFilter {
        final Bson filter;
        final boolean invalid;

        private CriterionFilter(Bson filter, boolean invalid) {
            this.filter = filter;
            this.invalid = invalid;
        }

        static CriterionFilter of(Bson filter) {
            return new CriterionFilter(filter, false);
        }

        static CriterionFilter invalid() {
            return new CriterionFilter(null, true);
        }
    }

    private static CriterionFilter metadataCriterionToFilter(PvSelector.MetadataQuery.Criterion criterion) {
        switch (criterion.getCriterionCase()) {
            case PVNAMECRITERION -> {
                final var c = criterion.getPvNameCriterion();
                return CriterionFilter.of(MongoQueryFilterBuilder.nameMatchFilter(
                        BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                        c.getExactList(), c.getPrefixList(), c.getContainsList()));
            }
            case ALIASESCRITERION -> {
                final var c = criterion.getAliasesCriterion();
                return CriterionFilter.of(MongoQueryFilterBuilder.nameMatchFilter(
                        BsonConstants.BSON_KEY_PV_METADATA_ALIASES,
                        c.getExactList(), c.getPrefixList(), c.getContainsList()));
            }
            case TAGSCRITERION -> {
                return CriterionFilter.of(
                        MongoQueryFilterBuilder.tagsFilter(criterion.getTagsCriterion().getValuesList()));
            }
            case ATTRIBUTESCRITERION -> {
                final var c = criterion.getAttributesCriterion();
                return CriterionFilter.of(
                        MongoQueryFilterBuilder.attributeFilter(c.getKey(), c.getValuesList()));
            }
            default -> {
                // CRITERION_NOT_SET or unknown → exactly-one-criterion violation
                return CriterionFilter.invalid();
            }
        }
    }

    // -----------------------------------------------------------------------
    // ConfigurationSelector resolution (Q3)
    // -----------------------------------------------------------------------

    /**
     * @return the effective retrieval intervals, or null on database error. An empty list means the
     *     selector matched nothing inside the query range (caller yields an empty result). A
     *     malformed selector (empty criteria list, or a criterion with no arm set) is a client error
     *     and rejects — distinct from a well-formed selector that simply matches no activations, so a
     *     mis-built request is not silently indistinguishable from "no data" (mirrors the PvSelector
     *     metadata path and V1 {@code queryProviders}, both of which reject malformed criteria).
     */
    private ResolvedIntervals resolveIntervals(QuerySpec querySpec, TimeInterval queryRange) {

        if (!querySpec.hasConfigurationSelector()) {
            // no selector → the whole query range is the single retrieval interval
            final List<TimeInterval> single = new ArrayList<>();
            single.add(queryRange);
            return ResolvedIntervals.ok(single);
        }

        final ConfigurationSelector selector = querySpec.getConfigurationSelector();
        if (selector.getCriteriaList().isEmpty()) {
            // selector present but no criteria → malformed request (cf. queryProviders "criteria list
            // must not be empty"), not a "match nothing" intent → reject
            return ResolvedIntervals.reject("configurationSelector.criteria list must not be empty");
        }

        final List<Bson> criteriaFilters = new ArrayList<>();
        for (ConfigurationSelector.Criterion criterion : selector.getCriteriaList()) {
            final Bson f = configCriterionToFilter(criterion);
            if (f == null) {
                // exactly-one-criterion violation → reject (a client build error, not "no match")
                return ResolvedIntervals.reject("configurationSelector criterion must set exactly one arm");
            }
            criteriaFilters.add(f);
        }

        final List<TimeInterval> activationIntervals = mongoClient.resolveConfigurationIntervals(criteriaFilters);
        if (activationIntervals == null) {
            return ResolvedIntervals.reject("configuration selector resolution failed (database error)");
        }

        // union the activation intervals, then intersect with the query range → fragmented result
        // (an empty result here is a valid "selector matched no activations in range" outcome)
        return ResolvedIntervals.ok(TimeInterval.intersectAll(activationIntervals, queryRange));
    }

    /**
     * Result of resolving the {@link ConfigurationSelector}: either the effective retrieval intervals
     * (possibly empty = well-formed selector matched nothing in range) or a reject message (malformed
     * selector or database error). Exactly one of the two is non-null.
     */
    private static final class ResolvedIntervals {
        final List<TimeInterval> intervals;
        final String error;

        private ResolvedIntervals(List<TimeInterval> intervals, String error) {
            this.intervals = intervals;
            this.error = error;
        }

        static ResolvedIntervals ok(List<TimeInterval> intervals) {
            return new ResolvedIntervals(intervals, null);
        }

        static ResolvedIntervals reject(String error) {
            return new ResolvedIntervals(null, error);
        }
    }

    private static Bson configCriterionToFilter(ConfigurationSelector.Criterion criterion) {
        switch (criterion.getCriterionCase()) {
            case CONFIGURATIONNAMECRITERION -> {
                return com.mongodb.client.model.Filters.in(
                        BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME,
                        criterion.getConfigurationNameCriterion().getValuesList());
            }
            case CLIENTACTIVATIONIDCRITERION -> {
                return com.mongodb.client.model.Filters.in(
                        BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID,
                        criterion.getClientActivationIdCriterion().getValuesList());
            }
            case CATEGORYCRITERION -> {
                return com.mongodb.client.model.Filters.in(
                        BsonConstants.BSON_KEY_ACTIVATION_INTERNAL_CATEGORY,
                        criterion.getCategoryCriterion().getValuesList());
            }
            case TAGSCRITERION -> {
                return MongoQueryFilterBuilder.tagsFilter(criterion.getTagsCriterion().getValuesList());
            }
            case ATTRIBUTESCRITERION -> {
                final var c = criterion.getAttributesCriterion();
                return MongoQueryFilterBuilder.attributeFilter(c.getKey(), c.getValuesList());
            }
            default -> {
                return null;
            }
        }
    }
}
