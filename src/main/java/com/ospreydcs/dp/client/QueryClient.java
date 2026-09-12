package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.criteria.AttributeCriterion;
import com.ospreydcs.dp.client.criteria.ClientCriteria;
import com.ospreydcs.dp.client.criteria.TextMatch;
import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.QueryBucketsApiResult;
import com.ospreydcs.dp.client.result.QueryProvidersApiResult;
import com.ospreydcs.dp.client.result.QueryPvStatsApiResult;
import com.ospreydcs.dp.client.result.QuerySamplesApiResult;
import com.ospreydcs.dp.client.result.QueryTableApiResult;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.*;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QueryClient extends ServiceApiClientBase {

    public record QueryTableRequestParams(
            QueryTableRequest.TableResultFormat tableResultFormat,
            List<String> pvNameList,
            String pvNamePattern,
            Long beginTimeSeconds,
            Long beginTimeNanos,
            Long endTimeSeconds,
            Long endTimeNanos
    ) {
    }

    public static class QueryTableResponseObserver
            extends ApiResponseObserverBase<QueryTableResponse> {

        private final List<QueryTableResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryTableResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryTableResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryTableResponse response) {
            responseList.add(response);
            return true;
        }

        public QueryTableResponse getQueryResponse() {
            if (responseList.isEmpty()) {
                return null;
            } else {
                return responseList.get(0);
            }
        }
    }

    public static class QueryPvStatsResponseObserver
            extends ApiResponseObserverBase<QueryPvStatsResponse> {

        private final List<QueryPvStatsResponse> responseList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryPvStatsResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryPvStatsResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryPvStatsResponse response) {
            responseList.add(response);
            return true;
        }

        public QueryPvStatsResponse getResponse() {
            if (responseList.isEmpty()) {
                return null;
            } else {
                return responseList.get(0);
            }
        }
    }

    public static class QueryProvidersRequestParams {

        public String idCriterion = null;
        public String textCriterion = null;
        public String tagsCriterion = null;
        public String attributesCriterionKey = null;
        public String attributesCriterionValue = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setTextCriterion(String textCriterion) {
            this.textCriterion = textCriterion;
        }

        public void setTagsCriterion(String tagsCriterion) {
            this.tagsCriterion = tagsCriterion;
        }

        public void setAttributesCriterion(String attributeCriterionKey, String attributeCriterionValue) {
            this.attributesCriterionKey = attributeCriterionKey;
            this.attributesCriterionValue = attributeCriterionValue;
        }
    }

    public static class QueryProvidersResponseObserver
            extends ApiResponseObserverBase<QueryProvidersResponse> {

        private final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfoList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryProvidersResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryProvidersResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryProvidersResponse response) {

            if (!response.hasProvidersResult()) {
                recordFailure(observerName() + " response does not contain ProvidersResult");
                return false;
            }

            providerInfoList.addAll(response.getProvidersResult().getProviderInfosList());
            return true;
        }

        public List<QueryProvidersResponse.ProvidersResult.ProviderInfo> getProviderInfoList() {
            return providerInfoList;
        }
    }

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public QueryClient(ManagedChannel channel) {
        super(channel);
    }

    public static QueryTableRequest buildQueryTableRequest(QueryTableRequestParams params) {

        QueryTableRequest.Builder requestBuilder = QueryTableRequest.newBuilder();

        // set format
        if (params.tableResultFormat != null) {
            requestBuilder.setFormat(params.tableResultFormat);
        }

        // set pvNameList or PvNamePattern
        if (params.pvNameList != null && !params.pvNameList.isEmpty()) {
            PvNameList pvNameList = PvNameList.newBuilder()
                    .addAllPvNames(params.pvNameList)
                    .build();
            requestBuilder.setPvNameList(pvNameList);
        } else if (params.pvNamePattern != null && !params.pvNamePattern.isBlank()) {
            PvNamePattern pvNamePattern = PvNamePattern.newBuilder()
                    .setPattern(params.pvNamePattern)
                    .build();
            requestBuilder.setPvNamePattern(pvNamePattern);
        }

        // set begin time
        if (params.beginTimeSeconds != null) {
            final Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(params.beginTimeSeconds);
            if (params.beginTimeNanos != null) beginTimeBuilder.setNanoseconds(params.beginTimeNanos);
            beginTimeBuilder.build();
            requestBuilder.setBeginTime(beginTimeBuilder);
        }

        // set end time
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            requestBuilder.setEndTime(endTimeBuilder);
        }

        return requestBuilder.build();
    }

    public QueryTableApiResult sendQueryTable(QueryTableRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTableResponseObserver responseObserver = new QueryTableResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryTableApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryTableApiResult(responseObserver.getQueryResponse());
        }
    }

    public QueryTableApiResult queryTable(
            QueryTableRequestParams params
    ) {
        final QueryTableRequest request = buildQueryTableRequest(params);
        return sendQueryTable(request);
    }

    public static QueryPvStatsRequest buildQueryPvStatsRequest(List<String> pvNames) {

        QueryPvStatsRequest.Builder requestBuilder = QueryPvStatsRequest.newBuilder();

        PvNameList.Builder pvNameListBuilder = PvNameList.newBuilder();
        pvNameListBuilder.addAllPvNames(pvNames);
        pvNameListBuilder.build();

        requestBuilder.setPvNameList(pvNameListBuilder);
        return requestBuilder.build();
    }

    public static QueryPvStatsRequest buildQueryPvStatsRequest(String columnNamePattern) {

        QueryPvStatsRequest.Builder requestBuilder = QueryPvStatsRequest.newBuilder();

        PvNamePattern.Builder pvNamePatternBuilder = PvNamePattern.newBuilder();
        pvNamePatternBuilder.setPattern(columnNamePattern);
        pvNamePatternBuilder.build();

        requestBuilder.setPvNamePattern(pvNamePatternBuilder);
        return requestBuilder.build();
    }

    public QueryPvStatsApiResult sendQueryPvStats(
            QueryPvStatsRequest request
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryPvStatsResponseObserver responseObserver = new QueryPvStatsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryPvStats(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryPvStatsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryPvStatsApiResult(responseObserver.getResponse());
        }
    }

    public QueryPvStatsApiResult queryPvStats(
            List<String> columnNames
    ) {
        final QueryPvStatsRequest request = buildQueryPvStatsRequest(columnNames);
        return sendQueryPvStats(request);
    }

    public QueryPvStatsApiResult queryPvStats(
            String columnNamePattern
    ) {
        final QueryPvStatsRequest request = buildQueryPvStatsRequest(columnNamePattern);
        return sendQueryPvStats(request);
    }
    
    public static QueryProvidersRequest buildQueryProvidersRequest(QueryProvidersRequestParams params) {

        QueryProvidersRequest.Builder requestBuilder = QueryProvidersRequest.newBuilder();

        if (params.idCriterion != null) {
            QueryProvidersRequest.Criterion.IdCriterion criterion =
                    QueryProvidersRequest.Criterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setIdCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.textCriterion != null) {
            QueryProvidersRequest.Criterion.TextCriterion criterion =
                    QueryProvidersRequest.Criterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTextCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.tagsCriterion != null) {
            QueryProvidersRequest.Criterion.TagsCriterion criterion =
                    QueryProvidersRequest.Criterion.TagsCriterion.newBuilder()
                            .setTagValue(params.tagsCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTagsCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.attributesCriterionKey != null && params.attributesCriterionValue != null) {
            QueryProvidersRequest.Criterion.AttributesCriterion criterion =
                    QueryProvidersRequest.Criterion.AttributesCriterion.newBuilder()
                            .setKey(params.attributesCriterionKey)
                            .setValue(params.attributesCriterionValue)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setAttributesCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        return requestBuilder.build();
    }

    public QueryProvidersApiResult sendQueryProviders(
            QueryProvidersRequest request
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryProvidersResponseObserver responseObserver = new QueryProvidersResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviders(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryProvidersApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryProvidersApiResult(responseObserver.getProviderInfoList());
        }
    }

    public QueryProvidersApiResult queryProviders(
            QueryProvidersRequestParams queryParams
    ) {
        final QueryProvidersRequest request = buildQueryProvidersRequest(queryParams);
        return sendQueryProviders(request);
    }

    // =========================================================
    // Query API V2 (queryBuckets / queryBucketsStream / querySamples / querySamplesStream)
    // =========================================================

    /**
     * Selects which PVs a Query API V2 request covers.
     *
     * <p>Modeled as a sealed interface rather than a record with three nullable fields because
     * {@code PvSelector} is a strict proto oneof: exactly one arm may be set, and the server
     * rejects an unset selector.  A record with three nullable fields makes the invalid
     * combination representable, and the only precedent for resolving it in this class —
     * {@link #buildQueryTableRequest} — silently prefers {@code pvNameList} over {@code
     * pvNamePattern} when both are supplied, which hands the caller a query they did not ask for
     * with no diagnostic.  With a sealed interface the invalid combination does not compile.
     */
    public sealed interface PvSelectorParams
            permits PvNameListSelector, PvNamePatternSelector, PvMetadataSelector {
    }

    /**
     * Selects an explicit list of PV names.  An empty list is rejected by the server.
     */
    public record PvNameListSelector(List<String> pvNames) implements PvSelectorParams {
    }

    /**
     * Selects PVs whose names match a regular expression.  A blank pattern is rejected by the
     * server; use {@code ".*"} to select every PV in the archive.
     */
    public record PvNamePatternSelector(String pattern) implements PvSelectorParams {
    }

    /**
     * Selects PVs by metadata, mirroring the query language of {@code queryPvMetadata()}.
     * Criteria are ANDed; values within a criterion are ORed.  A null or empty field contributes
     * no criterion, and blank values are dropped before the request is built (see {@link
     * ClientCriteria#nonBlank}).
     *
     * <p><strong>An all-empty selector is not an error — it matches every PV in the archive.</strong>
     * This is the opposite of {@code configurationSelector}, whose empty form the server rejects
     * (see {@link QuerySpecParams}).  Because a selector built entirely from unfilled optional
     * fields resolves to a scan of the whole archive rather than a rejection, prefer stating an
     * all-PV query explicitly as a {@link PvNamePatternSelector} of {@code ".*"}, which is visible
     * in the request.
     */
    public record PvMetadataSelector(
            TextMatch pvName,
            TextMatch aliases,
            List<String> tagsAnyOf,
            List<AttributeCriterion> attributes
    ) implements PvSelectorParams {
    }

    /**
     * A single non-temporal configuration-matching criterion for {@code configurationSelector}.
     * Exactly one arm should be populated per instance; a criterion with no usable arm is dropped
     * when the request is built, and one with more than one arm populated emits the first
     * populated arm in declaration order.
     *
     * <p>Time is deliberately not part of a criterion: {@link QuerySpecParams}'s time range is the
     * single time axis for the whole query.
     */
    public record ConfigurationCriterion(
            List<String> configurationNameAnyOf,
            List<String> clientActivationIdAnyOf,
            List<String> categoryAnyOf,
            List<String> tagsAnyOf,
            AttributeCriterion attribute
    ) {
    }

    /**
     * The logical Query API V2 query — the "WHERE" clause — shared by all four V2 methods.
     *
     * <p>{@code beginTime} and {@code endTime} describe the half-open interval
     * {@code [beginTime, endTime)} and are required by the server, as is {@code pvSelector}.
     * {@code configurationCriteria} is optional: when it is null, empty, or contains no usable
     * criterion, the built request carries <strong>no</strong> {@code configurationSelector} at
     * all, which is how "no configuration restriction" is expressed.
     *
     * <p><strong>Do not attempt to express "no restriction" as an empty criteria list.</strong>
     * The server rejects a {@code ConfigurationSelector} whose criteria list is empty — deliberately,
     * so that a half-built selector is loud rather than silently returning an empty result. The
     * builder therefore drops the whole selector rather than emitting an empty one.
     *
     * <p>{@code sampleStatusSelector} is absent here by design: the server rejects it on the
     * bucket-oriented methods, so it lives on {@link QuerySamplesParams} only.
     */
    public record QuerySpecParams(
            Timestamp beginTime,
            Timestamp endTime,
            PvSelectorParams pvSelector,
            List<ConfigurationCriterion> configurationCriteria
    ) {
    }

    /**
     * Restricts returned samples by sample status.  Supported by the sample-oriented methods only;
     * the server rejects it on {@code queryBuckets()} / {@code queryBucketsStream()}, which is why
     * {@link QueryBucketsParams} does not carry it.
     *
     * <p>{@code domain} and {@code mode} are required.  An empty {@code layers} list selects every
     * layer in the domain; an empty {@code statusCodes} list matches a status with any code.
     */
    public record SampleStatusSelectorParams(
            String domain,
            List<String> layers,
            List<Integer> statusCodes,
            SampleStatusSelector.Mode mode
    ) {
    }

    /**
     * Builds the {@code QuerySpec} shared by all four Query API V2 methods.
     *
     * <p>Performs no validation of its own — every rule (required time range, required PV
     * selector, non-empty name list, non-blank pattern) is the server's, and a client-side copy
     * would drift from it.  What it does do is drop values that would turn an omitted filter into
     * a wrong answer or an avoidable rejection: blank criterion values (issue #243) and an
     * all-empty {@code configurationSelector}.
     */
    public static QuerySpec buildQuerySpec(QuerySpecParams params) {

        final QuerySpec.Builder specBuilder = QuerySpec.newBuilder();

        // time range
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        if (params.beginTime() != null) {
            timeRangeBuilder.setBeginTime(params.beginTime());
        }
        if (params.endTime() != null) {
            timeRangeBuilder.setEndTime(params.endTime());
        }
        specBuilder.setTimeRange(timeRangeBuilder);

        // pv selector -- exhaustive over the sealed hierarchy, so a new arm is a compile error
        // here rather than a silently unselected PV set
        if (params.pvSelector() != null) {
            specBuilder.setPvSelector(buildPvSelector(params.pvSelector()));
        }

        // configuration selector.  Built into a local builder first so that the selector can be
        // dropped entirely when no criterion survives: an EMPTY ConfigurationSelector is rejected
        // by the server, so emitting one would turn "the caller supplied no configuration filter"
        // into a failed request rather than an unrestricted query.  This is the same reasoning
        // buildQueryPvMetadataRequest records for its criteria.
        final ConfigurationSelector.Builder configSelectorBuilder = ConfigurationSelector.newBuilder();
        if (params.configurationCriteria() != null) {
            for (ConfigurationCriterion criterion : params.configurationCriteria()) {
                if (criterion == null) {
                    continue;
                }
                final ConfigurationSelector.Criterion built = buildConfigurationCriterion(criterion);
                if (built != null) {
                    configSelectorBuilder.addCriteria(built);
                }
            }
        }
        if (configSelectorBuilder.getCriteriaCount() > 0) {
            specBuilder.setConfigurationSelector(configSelectorBuilder);
        }

        return specBuilder.build();
    }

    private static PvSelector buildPvSelector(PvSelectorParams pvSelectorParams) {

        final PvSelector.Builder selectorBuilder = PvSelector.newBuilder();

        switch (pvSelectorParams) {

            case PvNameListSelector nameList -> {
                final PvNameList.Builder builder = PvNameList.newBuilder();
                if (nameList.pvNames() != null) {
                    // blanks are dropped here as everywhere else: a blank PV name matches nothing
                    // on this path rather than everything, but sending one is still an avoidable
                    // way to silently shrink the result
                    builder.addAllPvNames(ClientCriteria.nonBlank(nameList.pvNames()));
                }
                selectorBuilder.setPvNameList(builder);
            }

            case PvNamePatternSelector pattern -> {
                final PvNamePattern.Builder builder = PvNamePattern.newBuilder();
                if (pattern.pattern() != null) {
                    builder.setPattern(pattern.pattern());
                }
                selectorBuilder.setPvNamePattern(builder);
            }

            case PvMetadataSelector metadata -> selectorBuilder.setMetadataQuery(
                    buildMetadataQuery(metadata));
        }

        return selectorBuilder.build();
    }

    private static PvSelector.MetadataQuery buildMetadataQuery(PvMetadataSelector params) {

        final PvSelector.MetadataQuery.Builder queryBuilder = PvSelector.MetadataQuery.newBuilder();

        // pvName criterion -- omitted entirely when it carries no non-blank value, since a
        // criterion with all three lists empty is rejected by the server
        if (params.pvName() != null && !params.pvName().isEmpty()) {
            final PvSelector.MetadataQuery.Criterion.PvNameCriterion.Builder criterionBuilder =
                    PvSelector.MetadataQuery.Criterion.PvNameCriterion.newBuilder();
            criterionBuilder.addAllExact(ClientCriteria.nonBlank(params.pvName().exact()));
            criterionBuilder.addAllPrefix(ClientCriteria.nonBlank(params.pvName().prefix()));
            criterionBuilder.addAllContains(ClientCriteria.nonBlank(params.pvName().contains()));
            queryBuilder.addCriteria(PvSelector.MetadataQuery.Criterion.newBuilder()
                    .setPvNameCriterion(criterionBuilder));
        }

        // aliases criterion
        if (params.aliases() != null && !params.aliases().isEmpty()) {
            final PvSelector.MetadataQuery.Criterion.AliasesCriterion.Builder criterionBuilder =
                    PvSelector.MetadataQuery.Criterion.AliasesCriterion.newBuilder();
            criterionBuilder.addAllExact(ClientCriteria.nonBlank(params.aliases().exact()));
            criterionBuilder.addAllPrefix(ClientCriteria.nonBlank(params.aliases().prefix()));
            criterionBuilder.addAllContains(ClientCriteria.nonBlank(params.aliases().contains()));
            queryBuilder.addCriteria(PvSelector.MetadataQuery.Criterion.newBuilder()
                    .setAliasesCriterion(criterionBuilder));
        }

        // tags criterion
        final List<String> tags = ClientCriteria.nonBlank(params.tagsAnyOf());
        if (!tags.isEmpty()) {
            queryBuilder.addCriteria(PvSelector.MetadataQuery.Criterion.newBuilder()
                    .setTagsCriterion(PvSelector.MetadataQuery.Criterion.TagsCriterion.newBuilder()
                            .addAllValues(tags)));
        }

        // attributes criteria -- one criterion per entry; a blank key is an avoidable rejection
        if (params.attributes() != null) {
            for (AttributeCriterion attribute : params.attributes()) {
                if (attribute == null || ClientCriteria.isBlankKey(attribute.key())) {
                    continue;
                }
                final PvSelector.MetadataQuery.Criterion.AttributesCriterion.Builder criterionBuilder =
                        PvSelector.MetadataQuery.Criterion.AttributesCriterion.newBuilder()
                                .setKey(attribute.key());
                criterionBuilder.addAllValues(ClientCriteria.nonBlank(attribute.values()));
                queryBuilder.addCriteria(PvSelector.MetadataQuery.Criterion.newBuilder()
                        .setAttributesCriterion(criterionBuilder));
            }
        }

        return queryBuilder.build();
    }

    /**
     * Builds one {@code ConfigurationSelector.Criterion}, or returns null when the supplied
     * criterion carries no usable value.  Returning null rather than an empty criterion matters:
     * the server rejects a criterion with no arm set, so a criterion built from unfilled optional
     * fields must be dropped by the caller rather than sent.
     */
    private static ConfigurationSelector.Criterion buildConfigurationCriterion(
            ConfigurationCriterion criterion
    ) {
        final ConfigurationSelector.Criterion.Builder builder =
                ConfigurationSelector.Criterion.newBuilder();

        final List<String> names = ClientCriteria.nonBlank(criterion.configurationNameAnyOf());
        if (!names.isEmpty()) {
            return builder.setConfigurationNameCriterion(
                    ConfigurationSelector.Criterion.ConfigurationNameCriterion.newBuilder()
                            .addAllValues(names)).build();
        }

        final List<String> activationIds = ClientCriteria.nonBlank(criterion.clientActivationIdAnyOf());
        if (!activationIds.isEmpty()) {
            return builder.setClientActivationIdCriterion(
                    ConfigurationSelector.Criterion.ClientActivationIdCriterion.newBuilder()
                            .addAllValues(activationIds)).build();
        }

        final List<String> categories = ClientCriteria.nonBlank(criterion.categoryAnyOf());
        if (!categories.isEmpty()) {
            return builder.setCategoryCriterion(
                    ConfigurationSelector.Criterion.CategoryCriterion.newBuilder()
                            .addAllValues(categories)).build();
        }

        final List<String> tags = ClientCriteria.nonBlank(criterion.tagsAnyOf());
        if (!tags.isEmpty()) {
            return builder.setTagsCriterion(
                    ConfigurationSelector.Criterion.TagsCriterion.newBuilder()
                            .addAllValues(tags)).build();
        }

        final AttributeCriterion attribute = criterion.attribute();
        if (attribute != null && !ClientCriteria.isBlankKey(attribute.key())) {
            return builder.setAttributesCriterion(
                    ConfigurationSelector.Criterion.AttributesCriterion.newBuilder()
                            .setKey(attribute.key())
                            .addAllValues(ClientCriteria.nonBlank(attribute.values()))).build();
        }

        return null;
    }

    private static SampleStatusSelector buildSampleStatusSelector(SampleStatusSelectorParams params) {

        final SampleStatusSelector.Builder builder = SampleStatusSelector.newBuilder();
        if (params.domain() != null) {
            builder.setDomain(params.domain());
        }
        if (params.layers() != null) {
            builder.addAllLayers(ClientCriteria.nonBlank(params.layers()));
        }
        if (params.statusCodes() != null) {
            for (Integer statusCode : params.statusCodes()) {
                if (statusCode != null) {
                    builder.addStatusCodes(statusCode);
                }
            }
        }
        if (params.mode() != null) {
            builder.setMode(params.mode());
        }
        return builder.build();
    }

    /**
     * Builds the {@code ExecutionOptions} for a V2 request.  {@code limit} is set only when
     * positive and {@code pageToken} only when non-blank, so that an unset field is genuinely
     * absent from the request rather than present as a proto default.
     */
    private static ExecutionOptions buildExecutionOptions(int limit, String pageToken) {

        final ExecutionOptions.Builder builder = ExecutionOptions.newBuilder();
        if (limit > 0) {
            builder.setLimit(limit);
        }
        if (pageToken != null && !pageToken.isBlank()) {
            builder.setPageToken(pageToken);
        }
        return builder.build();
    }

    /**
     * Parameters for {@link #querySamples} and {@link #querySamplesStream}.
     *
     * <p><strong>Column metadata is never returned by either method.</strong>  It is not merely
     * defaulted on — the sample assembly path carries no column metadata at all, so {@code
     * ResultRepresentation.excludeColumnMetadata} is inert here and is deliberately not exposed on
     * this params type.  Use {@link #queryBuckets} or {@code AnnotationClient.queryPvMetadata()}
     * when metadata is needed.
     *
     * <p><strong>{@code limit} counts timestamps (rows), not buckets.</strong>  Unset or zero
     * selects the server default (10,000 rows); a value above the server maximum (100,000) is
     * <em>silently clamped</em> rather than rejected.  A page is bounded by whichever of the row
     * limit and the outgoing message byte budget trips first.
     *
     * <p><strong>A small {@code limit} costs server work without saving any.</strong>  The
     * underlying Mongo retrieval is not limited; the server drains buckets until the byte budget
     * trips and then truncates the assembled table to {@code limit} rows.  Leave {@code limit}
     * unset unless the caller needs small pages for a specific reason.
     *
     * <p>{@code pageToken} continues a unary query from a prior result's {@code nextPageToken} and
     * <strong>must be empty for the streaming method</strong> — the server rejects a non-empty
     * token on a streaming call, so {@link #buildQuerySamplesStreamRequest} drops it.
     *
     * <p><strong>Do not reuse a page token across queries.</strong>  A token encodes a position
     * only.  Nothing binds it to the query that produced it beyond a coarse kind check that
     * separates bucket tokens from sample tokens — replaying a sample token against a different
     * {@code QuerySpec} produces a well-formed but semantically wrong result rather than an error.
     * A malformed token <em>is</em> rejected here, unlike the annotation metadata queries, which
     * silently reset to the first page.
     *
     * @param querySpec the logical query; required
     * @param sampleStatusSelector optional per-sample status filter, or null for none
     * @param limit rows per page (unary) or per streamed message (streaming); 0 = server default
     * @param pageToken continuation token for the unary method; ignored by the streaming method
     * @param useSerializedColumns return columns in serialized form to reduce gRPC overhead
     */
    public record QuerySamplesParams(
            QuerySpecParams querySpec,
            SampleStatusSelectorParams sampleStatusSelector,
            int limit,
            String pageToken,
            boolean useSerializedColumns
    ) {
    }

    /**
     * Parameters for {@link #queryBuckets} and {@link #queryBucketsStream}.
     *
     * <p>Unlike {@link QuerySamplesParams}, {@code excludeColumnMetadata} <strong>is</strong>
     * functional here: bucket results carry column metadata where it was stored, and this flag
     * suppresses it.
     *
     * <p>{@code sampleStatusSelector} is absent by design — the server rejects it on the
     * bucket-oriented methods, which return storage buckets whole and cannot represent per-sample
     * filtering.  Offering a field that can only ever produce a rejection would be worse than not
     * offering it.
     *
     * <p>{@code limit} counts DataBuckets here, not rows; paging boundaries always fall between
     * whole buckets.  The token caveats on {@link QuerySamplesParams} apply identically.
     */
    public record QueryBucketsParams(
            QuerySpecParams querySpec,
            int limit,
            String pageToken,
            boolean useSerializedColumns,
            boolean excludeColumnMetadata
    ) {
    }

    public static QuerySamplesRequest buildQuerySamplesRequest(QuerySamplesParams params) {
        return buildQuerySamplesRequest(params, true);
    }

    /**
     * Builds a {@code QuerySamplesRequest}, optionally suppressing the page token.
     *
     * @param includePageToken false for the streaming method, whose server-side contract rejects a
     *                         non-empty token
     */
    private static QuerySamplesRequest buildQuerySamplesRequest(
            QuerySamplesParams params, boolean includePageToken
    ) {
        final QuerySamplesRequest.Builder requestBuilder = QuerySamplesRequest.newBuilder();

        final QuerySpec.Builder specBuilder =
                (params.querySpec() != null
                        ? buildQuerySpec(params.querySpec())
                        : buildQuerySpec(new QuerySpecParams(null, null, null, null)))
                        .toBuilder();

        // sampleStatusSelector lives on the QuerySpec but is accepted only by the sample-oriented
        // methods, so it is applied here rather than in the shared spec builder
        if (params.sampleStatusSelector() != null) {
            specBuilder.setSampleStatusSelector(buildSampleStatusSelector(params.sampleStatusSelector()));
        }
        requestBuilder.setQuerySpec(specBuilder);

        requestBuilder.setExecutionOptions(buildExecutionOptions(
                params.limit(), includePageToken ? params.pageToken() : null));

        // excludeColumnMetadata is not set: it is inert on the samples path, and setting it would
        // suggest to a reader that it does something here
        requestBuilder.setResultRepresentation(ResultRepresentation.newBuilder()
                .setUseSerializedColumns(params.useSerializedColumns()));

        return requestBuilder.build();
    }

    /**
     * Builds the request for {@link #querySamplesStream}.  The page token is <strong>dropped</strong>:
     * streaming is fire-and-consume and the server rejects a non-empty token on a streaming call,
     * so forwarding a token the caller left in a params instance shared with the unary method
     * would be a guaranteed rejection.
     */
    public static QuerySamplesRequest buildQuerySamplesStreamRequest(QuerySamplesParams params) {
        return buildQuerySamplesRequest(params, false);
    }

    public static QueryBucketsRequest buildQueryBucketsRequest(QueryBucketsParams params) {
        return buildQueryBucketsRequest(params, true);
    }

    private static QueryBucketsRequest buildQueryBucketsRequest(
            QueryBucketsParams params, boolean includePageToken
    ) {
        final QueryBucketsRequest.Builder requestBuilder = QueryBucketsRequest.newBuilder();

        requestBuilder.setQuerySpec(params.querySpec() != null
                ? buildQuerySpec(params.querySpec())
                : buildQuerySpec(new QuerySpecParams(null, null, null, null)));

        requestBuilder.setExecutionOptions(buildExecutionOptions(
                params.limit(), includePageToken ? params.pageToken() : null));

        requestBuilder.setResultRepresentation(ResultRepresentation.newBuilder()
                .setUseSerializedColumns(params.useSerializedColumns())
                .setExcludeColumnMetadata(params.excludeColumnMetadata()));

        return requestBuilder.build();
    }

    /**
     * Builds the request for {@link #queryBucketsStream}.  The page token is dropped for the same
     * reason as {@link #buildQuerySamplesStreamRequest}.
     */
    public static QueryBucketsRequest buildQueryBucketsStreamRequest(QueryBucketsParams params) {
        return buildQueryBucketsRequest(params, false);
    }

    public static class QuerySamplesResponseObserver
            extends ApiResponseObserverBase<QuerySamplesResponse> {

        private final AtomicReference<ColumnTable> columnTable = new AtomicReference<>(null);
        private final AtomicReference<String> nextPageToken = new AtomicReference<>("");

        @Override
        protected boolean hasExceptionalResult(QuerySamplesResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QuerySamplesResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QuerySamplesResponse response) {
            if (!response.hasSampleQueryResult()) {
                recordFailure(observerName() + " response does not contain SampleQueryResult");
                return false;
            }
            columnTable.set(response.getSampleQueryResult().getColumnTable());
            nextPageToken.set(response.getSampleQueryResult().getNextPageToken());
            return true;
        }

        public ColumnTable getColumnTable() {
            return columnTable.get();
        }

        public String getNextPageToken() {
            return nextPageToken.get();
        }
    }

    public static class QueryBucketsResponseObserver
            extends ApiResponseObserverBase<QueryBucketsResponse> {

        private final List<DataBucket> dataBuckets = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<String> nextPageToken = new AtomicReference<>("");

        @Override
        protected boolean hasExceptionalResult(QueryBucketsResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryBucketsResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryBucketsResponse response) {
            if (!response.hasBucketQueryResult()) {
                recordFailure(observerName() + " response does not contain BucketQueryResult");
                return false;
            }
            dataBuckets.addAll(response.getBucketQueryResult().getDataBucketsList());
            nextPageToken.set(response.getBucketQueryResult().getNextPageToken());
            return true;
        }

        public List<DataBucket> getDataBuckets() {
            return dataBuckets;
        }

        public String getNextPageToken() {
            return nextPageToken.get();
        }
    }

    /**
     * Accumulates a {@code querySamplesStream} into a single {@link ColumnTable}.
     *
     * <p>Each streamed message is one page of the same logical table: its own slice of the
     * timestamp axis, and one column per resolved PV.  Accumulating them means concatenating the
     * timestamp lists and appending each page's values to the matching column.
     *
     * <p><strong>Columns are merged by name, never by position.</strong>  The server seeds a column
     * for every resolved PV on every page, so the column set is stable in practice — but merging by
     * index would silently mis-align an entire PV's values against the timestamp axis if that ever
     * stopped holding, which is a wrong answer rather than an error.  A page whose column set
     * differs from the first page's is therefore a hard failure: the accumulated table cannot be
     * built correctly, and reporting a partial one would be worse than reporting nothing.
     *
     * <p>Serialized columns ({@code ResultRepresentation.useSerializedColumns}) cannot be merged
     * without deserializing them, so a stream carrying them is accumulated as the raw per-page
     * serialized column lists concatenated, with the timestamp axis concatenated as usual.  Callers
     * using serialized columns on a streaming call should deserialize per page; the unary method is
     * the better fit when a single assembled table is wanted.
     */
    public static class QuerySamplesStreamResponseObserver
            implements StreamObserver<QuerySamplesResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<ApiResultStatus> apiResultStatus =
                new AtomicReference<>(ApiResultStatus.NONE);

        // accumulated state, guarded by the monitor of this observer
        private final List<Timestamp> timestamps = new ArrayList<>();
        private final List<String> columnNames = new ArrayList<>();
        private final Map<String, List<DataValue>> valuesByColumnName = new LinkedHashMap<>();
        private final List<SerializedDataColumn> serializedColumns = new ArrayList<>();
        private boolean columnSetEstablished = false;

        public void await() {
            try {
                if (!finishLatch.await(ApiResponseObserverBase.DEFAULT_AWAIT_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS)) {
                    recordFailure("QuerySamplesStreamResponseObserver timed out waiting for "
                            + "finishLatch", null);
                }
            } catch (InterruptedException e) {
                recordFailure("QuerySamplesStreamResponseObserver InterruptedException waiting for "
                        + "finishLatch", null);
                Thread.currentThread().interrupt();
            }
        }

        public boolean isError() {
            return isError.get();
        }

        public String getErrorMessage() {
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public ApiResultStatus getApiResultStatus() {
            return apiResultStatus.get();
        }

        private void recordFailure(String errorMsg, ApiResultStatus status) {
            if (status != null) {
                apiResultStatus.compareAndSet(ApiResultStatus.NONE, status);
            }
            isError.set(true);
            errorMessageList.add(errorMsg);
            finishLatch.countDown();
        }

        /**
         * Returns the accumulated table, or an empty table when the stream carried no message.
         */
        public synchronized ColumnTable getColumnTable() {

            final ColumnTable.Builder builder = ColumnTable.newBuilder();
            builder.setTimestampList(TimestampList.newBuilder().addAllTimestamps(timestamps));

            for (String columnName : columnNames) {
                builder.addDataColumns(DataColumn.newBuilder()
                        .setName(columnName)
                        .addAllDataValues(valuesByColumnName.get(columnName)));
            }
            builder.addAllSerializedDataColumns(serializedColumns);

            return builder.build();
        }

        @Override
        public void onNext(QuerySamplesResponse response) {

            if (response.hasExceptionalResult()) {
                final ExceptionalResult exceptionalResult = response.getExceptionalResult();
                // the service's message is recorded verbatim for the caller; this observer's
                // identity goes on the console line only.  See the message contract on
                // ApiResponseObserverBase.
                System.err.println("QuerySamplesStreamResponseObserver onNext received exceptional "
                        + "response: " + exceptionalResult.getMessage());
                recordFailure(
                        exceptionalResult.getMessage(),
                        ApiResultStatus.fromProto(exceptionalResult.getExceptionalResultStatus()));
                return;
            }

            if (!response.hasSampleQueryResult()) {
                recordFailure("QuerySamplesStreamResponseObserver response does not contain "
                        + "SampleQueryResult", null);
                return;
            }

            final ColumnTable page = response.getSampleQueryResult().getColumnTable();
            final String mergeError = accumulate(page);
            if (mergeError != null) {
                recordFailure("QuerySamplesStreamResponseObserver " + mergeError, null);
            }
        }

        /**
         * Merges one streamed page into the accumulated table.
         *
         * @return null on success, or a description of why the page could not be merged
         */
        private synchronized String accumulate(ColumnTable page) {

            final int pageRowCount = page.getTimestampList().getTimestampsCount();

            // establish the column set from the first page carrying columns, then require every
            // later page to match it.  Merging by name means a reordered page is fine; a page with
            // a different SET of columns is not, because the missing column's rows cannot be
            // filled in and appending the rest would mis-align that column against the axis.
            if (!page.getDataColumnsList().isEmpty()) {

                final List<String> pageColumnNames = page.getDataColumnsList().stream()
                        .map(DataColumn::getName)
                        .toList();

                if (!columnSetEstablished) {
                    columnSetEstablished = true;
                    for (String name : pageColumnNames) {
                        if (valuesByColumnName.containsKey(name)) {
                            return "received a page with duplicate column name \"" + name + "\"";
                        }
                        columnNames.add(name);
                        valuesByColumnName.put(name, new ArrayList<>());
                    }
                } else if (!Set.copyOf(pageColumnNames).equals(Set.copyOf(columnNames))
                        || pageColumnNames.size() != columnNames.size()) {
                    return "received a page whose column set differs from the first page's "
                            + "(first page: " + columnNames + ", this page: " + pageColumnNames
                            + "); the accumulated table cannot be aligned";
                }

                for (DataColumn column : page.getDataColumnsList()) {
                    if (column.getDataValuesCount() != pageRowCount) {
                        return "received a page whose column \"" + column.getName() + "\" holds "
                                + column.getDataValuesCount() + " values for " + pageRowCount
                                + " timestamps";
                    }
                    valuesByColumnName.get(column.getName()).addAll(column.getDataValuesList());
                }
            }

            serializedColumns.addAll(page.getSerializedDataColumnsList());
            timestamps.addAll(page.getTimestampList().getTimestampsList());

            return null;
        }

        @Override
        public void onError(Throwable t) {
            recordFailure("QuerySamplesStreamResponseObserver onError: " + Status.fromThrowable(t),
                    null);
        }

        @Override
        public void onCompleted() {
            finishLatch.countDown();
        }
    }

    /**
     * Accumulates a {@code queryBucketsStream} into a single bucket list.  Buckets are independent
     * objects, so accumulation is plain concatenation — there is no alignment to preserve and
     * therefore none of {@link QuerySamplesStreamResponseObserver}'s merge machinery.
     */
    public static class QueryBucketsStreamResponseObserver
            implements StreamObserver<QueryBucketsResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<ApiResultStatus> apiResultStatus =
                new AtomicReference<>(ApiResultStatus.NONE);
        private final List<DataBucket> dataBuckets = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                if (!finishLatch.await(ApiResponseObserverBase.DEFAULT_AWAIT_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS)) {
                    recordFailure("QueryBucketsStreamResponseObserver timed out waiting for "
                            + "finishLatch", null);
                }
            } catch (InterruptedException e) {
                recordFailure("QueryBucketsStreamResponseObserver InterruptedException waiting for "
                        + "finishLatch", null);
                Thread.currentThread().interrupt();
            }
        }

        public boolean isError() {
            return isError.get();
        }

        public String getErrorMessage() {
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public ApiResultStatus getApiResultStatus() {
            return apiResultStatus.get();
        }

        public List<DataBucket> getDataBuckets() {
            return dataBuckets;
        }

        private void recordFailure(String errorMsg, ApiResultStatus status) {
            if (status != null) {
                apiResultStatus.compareAndSet(ApiResultStatus.NONE, status);
            }
            isError.set(true);
            errorMessageList.add(errorMsg);
            finishLatch.countDown();
        }

        @Override
        public void onNext(QueryBucketsResponse response) {

            if (response.hasExceptionalResult()) {
                final ExceptionalResult exceptionalResult = response.getExceptionalResult();
                System.err.println("QueryBucketsStreamResponseObserver onNext received exceptional "
                        + "response: " + exceptionalResult.getMessage());
                recordFailure(
                        exceptionalResult.getMessage(),
                        ApiResultStatus.fromProto(exceptionalResult.getExceptionalResultStatus()));
                return;
            }

            if (!response.hasBucketQueryResult()) {
                recordFailure("QueryBucketsStreamResponseObserver response does not contain "
                        + "BucketQueryResult", null);
                return;
            }

            dataBuckets.addAll(response.getBucketQueryResult().getDataBucketsList());
        }

        @Override
        public void onError(Throwable t) {
            recordFailure("QueryBucketsStreamResponseObserver onError: " + Status.fromThrowable(t),
                    null);
        }

        @Override
        public void onCompleted() {
            finishLatch.countDown();
        }
    }

    public QuerySamplesApiResult sendQuerySamples(QuerySamplesRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QuerySamplesResponseObserver responseObserver = new QuerySamplesResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.querySamples(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QuerySamplesApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QuerySamplesApiResult(
                    responseObserver.getColumnTable(), responseObserver.getNextPageToken());
        }
    }

    /**
     * Unary sample-oriented query (Query API V2) with resumable paging.
     *
     * <p>Returns one page of aligned sample data as a column-oriented table.  Samples are trimmed
     * to the half-open interval {@code [beginTime, endTime)}; bucket boundaries are hidden.  An
     * empty result is a success carrying an empty table, not a rejection.
     *
     * <p><strong>Result shape.</strong>  Columns are bare PV names, sorted ascending and deduped.
     * Every resolved PV gets a column even when it has no data in the window.  There is exactly one
     * {@code DataValue} per column per {@code timestampList} entry; a PV with no sample at a given
     * timestamp has an <em>unset</em> {@code DataValue} value oneof at that position.  There is no
     * timestamp column — the axis lives only in {@code ColumnTable.timestampList}.
     *
     * <p><strong>Paging.</strong>  Pass a prior result's {@code nextPageToken} as {@code
     * params.pageToken} to continue; an empty {@code nextPageToken} indicates the last page.  A
     * page is bounded by whichever of {@code limit} rows and the outgoing message byte budget
     * (4,096,000 bytes by default) trips first.  The byte accounting measures sample values only —
     * not the timestamp list, per-column framing, names, or the response envelope — so a page can
     * <strong>overshoot the budget by up to one bucket</strong>.  Raise the client channel's
     * {@code maxInboundMessageSize} accordingly.
     *
     * <p><strong>A single oversized row is a hard error, not a page.</strong>  If the values for
     * one timestamp across all selected PVs exceed the byte budget, the request fails.  The
     * server's message suggests narrowing the PV set or the time range; only the former can help,
     * since the offending row is a single instant.
     *
     * <p><strong>A non-scalar PV is rejected mid-assembly, not pre-flight.</strong>  The rejection
     * fires when an array, image, struct or other non-scalar value is actually encountered, so a
     * non-scalar PV with no buckets in the requested window <em>passes silently</em>, and the same
     * PV set can succeed on one page and be rejected on the next.  Only the first offending PV is
     * named, and which one that is depends on bucket iteration order.
     *
     * <p><strong>A selector resolving to more than 10,000 PVs is rejected.</strong>
     *
     * <p>See {@link QuerySamplesParams} for the {@code limit}, page-token and column-metadata
     * caveats.
     */
    public QuerySamplesApiResult querySamples(QuerySamplesParams params) {
        final QuerySamplesRequest request = buildQuerySamplesRequest(params);
        return sendQuerySamples(request);
    }

    public QuerySamplesApiResult sendQuerySamplesStream(QuerySamplesRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QuerySamplesStreamResponseObserver responseObserver =
                new QuerySamplesStreamResponseObserver();

        new Thread(() -> {
            asyncStub.querySamplesStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QuerySamplesApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            // streaming is fire-and-consume: the table is the accumulated result of the whole
            // stream and there is no continuation token
            return new QuerySamplesApiResult(responseObserver.getColumnTable(), "");
        }
    }

    /**
     * Server-streaming sample-oriented query (Query API V2).
     *
     * <p>Streams to completion and accumulates every message into a single {@link ColumnTable},
     * concatenating the timestamp axis and merging columns <strong>by name</strong>.  The result's
     * {@code nextPageToken} is always empty — the stream itself signals completion.
     *
     * <p>{@code params.pageToken} is <strong>ignored</strong>: the server rejects a non-empty token
     * on a streaming call, so the builder drops it rather than forwarding a token left over from a
     * unary call sharing the same params instance.  Use {@link #querySamples} when resumable paging
     * is required.
     *
     * <p>{@code params.limit} means the per-message chunk size here, not a total cap.
     *
     * <p>Every result-shape and failure-mode caveat on {@link #querySamples} applies, with one
     * addition: a page whose column set differs from the first page's fails the call rather than
     * producing a mis-aligned table.
     */
    public QuerySamplesApiResult querySamplesStream(QuerySamplesParams params) {
        final QuerySamplesRequest request = buildQuerySamplesStreamRequest(params);
        return sendQuerySamplesStream(request);
    }

    public QueryBucketsApiResult sendQueryBuckets(QueryBucketsRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryBucketsResponseObserver responseObserver = new QueryBucketsResponseObserver();

        new Thread(() -> {
            asyncStub.queryBuckets(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryBucketsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryBucketsApiResult(
                    responseObserver.getDataBuckets(), responseObserver.getNextPageToken());
        }
    }

    /**
     * Unary bucket-oriented query (Query API V2) with resumable paging.
     *
     * <p>Returns one page of whole storage buckets.  Paging boundaries always fall <em>between</em>
     * buckets, so a returned bucket may carry samples outside the requested {@code TimeRange} —
     * unlike {@link #querySamples}, which trims to the range.  An empty result is a success
     * carrying an empty bucket list.
     *
     * <p><strong>Column metadata is returned here</strong>, where stored, unless {@code
     * params.excludeColumnMetadata} suppresses it.  This is the flag's only functional home: it is
     * inert on the sample-oriented methods.
     *
     * <p><strong>Non-scalar PVs are fine here.</strong>  Buckets carry their stored column type
     * unchanged, so the non-scalar rejection that applies to {@link #querySamples} has no analogue
     * on this path.
     *
     * <p>{@code params.limit} counts buckets.  The page-token caveats on {@link QuerySamplesParams}
     * apply identically — a token encodes a position only, and must not be replayed against a
     * different {@code QuerySpec}.
     */
    public QueryBucketsApiResult queryBuckets(QueryBucketsParams params) {
        final QueryBucketsRequest request = buildQueryBucketsRequest(params);
        return sendQueryBuckets(request);
    }

    public QueryBucketsApiResult sendQueryBucketsStream(QueryBucketsRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryBucketsStreamResponseObserver responseObserver =
                new QueryBucketsStreamResponseObserver();

        new Thread(() -> {
            asyncStub.queryBucketsStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryBucketsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            // streaming is fire-and-consume: the buckets are the accumulated result of the whole
            // stream and there is no continuation token
            return new QueryBucketsApiResult(responseObserver.getDataBuckets(), "");
        }
    }

    /**
     * Server-streaming bucket-oriented query (Query API V2).
     *
     * <p>Streams to completion and accumulates the buckets of every message into a single result;
     * {@code nextPageToken} is always empty.  {@code params.pageToken} is ignored for the same
     * reason as on {@link #querySamplesStream}, and {@code params.limit} is the per-message chunk
     * size.  Use {@link #queryBuckets} when resumable paging is required.
     */
    public QueryBucketsApiResult queryBucketsStream(QueryBucketsParams params) {
        final QueryBucketsRequest request = buildQueryBucketsStreamRequest(params);
        return sendQueryBucketsStream(request);
    }

}
