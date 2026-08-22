package com.ospreydcs.dp.service.common.bson.samplestatus;

import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Sample-wise operations on SampleStatusBucketDocuments, shared by the save path (carving
 * colliding timestamps out of existing documents to maintain the no-duplicate-identity-key
 * storage invariant) and the delete path (trimming/splitting boundary documents so deletion is
 * exact at the sample axis).
 *
 * <p>Documents are expanded to per-sample points keyed by epoch-nanos timestamps, filtered, and
 * rebuilt. A rebuilt run of two or more evenly spaced points re-emits as a SamplingClock axis
 * (so trimming a clock document yields a shorter clock, and an interior delete splits it into
 * two clock documents); anything else re-emits as a TimestampList.
 */
public class SampleStatusDocumentUtility {

    /** One sample status expanded from a document: companions are null when the document has none. */
    public record StatusPoint(long timestampNanos, int statusCode, Float confidence, String reason) {}

    /**
     * Outcome of removing samples from a document: the documents that replace it (empty when no
     * samples survive), and the number of samples removed. A removedCount of zero means the
     * document was untouched and must not be rewritten.
     */
    public record RemovalResult(List<SampleStatusBucketDocument> replacementDocuments, long removedCount) {

        public static final RemovalResult UNTOUCHED = new RemovalResult(List.of(), 0);
    }

    public static long timestampNanos(Timestamp timestamp) {
        return timestamp.getEpochSeconds() * 1_000_000_000L + timestamp.getNanoseconds();
    }

    public static Timestamp timestampFromNanos(long epochNanos) {
        return Timestamp.newBuilder()
                .setEpochSeconds(Math.floorDiv(epochNanos, 1_000_000_000L))
                .setNanoseconds(Math.floorMod(epochNanos, 1_000_000_000L))
                .build();
    }

    /**
     * Returns the epoch-nanos timestamp of each sample specified by a DataTimestamps axis, in
     * axis order.
     */
    public static List<Long> timestampNanosList(DataTimestamps dataTimestamps) {

        final List<Long> result = new ArrayList<>();

        switch (dataTimestamps.getValueCase()) {
            case SAMPLINGCLOCK -> {
                final SamplingClock clock = dataTimestamps.getSamplingClock();
                final long startNanos = timestampNanos(clock.getStartTime());
                for (int i = 0; i < clock.getCount(); i++) {
                    result.add(startNanos + i * clock.getPeriodNanos());
                }
            }
            case TIMESTAMPLIST -> {
                for (Timestamp timestamp : dataTimestamps.getTimestampList().getTimestampsList()) {
                    result.add(timestampNanos(timestamp));
                }
            }
            default -> {
            }
        }

        return result;
    }

    /**
     * Expands a document to its per-sample points. A stored document whose arrays do not line up
     * with its time axis is malformed and surfaces as DpException per the bucket-deserialization
     * contract.
     */
    public static List<StatusPoint> expandDocument(SampleStatusBucketDocument document) throws DpException {

        if (document.getDataTimestamps() == null) {
            throw new DpException(
                    "SampleStatusBucketDocument missing dataTimestamps for document with id: " + document.getId());
        }
        final List<Long> timestamps = timestampNanosList(document.getDataTimestamps().toDataTimestamps());

        final List<Integer> statusCodes = document.getStatusCodes();
        if (statusCodes == null || statusCodes.size() != timestamps.size()) {
            throw new DpException(
                    "SampleStatusBucketDocument statusCodes size does not match timestamp count for document with id: "
                            + document.getId());
        }
        final List<Float> confidence = document.getConfidence();
        if (confidence != null && confidence.size() != timestamps.size()) {
            throw new DpException(
                    "SampleStatusBucketDocument confidence size does not match timestamp count for document with id: "
                            + document.getId());
        }
        final List<String> reasons = document.getReasons();
        if (reasons != null && reasons.size() != timestamps.size()) {
            throw new DpException(
                    "SampleStatusBucketDocument reasons size does not match timestamp count for document with id: "
                            + document.getId());
        }

        final List<StatusPoint> points = new ArrayList<>(timestamps.size());
        for (int i = 0; i < timestamps.size(); i++) {
            points.add(new StatusPoint(
                    timestamps.get(i),
                    statusCodes.get(i),
                    confidence == null ? null : confidence.get(i),
                    reasons == null ? null : reasons.get(i)));
        }
        return points;
    }

    /**
     * Removes the samples whose timestamps appear in the given set (exact nanosecond equality).
     * Used by the save path to carve identity-key collisions out of existing documents before
     * inserting the incoming column.
     */
    public static RemovalResult removeTimestamps(
            SampleStatusBucketDocument document,
            Set<Long> timestampNanosToRemove
    ) throws DpException {

        final List<StatusPoint> points = expandDocument(document);
        final List<StatusPoint> survivors = new ArrayList<>(points.size());
        for (StatusPoint point : points) {
            if (!timestampNanosToRemove.contains(point.timestampNanos())) {
                survivors.add(point);
            }
        }

        final long removedCount = points.size() - survivors.size();
        if (removedCount == 0) {
            return RemovalResult.UNTOUCHED;
        }
        if (survivors.isEmpty()) {
            return new RemovalResult(List.of(), removedCount);
        }
        return new RemovalResult(List.of(buildReplacementDocument(document, survivors)), removedCount);
    }

    /**
     * Removes the samples whose timestamps fall within the half-open range
     * [beginNanos, endNanos). Survivors before and after the range become separate replacement
     * documents, so an interior deletion splits the document in two and neither replacement spans
     * the deleted gap.
     */
    public static RemovalResult removeRange(
            SampleStatusBucketDocument document,
            long beginNanos,
            long endNanos
    ) throws DpException {

        final List<StatusPoint> points = expandDocument(document);
        final List<StatusPoint> beforeRange = new ArrayList<>();
        final List<StatusPoint> afterRange = new ArrayList<>();
        long removedCount = 0;
        for (StatusPoint point : points) {
            if (point.timestampNanos() < beginNanos) {
                beforeRange.add(point);
            } else if (point.timestampNanos() >= endNanos) {
                afterRange.add(point);
            } else {
                removedCount++;
            }
        }

        if (removedCount == 0) {
            return RemovalResult.UNTOUCHED;
        }
        final List<SampleStatusBucketDocument> replacements = new ArrayList<>(2);
        if (!beforeRange.isEmpty()) {
            replacements.add(buildReplacementDocument(document, beforeRange));
        }
        if (!afterRange.isEmpty()) {
            replacements.add(buildReplacementDocument(document, afterRange));
        }
        return new RemovalResult(replacements, removedCount);
    }

    /**
     * Builds the DataTimestamps axis for a run of points: two or more evenly spaced points
     * re-emit as a SamplingClock, anything else as a TimestampList.
     */
    public static DataTimestamps buildDataTimestamps(List<StatusPoint> points) {

        if (points.size() >= 2) {
            final long period = points.get(1).timestampNanos() - points.get(0).timestampNanos();
            boolean evenlySpaced = true;
            for (int i = 2; i < points.size(); i++) {
                if (points.get(i).timestampNanos() - points.get(i - 1).timestampNanos() != period) {
                    evenlySpaced = false;
                    break;
                }
            }
            if (evenlySpaced) {
                return DataTimestamps.newBuilder()
                        .setSamplingClock(SamplingClock.newBuilder()
                                .setStartTime(timestampFromNanos(points.get(0).timestampNanos()))
                                .setPeriodNanos(period)
                                .setCount(points.size()))
                        .build();
            }
        }

        final TimestampList.Builder listBuilder = TimestampList.newBuilder();
        for (StatusPoint point : points) {
            listBuilder.addTimestamps(timestampFromNanos(point.timestampNanos()));
        }
        return DataTimestamps.newBuilder().setTimestampList(listBuilder).build();
    }

    /**
     * Builds a replacement document holding the given surviving points. Identity and provenance
     * fields are copied from the original: the delete path preserves them as-is, and the save
     * path overwrites provenance with the incoming request's before inserting (rewritten
     * documents take the incoming source/modifiedBy and a fresh updatedTime).
     */
    private static SampleStatusBucketDocument buildReplacementDocument(
            SampleStatusBucketDocument original,
            List<StatusPoint> points
    ) {
        final SampleStatusBucketDocument document = new SampleStatusBucketDocument();

        document.setPvName(original.getPvName());
        document.setDomain(original.getDomain());
        document.setLayer(original.getLayer());
        document.setSource(original.getSource());
        document.setModifiedBy(original.getModifiedBy());
        document.setUpdatedTime(original.getUpdatedTime());

        document.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(buildDataTimestamps(points)));
        document.setFirstTimeNanos(points.get(0).timestampNanos());
        document.setLastTimeNanos(points.get(points.size() - 1).timestampNanos());

        final List<Integer> statusCodes = new ArrayList<>(points.size());
        for (StatusPoint point : points) {
            statusCodes.add(point.statusCode());
        }
        document.setStatusCodes(statusCodes);

        // companion presence is homogeneous within a document, so probe the original's arrays
        if (original.getConfidence() != null) {
            final List<Float> confidence = new ArrayList<>(points.size());
            for (StatusPoint point : points) {
                confidence.add(point.confidence());
            }
            document.setConfidence(confidence);
        }
        if (original.getReasons() != null) {
            final List<String> reasons = new ArrayList<>(points.size());
            for (StatusPoint point : points) {
                reasons.add(point.reason());
            }
            document.setReasons(reasons);
        }

        return document;
    }
}
