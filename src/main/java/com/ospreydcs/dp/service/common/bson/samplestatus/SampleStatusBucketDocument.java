package com.ospreydcs.dp.service.common.bson.samplestatus;

import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusBucket;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB document for the sampleStatusBuckets collection: statuses for one PV in one
 * (domain, layer) over a contiguous time period, paralleling BucketDocument for time-series data.
 *
 * <p>Storage invariant: no two documents ever assert a status for the same identity key
 * (pvName, timestamp, domain, layer). The save path maintains this by carving colliding
 * timestamps out of existing documents before inserting new ones (see
 * MongoSyncAnnotationClient.saveSampleStatuses()), which is what lets the query path return
 * documents whole with no read-time conflict resolution.
 *
 * <p>The time span is denormalized to firstTimeNanos/lastTimeNanos epoch-nanos scalars so the
 * overlap predicate is a plain indexed range comparison, avoiding the seconds/nanos $or
 * construction that defeats index bounds on the buckets collection (#198). Note that unlike data
 * buckets, sample status documents have no maximum span: sparse labeling over an arbitrarily wide
 * range is first-class, so overlap queries must not assume a bounded document span (no #197-style
 * firstTime lower bound).
 */
public class SampleStatusBucketDocument {

    // instance variables
    private ObjectId id;
    private String pvName;
    private String domain;
    private String layer;
    private DataTimestampsDocument dataTimestamps;
    private long firstTimeNanos;
    private long lastTimeNanos;
    private List<Integer> statusCodes;
    private List<Float> confidence;
    private List<String> reasons;
    private String source;
    private String modifiedBy;
    private Instant updatedTime;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public long getFirstTimeNanos() {
        return firstTimeNanos;
    }

    public void setFirstTimeNanos(long firstTimeNanos) {
        this.firstTimeNanos = firstTimeNanos;
    }

    public long getLastTimeNanos() {
        return lastTimeNanos;
    }

    public void setLastTimeNanos(long lastTimeNanos) {
        this.lastTimeNanos = lastTimeNanos;
    }

    public List<Integer> getStatusCodes() {
        return statusCodes;
    }

    public void setStatusCodes(List<Integer> statusCodes) {
        this.statusCodes = statusCodes;
    }

    public List<Float> getConfidence() {
        return confidence;
    }

    public void setConfidence(List<Float> confidence) {
        this.confidence = confidence;
    }

    public List<String> getReasons() {
        return reasons;
    }

    public void setReasons(List<String> reasons) {
        this.reasons = reasons;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public Instant getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Instant updatedTime) {
        this.updatedTime = updatedTime;
    }

    private static long timestampDocumentNanos(TimestampDocument timestampDocument) {
        return timestampDocument.getSeconds() * 1_000_000_000L + timestampDocument.getNanos();
    }

    public static SampleStatusBucketDocument fromSampleStatusColumn(
            String domain,
            String layer,
            DataTimestamps dataTimestamps,
            SampleStatusColumn column,
            String source,
            String modifiedBy,
            Instant updatedTime
    ) {
        final SampleStatusBucketDocument document = new SampleStatusBucketDocument();

        document.setPvName(column.getPvName());
        document.setDomain(domain);
        document.setLayer(layer);

        final DataTimestampsDocument dataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(dataTimestamps);
        document.setDataTimestamps(dataTimestampsDocument);
        document.setFirstTimeNanos(timestampDocumentNanos(dataTimestampsDocument.getFirstTime()));
        document.setLastTimeNanos(timestampDocumentNanos(dataTimestampsDocument.getLastTime()));

        document.setStatusCodes(new ArrayList<>(column.getStatusCodesList()));
        if (!column.getConfidenceList().isEmpty()) {
            document.setConfidence(new ArrayList<>(column.getConfidenceList()));
        }
        if (!column.getReasonsList().isEmpty()) {
            document.setReasons(new ArrayList<>(column.getReasonsList()));
        }

        if (source != null && !source.isBlank()) {
            document.setSource(source);
        }
        if (modifiedBy != null && !modifiedBy.isBlank()) {
            document.setModifiedBy(modifiedBy);
        }
        document.setUpdatedTime(updatedTime);

        return document;
    }

    public SampleStatusBucket toSampleStatusBucket() throws DpException {

        final SampleStatusColumn.Builder columnBuilder = SampleStatusColumn.newBuilder();
        if (this.pvName != null) {
            columnBuilder.setPvName(this.pvName);
        }
        if (this.statusCodes != null) {
            columnBuilder.addAllStatusCodes(this.statusCodes);
        }
        if (this.confidence != null) {
            columnBuilder.addAllConfidence(this.confidence);
        }
        if (this.reasons != null) {
            columnBuilder.addAllReasons(this.reasons);
        }

        final SampleStatusBucket.Builder builder = SampleStatusBucket.newBuilder();
        if (this.domain != null) {
            builder.setDomain(this.domain);
        }
        if (this.layer != null) {
            builder.setLayer(this.layer);
        }
        if (this.dataTimestamps == null) {
            // surface a malformed stored document as a reportable error, never an unchecked throw
            throw new DpException(
                    "SampleStatusBucketDocument missing dataTimestamps for document with id: " + this.id);
        }
        builder.setDataTimestamps(this.dataTimestamps.toDataTimestamps());
        builder.setStatusColumn(columnBuilder.build());
        if (this.source != null) {
            builder.setSource(this.source);
        }
        if (this.modifiedBy != null) {
            builder.setModifiedBy(this.modifiedBy);
        }
        if (this.updatedTime != null) {
            builder.setUpdatedTime(TimestampUtility.getTimestampFromInstant(this.updatedTime));
        }

        return builder.build();
    }
}
