package com.ospreydcs.dp.service.common.bson.bucket;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * This POJO is for writing time series data to mongodb by customizing the code registry.
 *
 * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED WITHOUT ACCESSOR METHODS!!!
 */
public class BucketDocument {

    private String id;
    private String pvName;
    private Date firstTime;
    private long firstSeconds;
    private long firstNanos;
    private Date lastTime;
    private long lastSeconds;
    private long lastNanos;
    private EventMetadataDocument eventMetadata;
    private long samplePeriod;
    private int sampleCount;
    private DataColumnDocument dataColumn;
    private int dataTimestampsCase;
    private String dataTimestampsType;
    private byte[] dataTimestampsBytes = null;
    private Map<String, String> attributeMap;
    private String providerId;
    private String clientRequestId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public Date getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(Date firstTime) {
        this.firstTime = firstTime;
    }

    public long getFirstSeconds() {
        return firstSeconds;
    }

    public void setFirstSeconds(long firstSeconds) {
        this.firstSeconds = firstSeconds;
    }

    public long getFirstNanos() {
        return firstNanos;
    }

    public void setFirstNanos(long firstNanos) {
        this.firstNanos = firstNanos;
    }

    public Date getLastTime() {
        return lastTime;
    }

    public void setLastTime(Date lastTime) {
        this.lastTime = lastTime;
    }

    public long getLastSeconds() {
        return lastSeconds;
    }

    public void setLastSeconds(long lastSeconds) {
        this.lastSeconds = lastSeconds;
    }

    public long getLastNanos() {
        return lastNanos;
    }

    public void setLastNanos(long lastNanos) {
        this.lastNanos = lastNanos;
    }

    public EventMetadataDocument getEventMetadata() {
        return eventMetadata;
    }

    public void setEventMetadata(EventMetadataDocument eventMetadata) {
        this.eventMetadata = eventMetadata;
    }

    public DataColumnDocument getDataColumn() {
        return dataColumn;
    }

    public void setDataColumn(DataColumnDocument dataColumn) {
        this.dataColumn = dataColumn;
    }

    public int getDataTimestampsCase() {
        return dataTimestampsCase;
    }

    public void setDataTimestampsCase(int dataTimestampsCase) {
        this.dataTimestampsCase = dataTimestampsCase;
    }

    public String getDataTimestampsType() {
        return dataTimestampsType;
    }

    public void setDataTimestampsType(String dataTimestampsType) {
        this.dataTimestampsType = dataTimestampsType;
    }

    public byte[] getDataTimestampsBytes() {
        return dataTimestampsBytes;
    }

    public void setDataTimestampsBytes(byte[] dataTimestampsBytes) {
        this.dataTimestampsBytes = dataTimestampsBytes;
    }

    public void writeDataTimestampsContent(DataTimestamps dataTimestamps) {
        this.dataTimestampsBytes = dataTimestamps.toByteArray();
    }

    public DataTimestamps readDataTimestampsContent() {
        if (dataTimestampsBytes == null) {
            return null;
        } else {
            try {
                return DataTimestamps.parseFrom(dataTimestampsBytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public long getSamplePeriod() {
        return samplePeriod;
    }

    public void setSamplePeriod(long samplePeriod) {
        this.samplePeriod = samplePeriod;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     *
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @return
     */
    public static List<BucketDocument> generateBucketsFromRequest(IngestDataRequest request)
            throws DpIngestionException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // get timestamp details
        DataTimestampsUtility.DataTimestampsModel timeSpecModel = new DataTimestampsUtility.DataTimestampsModel(request.getIngestionDataFrame().getDataTimestamps());
        final Timestamp firstTimestamp = timeSpecModel.getFirstTimestamp();
        final long firstTimestampSeconds = firstTimestamp.getEpochSeconds();
        final long firstTimestampNanos = firstTimestamp.getNanoseconds();
        final Date firstTimestampDate = TimestampUtility.dateFromTimestamp(firstTimestamp);
        final Timestamp lastTimestamp = timeSpecModel.getLastTimestamp();
        final long lastTimestampSeconds = lastTimestamp.getEpochSeconds();
        final long lastTimestampNanos = lastTimestamp.getNanoseconds();
        final Date lastTimestampDate = TimestampUtility.dateFromTimestamp(lastTimestamp);

        // create BSON document for each column
        final List<DataColumn> columns = request.getIngestionDataFrame().getDataColumnsList();
        for (DataColumn column : columns) {
            final String pvName = column.getName();
            final String documentId = pvName + "-" + firstTimestampSeconds + "-" + firstTimestampNanos;

            BucketDocument bucket = new BucketDocument();

            bucket.setId(documentId);
            bucket.setPvName(pvName);
            bucket.setProviderId(request.getProviderId());
            bucket.setClientRequestId(request.getClientRequestId());

            // create DataColumnDocument for DataColumn
            DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(column);
            bucket.setDataColumn(dataColumnDocument);

            final DataTimestamps requestDataTimestamps = request.getIngestionDataFrame().getDataTimestamps();
            bucket.writeDataTimestampsContent(requestDataTimestamps);
            final DataTimestamps.ValueCase dataTimestampsCase = requestDataTimestamps.getValueCase();
            bucket.setDataTimestampsCase(dataTimestampsCase.getNumber());
            bucket.setDataTimestampsType(dataTimestampsCase.name());

            bucket.setFirstTime(firstTimestampDate);
            bucket.setFirstSeconds(firstTimestampSeconds);
            bucket.setFirstNanos(firstTimestampNanos);
            bucket.setLastTime(lastTimestampDate);
            bucket.setLastSeconds(lastTimestampSeconds);
            bucket.setLastNanos(lastTimestampNanos);
            bucket.setSamplePeriod(timeSpecModel.getSamplePeriodNanos());
            bucket.setSampleCount(timeSpecModel.getSampleCount());

            // add attributes
            final Map<String, String> attributeMap =
                    AttributesUtility.attributeMapFromList(request.getAttributesList());
            bucket.setAttributeMap(attributeMap);

            // add event metadata
            String eventDescription = "";
            long eventStartSeconds = 0;
            long eventStartNanos = 0;
            long eventStopSeconds = 0;
            long eventStopNanos = 0;
            if (request.hasEventMetadata()) {
                if (request.getEventMetadata().getDescription() != null) {
                    eventDescription = request.getEventMetadata().getDescription();
                }
                if (request.getEventMetadata().hasStartTimestamp()) {
                    eventStartSeconds = request.getEventMetadata().getStartTimestamp().getEpochSeconds();
                    eventStartNanos = request.getEventMetadata().getStartTimestamp().getNanoseconds();
                }
                if (request.getEventMetadata().hasStopTimestamp()) {
                    eventStopSeconds = request.getEventMetadata().getStopTimestamp().getEpochSeconds();
                    eventStopNanos = request.getEventMetadata().getStopTimestamp().getNanoseconds();
                }
            }
            EventMetadataDocument eventMetadataDocument = new EventMetadataDocument();
            eventMetadataDocument.setDescription(eventDescription);
            eventMetadataDocument.setStartSeconds(eventStartSeconds);
            eventMetadataDocument.setStartNanos(eventStartNanos);
            eventMetadataDocument.setStopSeconds(eventStopSeconds);
            eventMetadataDocument.setStopNanos(eventStopNanos);
            bucket.setEventMetadata(eventMetadataDocument);

            bucketList.add(bucket);
        }

        return bucketList;
    }

    public static QueryDataResponse.QueryData.DataBucket dataBucketFromDocument(
            BucketDocument document
    ) throws DpException {

        final QueryDataResponse.QueryData.DataBucket.Builder bucketBuilder =
                QueryDataResponse.QueryData.DataBucket.newBuilder();

        // add data timestamps
        DataTimestamps dataTimestamps = document.readDataTimestampsContent();
        bucketBuilder.setDataTimestamps(dataTimestamps);

        // add data values
        DataColumn dataColumn = document.getDataColumn().toDataColumn();
        bucketBuilder.setDataColumn(dataColumn);

        // add attributes
        if (document.getAttributeMap() != null) {
            for (var documentAttributeMapEntry : document.getAttributeMap().entrySet()) {
                final String documentAttributeKey = documentAttributeMapEntry.getKey();
                final String documentAttributeValue = documentAttributeMapEntry.getValue();
                final Attribute responseAttribute = Attribute.newBuilder()
                        .setName(documentAttributeKey)
                        .setValue(documentAttributeValue)
                        .build();
                bucketBuilder.addAttributes(responseAttribute);
            }
        }

        // add event metadata
        if (document.getEventMetadata() != null) {
            final EventMetadataDocument eventMetadataDocument = document.getEventMetadata();

            Timestamp responseEventStartTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(eventMetadataDocument.getStartSeconds())
                    .setNanoseconds(eventMetadataDocument.getStartNanos())
                    .build();
            Timestamp responseEventStopTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(eventMetadataDocument.getStopSeconds())
                    .setNanoseconds(eventMetadataDocument.getStopNanos())
                    .build();
            EventMetadata responseEventMetadata = EventMetadata.newBuilder()
                    .setDescription(eventMetadataDocument.getDescription())
                    .setStartTimestamp(responseEventStartTimestamp)
                    .setStopTimestamp(responseEventStopTimestamp)
                    .build();
            bucketBuilder.setEventMetadata(responseEventMetadata);
        }

        return bucketBuilder.build();
    }

}
