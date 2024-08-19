package com.ospreydcs.dp.service.common.bson.bucket;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;

import java.util.Date;
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
    private int dataTypeCase;
    private String dataType;
    private byte[] dataColumnBytes = null;
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

    public int getDataTypeCase() {
        return this.dataTypeCase;
    }

    public void setDataTypeCase(int dataTypeCase) {
        this.dataTypeCase = dataTypeCase;
    }

    public String getDataType() {
        return this.dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public byte[] getDataColumnBytes() {
        return this.dataColumnBytes;
    }

    public void setDataColumnBytes(byte[] content){
        this.dataColumnBytes = content;
    }

    public void writeDataColumnContent(DataColumn dataColumn) {
        this.dataColumnBytes = dataColumn.toByteArray();
    }

    public DataColumn readDataColumnContent() {
        if (dataColumnBytes == null) {
            return null;
        } else {
            try {
                return DataColumn.parseFrom(dataColumnBytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
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
}
