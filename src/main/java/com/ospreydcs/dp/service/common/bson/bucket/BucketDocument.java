package com.ospreydcs.dp.service.common.bson.bucket;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;

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
    private long samplePeriod;
    private int sampleCount;
    private int dataTypeCase;
    private String dataType;
    private byte[] dataColumnBytes = null;
    private Map<String, String> attributeMap;
    private long eventStartSeconds;
    private long eventStartNanos;
    private String eventDescription;
    private int providerId;
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

    public long getEventStartSeconds() {
        return eventStartSeconds;
    }

    public void setEventStartSeconds(long eventStartSeconds) {
        this.eventStartSeconds = eventStartSeconds;
    }

    public long getEventStartNanos() {
        return eventStartNanos;
    }

    public void setEventStartNanos(long eventStartNanos) {
        this.eventStartNanos = eventStartNanos;
    }

    public String getEventDescription() {
        return eventDescription;
    }

    public void setEventDescription(String eventDescription) {
        this.eventDescription = eventDescription;
    }

    public int getProviderId() {
        return providerId;
    }

    public void setProviderId(int providerId) {
        this.providerId = providerId;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }
}
