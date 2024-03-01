package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * This POJO is for writing time series data to mongodb by customizing the code registry.
 *
 * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED WITHOUT ACCESSOR METHODS!!!
 */
@BsonDiscriminator(key="dataType")
public abstract class BucketDocument<T> {
    private String id;
    private String columnName;
    private Date firstTime;
    private long firstSeconds;
    private long firstNanos;
    private Date lastTime;
    private long lastSeconds;
    private long lastNanos;
    private long sampleFrequency;
    private int numSamples;
    private String dataType;
    private List<T> columnDataList;
    private Map<String, String> attributeMap;
    private long eventSeconds;
    private long eventNanos;
    private String eventDescription;

    public abstract void addColumnDataValue(T dataValue, DataValue.Builder valueBuilder);

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
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

    public String getDataType() {
        return this.dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setColumnDataList(List<T> columnDataList) {
        this.columnDataList = columnDataList;
    }

    public List<T> getColumnDataList() { return this.columnDataList; }

    public void initColumnDataList() {
        this.columnDataList = new ArrayList<T>();
    }

    public void addColumnData(T data) {
        this.columnDataList.add(data);
    }

    public long getSampleFrequency() {
        return sampleFrequency;
    }

    public void setSampleFrequency(long sampleFrequency) {
        this.sampleFrequency = sampleFrequency;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int numSamples) {
        this.numSamples = numSamples;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    public long getEventSeconds() {
        return eventSeconds;
    }

    public void setEventSeconds(long eventSeconds) {
        this.eventSeconds = eventSeconds;
    }

    public long getEventNanos() {
        return eventNanos;
    }

    public void setEventNanos(long eventNanos) {
        this.eventNanos = eventNanos;
    }

    public String getEventDescription() {
        return eventDescription;
    }

    public void setEventDescription(String eventDescription) {
        this.eventDescription = eventDescription;
    }
}
