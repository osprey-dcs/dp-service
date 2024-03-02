package com.ospreydcs.dp.service.common.bson.annotation;

import java.util.List;

public class DataBlock {

    private long beginTimeSeconds;
    private long beginTimeNanos;

    private long endTimeSeconds;
    private long endTimeNanos;
    List<String> pvNames;

    public DataBlock(long beginTimeSeconds, long beginTimeNanos, long endTimeSeconds, long endTimeNanos, List<String> pvNames) {
        this.beginTimeSeconds = beginTimeSeconds;
        this.beginTimeNanos = beginTimeNanos;
        this.endTimeSeconds = endTimeSeconds;
        this.endTimeNanos = endTimeNanos;
        this.pvNames = pvNames;
    }

    public long getBeginTimeSeconds() {
        return beginTimeSeconds;
    }

    public void setBeginTimeSeconds(long beginTimeSeconds) {
        this.beginTimeSeconds = beginTimeSeconds;
    }

    public long getBeginTimeNanos() {
        return beginTimeNanos;
    }

    public void setBeginTimeNanos(long beginTimeNanos) {
        this.beginTimeNanos = beginTimeNanos;
    }

    public long getEndTimeSeconds() {
        return endTimeSeconds;
    }

    public void setEndTimeSeconds(long endTimeSeconds) {
        this.endTimeSeconds = endTimeSeconds;
    }

    public long getEndTimeNanos() {
        return endTimeNanos;
    }

    public void setEndTimeNanos(long endTimeNanos) {
        this.endTimeNanos = endTimeNanos;
    }

    public List<String> getPvNames() {
        return pvNames;
    }

    public void setPvNames(List<String> pvNames) {
        this.pvNames = pvNames;
    }

}
