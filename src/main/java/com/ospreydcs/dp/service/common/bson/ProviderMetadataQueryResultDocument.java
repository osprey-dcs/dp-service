package com.ospreydcs.dp.service.common.bson;

import java.util.Date;
import java.util.List;

public class ProviderMetadataQueryResultDocument {

    // instance variables
    private String id;
    private List<String> pvNames;
    private Date firstBucketTimestamp;
    private Date lastBucketTimestamp;
    private int numBuckets;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getPvNames() {
        return pvNames;
    }

    public void setPvNames(List<String> pvNames) {
        this.pvNames = pvNames;
    }

    public Date getFirstBucketTimestamp() {
        return firstBucketTimestamp;
    }

    public void setFirstBucketTimestamp(Date firstBucketTimestamp) {
        this.firstBucketTimestamp = firstBucketTimestamp;
    }

    public Date getLastBucketTimestamp() {
        return lastBucketTimestamp;
    }

    public void setLastBucketTimestamp(Date lastBucketTimestamp) {
        this.lastBucketTimestamp = lastBucketTimestamp;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
    }
}
