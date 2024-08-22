package com.ospreydcs.dp.service.common.bson;

import org.bson.types.ObjectId;

import java.util.Date;
import java.util.Map;

public class ProviderDocument {

    // instance variables
    private ObjectId id;
    private String name;
    private Map<String, String> attributeMap;
    private Date lastUpdated;

    public ProviderDocument() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
