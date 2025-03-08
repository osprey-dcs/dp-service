package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class ProviderDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String name;
    private String description;
    private List<String> tags;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
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

    public QueryProvidersResponse.ProvidersResult.ProviderInfo toProviderInfo() {

        QueryProvidersResponse.ProvidersResult.ProviderInfo.Builder providerInfoBuilder =
                QueryProvidersResponse.ProvidersResult.ProviderInfo.newBuilder();

        providerInfoBuilder.setId(this.getId().toString());
        providerInfoBuilder.setName(this.getName());
        providerInfoBuilder.setDescription(this.getDescription());
        providerInfoBuilder.addAllTags(this.getTags());
        providerInfoBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributeMap()));

        return providerInfoBuilder.build();
    }
}
