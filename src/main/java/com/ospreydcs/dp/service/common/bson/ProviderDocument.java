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

    public QueryProvidersResponse.ProvidersResult.ProviderInfo toProviderInfo() {

        QueryProvidersResponse.ProvidersResult.ProviderInfo.Builder providerInfoBuilder =
                QueryProvidersResponse.ProvidersResult.ProviderInfo.newBuilder();

        providerInfoBuilder.setId(this.getId().toString());
        providerInfoBuilder.setName(this.getName());
        providerInfoBuilder.setDescription(this.getDescription());
        providerInfoBuilder.addAllTags(this.getTags());
        providerInfoBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));

        return providerInfoBuilder.build();
    }
}
