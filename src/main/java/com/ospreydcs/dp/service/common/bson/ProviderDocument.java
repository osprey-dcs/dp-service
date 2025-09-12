package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.query.ProviderMetadata;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import org.bson.types.ObjectId;

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

    public QueryProvidersResponse.ProvidersResult.ProviderInfo toProviderInfo(ProviderMetadata providerMetadata) {

        QueryProvidersResponse.ProvidersResult.ProviderInfo.Builder providerInfoBuilder =
                QueryProvidersResponse.ProvidersResult.ProviderInfo.newBuilder();

        if (this.getId() != null) {
            providerInfoBuilder.setId(this.getId().toString());
        }

        if (this.getName() != null) {
            providerInfoBuilder.setName(this.getName());
        }

        if (this.getDescription() != null) {
            providerInfoBuilder.setDescription(this.getDescription());
        }

        if (this.getTags() != null) {
            providerInfoBuilder.addAllTags(this.getTags());
        }

        if (this.getAttributes() != null) {
            providerInfoBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        if (providerMetadata != null) {
            providerInfoBuilder.setProviderMetadata(providerMetadata);
        }

        return providerInfoBuilder.build();
    }
}
