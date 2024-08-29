package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusResponse;
import com.ospreydcs.dp.service.ingest.model.IngestionRequestStatus;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.List;

public class RequestStatusDocument {

    private ObjectId id;
    private String providerId;
    private String providerName;
    private String requestId;
    private Instant updateTime;
    private int requestStatusCase;
    private String requestStatusName;
    private String msg;
    private List<String> idsCreated;

    public RequestStatusDocument() {
    }

    public RequestStatusDocument(
            String providerId,
            String providerName,
            String requestId,
            IngestionRequestStatus status,
            String msg,
            List<String> idsCreated
    ) {
        this.providerId = providerId;
        this.setProviderName(providerName);
        this.requestId = requestId;
        this.setRequestStatusCase(status.ordinal());
        this.setRequestStatusName(status.name());
        this.msg = msg;
        this.idsCreated = idsCreated;
        this.updateTime = Instant.now();
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderName() { return providerName; }

    public void setProviderName(String providerName) { this.providerName = providerName; }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Instant updateTime) {
        this.updateTime = updateTime;
    }

    public int getRequestStatusCase() {
        return requestStatusCase;
    }

    public void setRequestStatusCase(int requestStatusCase) {
        this.requestStatusCase = requestStatusCase;
    }

    public String getRequestStatusName() {
        return requestStatusName;
    }

    public void setRequestStatusName(String requestStatusName) {
        this.requestStatusName = requestStatusName;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<String> getIdsCreated() {
        return idsCreated;
    }

    public void setIdsCreated(List<String> idsCreated) {
        this.idsCreated = idsCreated;
    }

    public QueryRequestStatusResponse.RequestStatusResult.RequestStatus buildRequestStatus(
            RequestStatusDocument requestStatusDocument
    ) {
        final QueryRequestStatusResponse.RequestStatusResult.RequestStatus.Builder requestStatusBuilder =
                QueryRequestStatusResponse.RequestStatusResult.RequestStatus.newBuilder();

        // add base annotation fields to response object
        requestStatusBuilder.setRequestStatusId(this.getId().toString());
        requestStatusBuilder.setProviderId(this.getProviderId());
        requestStatusBuilder.setProviderName(this.getProviderName());
        requestStatusBuilder.setRequestId(this.getRequestId());
        requestStatusBuilder.setStatusMessage(this.getMsg());
        requestStatusBuilder.addAllIdsCreated(this.getIdsCreated());

        // set update time
        Timestamp updateTimestamp = Timestamp.newBuilder()
                .setEpochSeconds(this.getUpdateTime().getEpochSecond())
                .setNanoseconds(this.getUpdateTime().getNano())
                .build();
        requestStatusBuilder.setUpdateTime(updateTimestamp);

        // set status enum
        switch(IngestionRequestStatus.valueOf(requestStatusDocument.getRequestStatusName())) {
            case SUCCESS -> {
                requestStatusBuilder.setIngestionRequestStatus(
                        com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus.INGESTION_REQUEST_STATUS_SUCCESS);
            }
            case REJECTED -> {
                requestStatusBuilder.setIngestionRequestStatus(
                        com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus.INGESTION_REQUEST_STATUS_REJECTED);
            }
            case ERROR -> {
                requestStatusBuilder.setIngestionRequestStatus(
                        com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus.INGESTION_REQUEST_STATUS_ERROR);
            }
        }

        return requestStatusBuilder.build();
    }
}
