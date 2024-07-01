package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.service.ingest.model.IngestionRequestStatus;

import java.time.Instant;
import java.util.List;

public class RequestStatusDocument {
    
    private Integer providerId;
    private String requestId;
    private Instant updateTime;
    private int requestStatusCase;
    private String requestStatusName;
    private String msg;
    private List<String> idsCreated;

    public RequestStatusDocument() {
    }

    public RequestStatusDocument(
            Integer providerId, String requestId, IngestionRequestStatus status, String msg, List<String> idsCreated) {

        this.providerId = providerId;
        this.requestId = requestId;
        this.setRequestStatusCase(status.ordinal());
        this.setRequestStatusName(status.name());
        this.msg = msg;
        this.idsCreated = idsCreated;
        this.updateTime = Instant.now();
    }

    public Integer getProviderId() {
        return providerId;
    }

    public void setProviderId(Integer providerId) {
        this.providerId = providerId;
    }

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

}
