package com.ospreydcs.dp.service.common.bson;

import java.time.Instant;
import java.util.List;

public class RequestStatusDocument {
    
    private Integer providerId;
    private String requestId;
    private Instant updateTime;
    private String status;
    private String msg;
    private List<String> idsCreated;

    public RequestStatusDocument() {
    }

    public RequestStatusDocument(
            Integer providerId, String requestId, String status, String msg, List<String> idsCreated) {

        this.providerId = providerId;
        this.requestId = requestId;
        this.status = status;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public Instant getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Instant updateTime) {
        this.updateTime = updateTime;
    }
}
