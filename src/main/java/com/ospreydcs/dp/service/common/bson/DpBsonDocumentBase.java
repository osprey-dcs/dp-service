package com.ospreydcs.dp.service.common.bson;

import java.time.Instant;
import java.util.List;

public abstract class DpBsonDocumentBase {

    // instance variables
    private Instant createdAt;
    private Instant updatedAt;

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    // NOTE - this method uses "add" instead of "set" to avoid problems in the Mongo framework expecting
    // methods that start with "set" to have a corresponding property.
    public void addCreationTime() {
        setCreatedAt(Instant.now());
    }

    // NOTE - this method uses "add" instead of "set" to avoid problems in the Mongo framework expecting
    // methods that start with "set" to have a corresponding property.
    public void addUpdatedTime() {
        setUpdatedAt(Instant.now());
    }
}
