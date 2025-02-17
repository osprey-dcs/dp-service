package com.ospreydcs.dp.service.common.bson;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;

public class DataTimestampsDocument {

    // instance variables
    private int valueCase;
    private String valueType;
    private byte[] bytes = null;

    public static DataTimestampsDocument fromDataTimestamps(
            DataTimestamps requestDataTimestamps
    ) {
        DataTimestampsDocument document = new DataTimestampsDocument();
        document.writeBytes(requestDataTimestamps);
        final DataTimestamps.ValueCase dataTimestampsCase = requestDataTimestamps.getValueCase();
        document.setValueCase(dataTimestampsCase.getNumber());
        document.setValueType(dataTimestampsCase.name());
        return document;
    }

    public int getValueCase() {
        return valueCase;
    }

    public void setValueCase(int valueCase) {
        this.valueCase = valueCase;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public void writeBytes(DataTimestamps dataTimestamps) {
        this.bytes = dataTimestamps.toByteArray();
    }

    public DataTimestamps readBytes() {
        if (this.bytes == null) {
            return null;
        } else {
            try {
                return DataTimestamps.parseFrom(this.bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
