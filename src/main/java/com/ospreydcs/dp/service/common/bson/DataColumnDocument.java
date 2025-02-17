package com.ospreydcs.dp.service.common.bson;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;

public class DataColumnDocument {

    // instance variables
    private int valueCase;
    private String valueType;
    private byte[] bytes = null;

    public static DataColumnDocument fromDataColumn(DataColumn requestDataColumn) {
        DataColumnDocument document = new DataColumnDocument();
        document.writeBytes(requestDataColumn);
        if ( ! requestDataColumn.getDataValuesList().isEmpty()) {
            final DataValue.ValueCase dataValueCase = requestDataColumn.getDataValues(0).getValueCase();
            document.setValueCase(dataValueCase.getNumber());
            document.setValueType(dataValueCase.name());
        }
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

    public void writeBytes(DataColumn dataColumn) {
        this.bytes = dataColumn.toByteArray();
    }

    public DataColumn readBytes() {
        if (this.bytes == null) {
            return null;
        } else {
            try {
                return DataColumn.parseFrom(this.bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
