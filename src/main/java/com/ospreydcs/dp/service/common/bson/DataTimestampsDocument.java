package com.ospreydcs.dp.service.common.bson;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataTimestampsDocument {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private int valueCase;
    private String valueType;
    private byte[] bytes = null;

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

    public DataTimestamps toDataTimestamps() throws DpException {

        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        if (this.bytes != null) {
            try {
                return DataTimestamps.parseFrom(this.bytes);
            } catch (InvalidProtocolBufferException e) {
                final String errorMsg =
                        "DataTimestampsDocument.toDataTimestamps() error parsing serialized byte array: " + e.getMessage();
                logger.error(errorMsg);
                throw new DpException(errorMsg);
            }
        }

        return dataTimestampsBuilder.build();
    }

}
