package com.ospreydcs.dp.service.common.bson;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataTimestampsDocument {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private int valueCase;
    private String valueType;
    private byte[] bytes = null;
    private long samplePeriod;
    private int sampleCount;
    private TimestampDocument firstTime;
    private TimestampDocument lastTime;

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

    public long getSamplePeriod() {
        return samplePeriod;
    }

    public void setSamplePeriod(long samplePeriod) {
        this.samplePeriod = samplePeriod;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }

    public TimestampDocument getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(TimestampDocument firstTime) {
        this.firstTime = firstTime;
    }

    public TimestampDocument getLastTime() {
        return lastTime;
    }

    public void setLastTime(TimestampDocument lastTime) {
        this.lastTime = lastTime;
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
        final DataTimestampsDocument document = new DataTimestampsDocument();

        // create TimestampDocuments for first/lastTime
        DataTimestampsUtility.DataTimestampsModel timeSpecModel =
                new DataTimestampsUtility.DataTimestampsModel(requestDataTimestamps);
        final Timestamp firstTimestamp = timeSpecModel.getFirstTimestamp();
        document.setFirstTime(TimestampDocument.fromTimestamp(firstTimestamp));
        final Timestamp lastTimestamp = timeSpecModel.getLastTimestamp();
        document.setLastTime(TimestampDocument.fromTimestamp(lastTimestamp));

        // set sample period and count
        document.setSamplePeriod(timeSpecModel.getSamplePeriodNanos());
        document.setSampleCount(timeSpecModel.getSampleCount());

        // serialize protobuf DataTimestamps object to byte array, set fields indicating type of serialized data
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
