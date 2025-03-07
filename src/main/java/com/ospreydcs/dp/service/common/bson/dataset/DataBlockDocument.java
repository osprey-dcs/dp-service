package com.ospreydcs.dp.service.common.bson.dataset;

import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;

import java.util.*;

public class DataBlockDocument {

    private TimestampDocument beginTime;
    private TimestampDocument endTime;
    List<String> pvNames;

    public TimestampDocument getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(TimestampDocument beginTime) {
        this.beginTime = beginTime;
    }

    public TimestampDocument getEndTime() {
        return endTime;
    }

    public void setEndTime(TimestampDocument endTime) {
        this.endTime = endTime;
    }

    public List<String> getPvNames() {
        return pvNames;
    }

    public void setPvNames(List<String> pvNames) {
        this.pvNames = pvNames;
    }

    public static DataBlockDocument fromDataBlock(DataBlock dataBlock) {
        
        DataBlockDocument document = new DataBlockDocument();

        document.setPvNames(List.copyOf(dataBlock.getPvNamesList()));
        
        document.setBeginTime(TimestampDocument.fromTimestamp(dataBlock.getBeginTime()));
        document.setEndTime(TimestampDocument.fromTimestamp(dataBlock.getEndTime()));

        return document;
    }

    public DataBlock toDataBlock() {
        DataBlock.Builder builder = DataBlock.newBuilder();
        builder.setBeginTime(this.getBeginTime().toTimestamp());
        builder.setEndTime(this.getEndTime().toTimestamp());
        builder.addAllPvNames(this.getPvNames());
        return builder.build();
    }
    
    public List<String> diffDataBlock(DataBlock requestDataBlock) {
        
        final List<String> diffs = new ArrayList<>();
        
        final Timestamp requestBlockBeginTime =  requestDataBlock.getBeginTime();
        final Timestamp requestBlockEndTime = requestDataBlock.getEndTime();

        if ( ! Objects.equals(this.beginTime.toTimestamp(), requestBlockBeginTime) ) {
            final String msg = "block beginTime mistmatch: " + this.getBeginTime()
                    + " expected: " + requestBlockEndTime;
            diffs.add(msg);
        }

        if ( ! Objects.equals(this.endTime.toTimestamp(), requestBlockEndTime) ) {
            final String msg = "block endTime mistmatch: " + this.getEndTime()
                    + " expected: " + requestBlockEndTime;
            diffs.add(msg);
        }

        if ( ! Objects.equals(requestDataBlock.getPvNamesList(), this.getPvNames())) {
            final String msg = "block pvNames list mismatch: " + this.getPvNames().toString()
                    + " expected: " + requestDataBlock.getPvNamesList();
            diffs.add(msg);
        }

        return diffs;
    }
    
}
