package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.service.common.model.TimestampDataMap;

import java.util.List;

public interface TabularDataExportFileInterface {
    void writeHeaderRow(List<String> headers);
    void writeData(TimestampDataMap timestampDataMap);
    void close();
}
