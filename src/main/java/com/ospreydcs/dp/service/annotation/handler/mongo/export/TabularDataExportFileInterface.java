package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;

import java.util.List;

public interface TabularDataExportFileInterface {
    void writeHeaderRow(List<String> headers) throws DpException;
    void writeData(TimestampDataMap timestampDataMap) throws DpException;
    void close() throws DpException;
}
