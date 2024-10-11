package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import java.util.List;

public interface TabularDataExportFileInterface {
    void writeHeaderRow(List<String> headers);
    void writeDataRow(List<String> data);
    void close();
}
