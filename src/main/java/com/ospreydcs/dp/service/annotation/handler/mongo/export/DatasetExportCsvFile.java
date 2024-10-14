package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DatasetExportCsvFile implements TabularDataExportFileInterface {

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    private final CsvWriter csvWriter;

    public DatasetExportCsvFile(DataSetDocument dataSet, String filePathString) throws IOException {
        Path filePath = Paths.get(filePathString);
        this.csvWriter = CsvWriter.builder().build(filePath);
    }

    @Override
    public void writeHeaderRow(List<String> headers) {
        this.csvWriter.writeRecord(headers);
    }

    @Override
    public void writeDataRow(List<String> data) {
        this.csvWriter.writeRecord(data);
    }

    @Override
    public void close() {
        try {
            this.csvWriter.close();
        } catch (IOException e) {
            logger.error("IOException in CsvWriter.close(): " + e.getMessage());
        }
    }

}
