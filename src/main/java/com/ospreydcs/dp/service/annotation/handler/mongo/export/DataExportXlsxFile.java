package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;
import java.util.List;

public class DataExportXlsxFile implements TabularDataExportFileInterface {

    // constants
    private final String SHEET_NAME_DATA = "data";

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    private final String filePathString;
    private final SXSSFWorkbook workbook;
    private final Sheet dataSheet;
    private final CreationHelper creationHelper;
    private int currentDataRowIndex = 1;

    public DataExportXlsxFile(DataSetDocument dataSet, String filePathString) throws DpException {
        this.filePathString = filePathString;
        this.workbook = new SXSSFWorkbook(1);
        this.dataSheet = workbook.createSheet(SHEET_NAME_DATA);
        this.creationHelper = workbook.getCreationHelper();
    }

    @Override
    public void writeHeaderRow(List<String> headers) throws DpException {
        final Row headerRot = dataSheet.createRow(0);
        for (int i = 0; i < headers.size(); i++) {
            final Cell cell = headerRot.createCell(i);
            cell.setCellValue(headers.get(i));
        }
    }

    private void setCellValue(Cell fileDataCell, DataValue dataValue) {

        switch (dataValue.getValueCase()) {
            case STRINGVALUE -> {
                fileDataCell.setCellValue(dataValue.getStringValue());
            }
            case BOOLEANVALUE -> {
                fileDataCell.setCellValue(dataValue.getBooleanValue());
            }
            case UINTVALUE -> {
                fileDataCell.setCellValue(dataValue.getUintValue());
            }
            case ULONGVALUE -> {
                fileDataCell.setCellValue(dataValue.getLongValue());
            }
            case INTVALUE -> {
                fileDataCell.setCellValue(dataValue.getIntValue());
            }
            case LONGVALUE -> {
                fileDataCell.setCellValue(dataValue.getLongValue());
            }
            case FLOATVALUE -> {
                fileDataCell.setCellValue(dataValue.getFloatValue());
            }
            case DOUBLEVALUE -> {
                fileDataCell.setCellValue(dataValue.getDoubleValue());
            }
//            case BYTEARRAYVALUE -> {
//            }
//            case ARRAYVALUE -> {
//            }
//            case STRUCTUREVALUE -> {
//            }
//            case IMAGEVALUE -> {
//            }
//            case TIMESTAMPVALUE -> {
//            }
//            case VALUE_NOT_SET -> {
//            }
            default -> {
                fileDataCell.setCellValue(dataValue.toString());
            }
        }
    }

    @Override
    public void writeData(TimestampDataMap timestampDataMap) throws DpException {
        final TimestampDataMap.DataRowIterator dataRowIterator = timestampDataMap.dataRowIterator();
        while (dataRowIterator.hasNext()) {
            final TimestampDataMap.DataRow sourceDataRow = dataRowIterator.next();
            final Row fileDataRow = dataSheet.createRow(currentDataRowIndex);
            fileDataRow.createCell(0).setCellValue(sourceDataRow.seconds());
            fileDataRow.createCell(1).setCellValue(sourceDataRow.nanos());
            int dataColumnIndex = 2;
            for (DataValue sourceCellValue : sourceDataRow.dataValues()) {
                Cell fileDataCell = fileDataRow.createCell(dataColumnIndex);
                setCellValue(fileDataCell, sourceCellValue);
                dataColumnIndex++;
            }

            this.currentDataRowIndex++;
        }
    }

    @Override
    public void close() throws DpException {
        try {
            final FileOutputStream fileOutputStream = new FileOutputStream(new File(filePathString));
            this.workbook.write(fileOutputStream);
            fileOutputStream.flush();
            fileOutputStream.close();
            this.workbook.dispose();
        } catch (IOException e) {
            logger.error("IOException writing and closing excel file: " + e.getMessage());
            throw new DpException(e);
        }
    }
}
