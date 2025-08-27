package com.ospreydcs.dp.client.utility;

import com.ospreydcs.dp.client.result.DataImportResult;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataImportUtility {

    private static final Logger logger = LogManager.getLogger();

    public static DataImportResult importXlsxData(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return new DataImportResult(true, "File path cannot be null or empty", null);
        }

        File file = new File(filePath);
        if (!file.exists()) {
            return new DataImportResult(true, "File does not exist: " + filePath, null);
        }

        if (!file.canRead()) {
            return new DataImportResult(true, "Cannot read file: " + filePath, null);
        }

        try (FileInputStream fileInputStream = new FileInputStream(file);
             XSSFWorkbook workbook = new XSSFWorkbook(fileInputStream)) {

            logger.debug("Reading XLSX file: {}", filePath);

            // Check if workbook has any sheets
            if (workbook.getNumberOfSheets() == 0) {
                return new DataImportResult(true, "No worksheets found in file", null);
            }

            List<DataImportResult.DataFrameResult> dataFrames = new ArrayList<>();

            // Process each sheet
            for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                Sheet sheet = workbook.getSheetAt(sheetIndex);
                String sheetName = sheet.getSheetName();
                
                logger.debug("Processing sheet '{}' (index {})", sheetName, sheetIndex);

                // Check if sheet has any rows
                if (sheet.getLastRowNum() < 0) {
                    logger.warn("Sheet '{}' is empty, skipping", sheetName);
                    continue;
                }

                // Process header row
                Row headerRow = sheet.getRow(0);
                if (headerRow == null) {
                    logger.warn("Sheet '{}' header row is missing, skipping", sheetName);
                    continue;
                }

                // Validate minimum column count (seconds, nanos, at least one data column)
                int columnCount = headerRow.getLastCellNum();
                if (columnCount < 3) {
                    logger.warn("Sheet '{}' must have at least 3 columns (seconds, nanos, and at least one data column), skipping", sheetName);
                    continue;
                }

                logger.debug("Sheet '{}' found {} columns in header row", sheetName, columnCount);

                // Extract data column names (skip first two timestamp columns)
                List<String> dataColumnNames = new ArrayList<>();
                boolean sheetValid = true;
                for (int colIndex = 2; colIndex < columnCount; colIndex++) {
                    Cell headerCell = headerRow.getCell(colIndex);
                    if (headerCell == null || headerCell.getCellType() == CellType.BLANK) {
                        logger.warn("Sheet '{}' header cell is empty at column {}, skipping sheet", sheetName, colIndex + 1);
                        sheetValid = false;
                        break;
                    }
                    String columnName = getCellStringValue(headerCell);
                    if (columnName.trim().isEmpty()) {
                        logger.warn("Sheet '{}' header cell is empty at column {}, skipping sheet", sheetName, colIndex + 1);
                        sheetValid = false;
                        break;
                    }
                    dataColumnNames.add(columnName);
                }
                
                if (!sheetValid) {
                    continue;
                }

                // Initialize data structures for this sheet
                List<Timestamp> timestamps = new ArrayList<>();
                List<List<DataValue>> columnDataLists = new ArrayList<>();
                for (int i = 0; i < dataColumnNames.size(); i++) {
                    columnDataLists.add(new ArrayList<>());
                }

                // Process data rows for this sheet
                int dataRowCount = 0;
                for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                    Row dataRow = sheet.getRow(rowIndex);
                    if (dataRow == null) {
                        logger.warn("Sheet '{}' empty row found at row {}, skipping", sheetName, rowIndex + 1);
                        continue;
                    }

                    // Validate row has correct number of columns
                    if (dataRow.getLastCellNum() != columnCount) {
                        logger.warn("Sheet '{}' row {} has {} columns, expected {}, skipping row", 
                            sheetName, rowIndex + 1, dataRow.getLastCellNum(), columnCount);
                        continue;
                    }

                    // Extract timestamp from first two columns
                    try {
                        // Seconds column (column 0)
                        Cell secondsCell = dataRow.getCell(0);
                        if (secondsCell == null || secondsCell.getCellType() == CellType.BLANK) {
                            logger.warn("Sheet '{}' seconds cell is empty at row {}, skipping row", sheetName, rowIndex + 1);
                            continue;
                        }
                        long epochSeconds = getCellLongValue(secondsCell);

                        // Nanoseconds column (column 1)
                        Cell nanosCell = dataRow.getCell(1);
                        if (nanosCell == null || nanosCell.getCellType() == CellType.BLANK) {
                            logger.warn("Sheet '{}' nanoseconds cell is empty at row {}, skipping row", sheetName, rowIndex + 1);
                            continue;
                        }
                        long nanoseconds = getCellLongValue(nanosCell);

                        // Create timestamp
                        Timestamp timestamp = Timestamp.newBuilder()
                                .setEpochSeconds(epochSeconds)
                                .setNanoseconds(nanoseconds)
                                .build();
                        timestamps.add(timestamp);

                    } catch (NumberFormatException e) {
                        logger.warn("Sheet '{}' invalid timestamp value at row {}, skipping row: {}", sheetName, rowIndex + 1, e.getMessage());
                        continue;
                    }

                    // Process data columns
                    boolean rowValid = true;
                    List<DataValue> rowDataValues = new ArrayList<>();
                    
                    for (int colIndex = 2; colIndex < columnCount; colIndex++) {
                        Cell dataCell = dataRow.getCell(colIndex);
                        if (dataCell == null || dataCell.getCellType() == CellType.BLANK) {
                            logger.warn("Sheet '{}' empty cell found at row {}, column {}, skipping row", sheetName, rowIndex + 1, colIndex + 1);
                            rowValid = false;
                            break;
                        }

                        DataValue dataValue = createDataValueFromCell(dataCell);
                        if (dataValue == null) {
                            logger.warn("Sheet '{}' unsupported cell type at row {}, column {}, skipping row", sheetName, rowIndex + 1, colIndex + 1);
                            rowValid = false;
                            break;
                        }

                        rowDataValues.add(dataValue);
                    }

                    if (rowValid) {
                        // Add row data to column lists
                        for (int i = 0; i < rowDataValues.size(); i++) {
                            columnDataLists.get(i).add(rowDataValues.get(i));
                        }
                        dataRowCount++;
                    }
                }

                logger.debug("Sheet '{}' processed {} data rows", sheetName, dataRowCount);

                // Skip sheets with no valid data rows
                if (dataRowCount == 0) {
                    logger.warn("Sheet '{}' has no valid data rows, skipping", sheetName);
                    continue;
                }

                // Build DataColumn objects for this sheet
                List<DataColumn> dataColumns = new ArrayList<>();
                for (int i = 0; i < dataColumnNames.size(); i++) {
                    DataColumn dataColumn = DataColumn.newBuilder()
                            .setName(dataColumnNames.get(i))
                            .addAllDataValues(columnDataLists.get(i))
                            .build();
                    dataColumns.add(dataColumn);
                }

                // Create and add data frame result for this sheet
                DataImportResult.DataFrameResult frameResult = new DataImportResult.DataFrameResult(
                    sheetName, timestamps, dataColumns);
                dataFrames.add(frameResult);
                
                logger.debug("Successfully imported sheet '{}': {} timestamps, {} columns", 
                    sheetName, timestamps.size(), dataColumns.size());
            }

            // Check if any sheets were successfully processed
            if (dataFrames.isEmpty()) {
                return new DataImportResult(true, "No valid data sheets found in file", null);
            }

            logger.debug("Successfully imported data from XLSX file: {} sheet(s) processed", dataFrames.size());
            return new DataImportResult(false, "", dataFrames);

        } catch (IOException e) {
            logger.error("IOException reading XLSX file {}: {}", filePath, e.getMessage());
            return new DataImportResult(true, "Error reading file: " + e.getMessage(), null);
        } catch (Exception e) {
            logger.error("Unexpected error processing XLSX file {}: {}", filePath, e.getMessage());
            return new DataImportResult(true, "Unexpected error: " + e.getMessage(), null);
        }
    }

    private static String getCellStringValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                return String.valueOf(cell.getNumericCellValue());
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue();
                } catch (IllegalStateException e) {
                    try {
                        return String.valueOf(cell.getNumericCellValue());
                    } catch (IllegalStateException e2) {
                        return String.valueOf(cell.getBooleanCellValue());
                    }
                }
            default:
                return "";
        }
    }

    private static long getCellLongValue(Cell cell) throws NumberFormatException {
        if (cell == null) {
            throw new NumberFormatException("Cell is null");
        }
        
        switch (cell.getCellType()) {
            case NUMERIC:
                double numericValue = cell.getNumericCellValue();
                return (long) numericValue;
            case STRING:
                return Long.parseLong(cell.getStringCellValue().trim());
            case FORMULA:
                try {
                    double formulaNumericValue = cell.getNumericCellValue();
                    return (long) formulaNumericValue;
                } catch (IllegalStateException e) {
                    return Long.parseLong(cell.getStringCellValue().trim());
                }
            default:
                throw new NumberFormatException("Cell type " + cell.getCellType() + " cannot be converted to long");
        }
    }

    private static DataValue createDataValueFromCell(Cell cell) {
        if (cell == null) {
            return null;
        }

        switch (cell.getCellType()) {
            case NUMERIC:
                return DataValue.newBuilder()
                        .setDoubleValue(cell.getNumericCellValue())
                        .build();
            case STRING:
                return DataValue.newBuilder()
                        .setStringValue(cell.getStringCellValue())
                        .build();
            case BOOLEAN:
                return DataValue.newBuilder()
                        .setBooleanValue(cell.getBooleanCellValue())
                        .build();
            case FORMULA:
                try {
                    // Try to evaluate formula as numeric first
                    double numericValue = cell.getNumericCellValue();
                    return DataValue.newBuilder()
                            .setDoubleValue(numericValue)
                            .build();
                } catch (IllegalStateException e) {
                    try {
                        // Try to evaluate as string
                        String stringValue = cell.getStringCellValue();
                        return DataValue.newBuilder()
                                .setStringValue(stringValue)
                                .build();
                    } catch (IllegalStateException e2) {
                        try {
                            // Try to evaluate as boolean
                            boolean booleanValue = cell.getBooleanCellValue();
                            return DataValue.newBuilder()
                                    .setBooleanValue(booleanValue)
                                    .build();
                        } catch (IllegalStateException e3) {
                            return null;
                        }
                    }
                }
            default:
                return null;
        }
    }
}
