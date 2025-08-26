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
            return new DataImportResult(true, "File path cannot be null or empty", null, null);
        }

        File file = new File(filePath);
        if (!file.exists()) {
            return new DataImportResult(true, "File does not exist: " + filePath, null, null);
        }

        if (!file.canRead()) {
            return new DataImportResult(true, "Cannot read file: " + filePath, null, null);
        }

        try (FileInputStream fileInputStream = new FileInputStream(file);
             XSSFWorkbook workbook = new XSSFWorkbook(fileInputStream)) {

            logger.debug("Reading XLSX file: {}", filePath);

            // Get the first sheet
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) {
                return new DataImportResult(true, "No worksheets found in file", null, null);
            }

            // Check if sheet has any rows
            if (sheet.getLastRowNum() < 0) {
                return new DataImportResult(true, "Worksheet is empty", null, null);
            }

            // Process header row
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                return new DataImportResult(true, "Header row is missing", null, null);
            }

            // Validate minimum column count (seconds, nanos, at least one data column)
            int columnCount = headerRow.getLastCellNum();
            if (columnCount < 3) {
                return new DataImportResult(true, "File must have at least 3 columns (seconds, nanos, and at least one data column)", null, null);
            }

            logger.debug("Found {} columns in header row", columnCount);

            // Extract data column names (skip first two timestamp columns)
            List<String> dataColumnNames = new ArrayList<>();
            for (int colIndex = 2; colIndex < columnCount; colIndex++) {
                Cell headerCell = headerRow.getCell(colIndex);
                if (headerCell == null || headerCell.getCellType() == CellType.BLANK) {
                    return new DataImportResult(true, "Header cell is empty at column " + (colIndex + 1), null, null);
                }
                String columnName = getCellStringValue(headerCell);
                if (columnName.trim().isEmpty()) {
                    return new DataImportResult(true, "Header cell is empty at column " + (colIndex + 1), null, null);
                }
                dataColumnNames.add(columnName);
            }

            // Initialize data structures
            List<Timestamp> timestamps = new ArrayList<>();
            List<List<DataValue>> columnDataLists = new ArrayList<>();
            for (int i = 0; i < dataColumnNames.size(); i++) {
                columnDataLists.add(new ArrayList<>());
            }

            // Process data rows
            int dataRowCount = 0;
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row dataRow = sheet.getRow(rowIndex);
                if (dataRow == null) {
                    return new DataImportResult(true, "Empty row found at row " + (rowIndex + 1), null, null);
                }

                // Validate row has correct number of columns
                if (dataRow.getLastCellNum() != columnCount) {
                    return new DataImportResult(true, "Row " + (rowIndex + 1) + " has " + dataRow.getLastCellNum() + " columns, expected " + columnCount, null, null);
                }

                // Extract timestamp from first two columns
                try {
                    // Seconds column (column 0)
                    Cell secondsCell = dataRow.getCell(0);
                    if (secondsCell == null || secondsCell.getCellType() == CellType.BLANK) {
                        return new DataImportResult(true, "Seconds cell is empty at row " + (rowIndex + 1), null, null);
                    }
                    long epochSeconds = getCellLongValue(secondsCell);

                    // Nanoseconds column (column 1)
                    Cell nanosCell = dataRow.getCell(1);
                    if (nanosCell == null || nanosCell.getCellType() == CellType.BLANK) {
                        return new DataImportResult(true, "Nanoseconds cell is empty at row " + (rowIndex + 1), null, null);
                    }
                    long nanoseconds = getCellLongValue(nanosCell);

                    // Create timestamp
                    Timestamp timestamp = Timestamp.newBuilder()
                            .setEpochSeconds(epochSeconds)
                            .setNanoseconds(nanoseconds)
                            .build();
                    timestamps.add(timestamp);

                } catch (NumberFormatException e) {
                    return new DataImportResult(true, "Invalid timestamp value at row " + (rowIndex + 1) + ": " + e.getMessage(), null, null);
                }

                // Process data columns
                for (int colIndex = 2; colIndex < columnCount; colIndex++) {
                    Cell dataCell = dataRow.getCell(colIndex);
                    if (dataCell == null || dataCell.getCellType() == CellType.BLANK) {
                        return new DataImportResult(true, "Empty cell found at row " + (rowIndex + 1) + ", column " + (colIndex + 1), null, null);
                    }

                    DataValue dataValue = createDataValueFromCell(dataCell);
                    if (dataValue == null) {
                        return new DataImportResult(true, "Unsupported cell type at row " + (rowIndex + 1) + ", column " + (colIndex + 1), null, null);
                    }

                    columnDataLists.get(colIndex - 2).add(dataValue);
                }

                dataRowCount++;
            }

            logger.debug("Processed {} data rows", dataRowCount);

            // Build DataColumn objects
            List<DataColumn> dataColumns = new ArrayList<>();
            for (int i = 0; i < dataColumnNames.size(); i++) {
                DataColumn dataColumn = DataColumn.newBuilder()
                        .setName(dataColumnNames.get(i))
                        .addAllDataValues(columnDataLists.get(i))
                        .build();
                dataColumns.add(dataColumn);
            }

            logger.debug("Successfully imported data from XLSX file: {} timestamps, {} columns", timestamps.size(), dataColumns.size());
            return new DataImportResult(false, "", timestamps, dataColumns);

        } catch (IOException e) {
            logger.error("IOException reading XLSX file {}: {}", filePath, e.getMessage());
            return new DataImportResult(true, "Error reading file: " + e.getMessage(), null, null);
        } catch (Exception e) {
            logger.error("Unexpected error processing XLSX file {}: {}", filePath, e.getMessage());
            return new DataImportResult(true, "Unexpected error: " + e.getMessage(), null, null);
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
