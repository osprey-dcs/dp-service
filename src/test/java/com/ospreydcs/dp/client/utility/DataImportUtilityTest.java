package com.ospreydcs.dp.client.utility;

import com.ospreydcs.dp.client.result.DataImportResult;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class DataImportUtilityTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testImportXlsxDataSuccess() throws IOException {
        // Create a test XLSX file
        File testFile = tempFolder.newFile("test_data.xlsx");
        createTestXlsxFile(testFile);

        // Test the import
        DataImportResult result = DataImportUtility.importXlsxData(testFile.getAbsolutePath());

        // Verify success
        assertNotNull(result);
        assertFalse(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg.isEmpty());
        assertNotNull(result.timestamps);
        assertNotNull(result.columns);

        // Verify data structure
        assertEquals(2, result.timestamps.size());
        assertEquals(2, result.columns.size());

        // Verify timestamps
        Timestamp timestamp1 = result.timestamps.get(0);
        assertEquals(1000000000L, timestamp1.getEpochSeconds());
        assertEquals(123456789L, timestamp1.getNanoseconds());

        Timestamp timestamp2 = result.timestamps.get(1);
        assertEquals(1000000001L, timestamp2.getEpochSeconds());
        assertEquals(987654321L, timestamp2.getNanoseconds());

        // Verify data columns
        DataColumn column1 = result.columns.get(0);
        assertEquals("PV1", column1.getName());
        assertEquals(2, column1.getDataValuesCount());
        assertEquals(3.14, column1.getDataValues(0).getDoubleValue(), 0.001);
        assertEquals(2.71, column1.getDataValues(1).getDoubleValue(), 0.001);

        DataColumn column2 = result.columns.get(1);
        assertEquals("PV2", column2.getName());
        assertEquals(2, column2.getDataValuesCount());
        assertEquals("test1", column2.getDataValues(0).getStringValue());
        assertEquals("test2", column2.getDataValues(1).getStringValue());
    }

    @Test
    public void testImportXlsxDataFileNotFound() {
        String nonExistentFile = tempFolder.getRoot().getAbsolutePath() + "/nonexistent.xlsx";
        DataImportResult result = DataImportUtility.importXlsxData(nonExistentFile);

        assertNotNull(result);
        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg.contains("does not exist"));
        assertNull(result.timestamps);
        assertNull(result.columns);
    }

    @Test
    public void testImportXlsxDataNullFilePath() {
        DataImportResult result = DataImportUtility.importXlsxData(null);

        assertNotNull(result);
        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg.contains("null or empty"));
        assertNull(result.timestamps);
        assertNull(result.columns);
    }

    @Test
    public void testImportXlsxDataEmptyFilePath() {
        DataImportResult result = DataImportUtility.importXlsxData("");

        assertNotNull(result);
        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg.contains("null or empty"));
        assertNull(result.timestamps);
        assertNull(result.columns);
    }

    @Test
    public void testImportXlsxDataInsufficientColumns() throws IOException {
        // Create a test XLSX file with insufficient columns
        File testFile = tempFolder.newFile("test_insufficient_columns.xlsx");
        createInsufficientColumnsXlsxFile(testFile);

        DataImportResult result = DataImportUtility.importXlsxData(testFile.getAbsolutePath());

        assertNotNull(result);
        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg.contains("at least 3 columns"));
        assertNull(result.timestamps);
        assertNull(result.columns);
    }

    private void createTestXlsxFile(File file) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(file)) {

            Sheet sheet = workbook.createSheet("TestData");

            // Create header row
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("seconds");
            headerRow.createCell(1).setCellValue("nanos");
            headerRow.createCell(2).setCellValue("PV1");
            headerRow.createCell(3).setCellValue("PV2");

            // Create first data row
            Row dataRow1 = sheet.createRow(1);
            dataRow1.createCell(0).setCellValue(1000000000L);
            dataRow1.createCell(1).setCellValue(123456789L);
            dataRow1.createCell(2).setCellValue(3.14);
            dataRow1.createCell(3).setCellValue("test1");

            // Create second data row
            Row dataRow2 = sheet.createRow(2);
            dataRow2.createCell(0).setCellValue(1000000001L);
            dataRow2.createCell(1).setCellValue(987654321L);
            dataRow2.createCell(2).setCellValue(2.71);
            dataRow2.createCell(3).setCellValue("test2");

            workbook.write(fos);
        }
    }

    private void createInsufficientColumnsXlsxFile(File file) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(file)) {

            Sheet sheet = workbook.createSheet("TestData");

            // Create header row with only 2 columns (insufficient)
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("seconds");
            headerRow.createCell(1).setCellValue("nanos");

            workbook.write(fos);
        }
    }
}