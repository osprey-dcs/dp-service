package com.ospreydcs.dp.service.annotation.handler.model;

import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.*;

public class ExportConfigurationTest {

    /*
     * Covers valid configuration using export-related config resources in application.yml to set export directories.
     * serverMountPoint is specified in the config file.
     * shareMountPoint is not specified in the config file, so it defaults to value of serverMountPoint.
     * baseUrl is not specified, so fileUrl is null.
     *
     */
    @Test
    public void testMinimumConfiguration() {

        // This test assumes application.yml in test/resources includes the following:
        //
        // Export:
        //  serverMountPoint: /tmp
        //  shareMountPoint:
        //  urlBase:

        final ExportConfiguration exportConfiguration = new ExportConfiguration();

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final String dateDirectory = LocalDate.now().format(dateFormatter);

        assertTrue(exportFilePaths.valid);
        assertEquals(
                "66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.filename);
        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/",
                exportFilePaths.serverDirectoryPath);
        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.shareFilePath);
        assertNull(exportFilePaths.fileUrl);
    }

    /*
     * Covers invalid configuration, which is flagged when the serverMountPoint config resource is not specified.
     */
    @Test
    public void testInvalidConfigurationUnspecifiedServerMountPoint() {

        final ExportConfiguration exportConfiguration =
                new ExportConfiguration("", "", "", true);

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        assertFalse(exportFilePaths.valid);
        assertEquals("serverMountPoint is not specified in configuration", exportFilePaths.validMsg);
    }

    /*
     * Covers invalid configuration, flagged because specified shareMountPoint doesn't exist.
     */
    @Test
    public void testInvalidConfigurationNonexistentShareMountPoint() {

        final ExportConfiguration exportConfiguration =
                new ExportConfiguration(
                        "/tmp",
                        "/share",
                        "",
                        true);

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        assertFalse(exportFilePaths.valid);
        assertTrue(exportFilePaths.validMsg.contains("shareMountPoint does not exist or is not a directory"));
    }

    /*
     * Covers configuration that specifies explicit values for serverMountPoint, shareMountPoint, and urlBase.
     */
    @Test
    public void testFullConfiruation() {

        final ExportConfiguration exportConfiguration = new ExportConfiguration(
                "/tmp",
                "/share",
                "https://www.ospreydcs.com/dp/export",
                false);

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final String dateDirectory = LocalDate.now().format(dateFormatter);

        assertTrue(exportFilePaths.valid);
        assertEquals(
                "66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.filename);
        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/",
                exportFilePaths.serverDirectoryPath);
        assertEquals(
                "/share/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.shareFilePath);
        assertEquals(
                "https://www.ospreydcs.com/dp/export/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.fileUrl);

    }

}
