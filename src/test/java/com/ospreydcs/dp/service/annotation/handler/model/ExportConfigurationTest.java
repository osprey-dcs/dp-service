package com.ospreydcs.dp.service.annotation.handler.model;

import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static java.io.File.separator;
import static org.junit.Assert.*;

public class ExportConfigurationTest {

    /*
     * Covers valid configuration using export-related config resources in application.yml to set serverFilePath,
     * shareFilePath, and fileUrl in ExportFilePaths.
     */
    @Test
    public void testDefaultConstructor() {

        // This test assumes application.yml in test/resources includes the following:
        //
        // Export:
        //  serverMountPoint: /tmp
        //  shareMountPoint: /share
        //  urlBase: https://www.ospreydcs.com/dp/export

        final ExportConfiguration exportConfiguration = new ExportConfiguration();

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final String dateDirectory = LocalDate.now().format(dateFormatter);

        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.serverFilePath);
        assertEquals(
                "/share/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.shareFilePath);
        assertEquals(
                "https://www.ospreydcs.com/dp/export/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.fileUrl);
    }

    /*
     * Covers invalid configuration, which is flagged when the serverMountPoint config resource is not specified.
     */
    @Test
    public void testInvalidConfiguration() {

        final ExportConfiguration exportConfiguration =
                new ExportConfiguration("", "", "");

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        assertFalse(exportFilePaths.valid);
    }

    /*
     * Covers use of serverFilePath for default value of shareFilePath in ExportFilePaths when latter is not specified.
     * Also checks that fileUrl is null in ExportFilePaths when not specified in configuration.
     */
    @Test
    public void testShareFilePathDefault() {

        final ExportConfiguration exportConfiguration = new ExportConfiguration(
                "/tmp",
                "",
                "");

        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(
                        "66fdb1f90fd0af7705e59e13",
                        ExportConfiguration.FILE_EXTENSION_HDF5);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final String dateDirectory = LocalDate.now().format(dateFormatter);

        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.serverFilePath);
        assertEquals(
                "/tmp/" + dateDirectory + "/9e/13/66fdb1f90fd0af7705e59e13.h5",
                exportFilePaths.shareFilePath);
        assertNull(exportFilePaths.fileUrl);
    }

}
