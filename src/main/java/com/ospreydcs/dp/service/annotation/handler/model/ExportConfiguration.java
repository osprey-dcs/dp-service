package com.ospreydcs.dp.service.annotation.handler.model;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static java.io.File.separator;
import static java.io.File.separatorChar;

public class ExportConfiguration {

    public static class ExportFilePaths {

        public final String filename;
        public final String serverDirectoryPath;
        public final String shareFilePath;
        public final String fileUrl;
        public final boolean valid;
        public final String validMsg;

        public ExportFilePaths(
                boolean valid,
                String validMsg,
                String filename,
                String serverDirectoryPath,
                String shareFilePath,
                String fileUrl
        ) {
            this.filename = filename;
            this.serverDirectoryPath = serverDirectoryPath;
            this.shareFilePath = shareFilePath;
            this.fileUrl = fileUrl;
            this.valid = valid;
            this.validMsg = validMsg;
        }
    }

    // constants
    public static final String FILE_EXTENSION_HDF5 = "h5";
    public static final String FILE_EXTENSION_CSV = "csv";

    // configuration
    public static final String CFG_KEY_EXPORT_SERVER_MOUNT_POINT = "Export.serverMountPoint";
    public static final String CFG_KEY_EXPORT_SHARE_MOUNT_POINT = "Export.shareMountPoint";
    public static final String CFG_KEY_EXPORT_URL_BASE = "Export.urlBase";

    // instance variables
    public String serverMountPoint = null;
    public String shareMountPoint = null;
    public String urlBase = null;
    public boolean valid = false;
    public String validMsg = "";

    public ExportConfiguration() {
        final String serverMountPointConfig = configMgr().getConfigString(CFG_KEY_EXPORT_SERVER_MOUNT_POINT);
        final String shareMountPointConfig = configMgr().getConfigString(CFG_KEY_EXPORT_SHARE_MOUNT_POINT);
        final String urlBaseConfig = configMgr().getConfigString(CFG_KEY_EXPORT_URL_BASE);
        initialize(serverMountPointConfig, shareMountPointConfig, urlBaseConfig, true);
    }

    public ExportConfiguration(
            String serverMountPoint,
            String shareMountPoint,
            String urlBase,
            boolean testDirectories
    ) {
        initialize(serverMountPoint, shareMountPoint, urlBase, testDirectories);
    }

    private void initialize(
            String serverMountPoint,
            String shareMountPoint,
            String urlBase,
            boolean testDirectories
    ) {
        if (serverMountPoint == null || serverMountPoint.isEmpty()) {
            this.validMsg = "serverMountPoint is not specified in configuration";
            this.valid = false;
            return;
        }

        this.serverMountPoint =
                (serverMountPoint.charAt(serverMountPoint.length() - 1) == separatorChar)
                        ? serverMountPoint : serverMountPoint + separatorChar;

        if (shareMountPoint != null && !shareMountPoint.isEmpty()) {
            this.shareMountPoint =
                    (shareMountPoint.charAt(shareMountPoint.length() - 1) == separatorChar)
                            ? shareMountPoint : shareMountPoint + separatorChar;
        }

        if (urlBase != null && !urlBase.isEmpty()) {
            this.urlBase =
                    (urlBase.charAt(urlBase.length() - 1) == separatorChar)
                            ? urlBase : urlBase + separatorChar;
        }

        // mark configuration invalid if serverMountPoint or shareMountPoint directories don't exist
        if (testDirectories) {

            Path serverMountPointPath = Paths.get(this.serverMountPoint);
            if (Files.notExists(serverMountPointPath)) {
                this.validMsg = "serverMountPoint does not exist or is not a directory: " + this.serverMountPoint;
                this.valid = false;
                return;
            }

            if (this.shareMountPoint != null) {
                Path shareMountPointPath = Paths.get(this.shareMountPoint);
                if (Files.notExists(shareMountPointPath) || !Files.isDirectory(shareMountPointPath)) {
                    this.validMsg = "shareMountPoint does not exist or is not a directory: " + this.shareMountPoint;
                    this.valid = false;
                    return;
                }
            }
        }

        this.valid = true;
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /*
     * Returns a string specifying a subdirectory path within a balanced directory structure using current date and
     * the last 4 characters of an objectId string (assumed to be a 24 digit mongo hex identifier).
     * E.g., for 10/1/24 and objectId=66fdb1f90fd0af7705e59e13, returns "20241001/9e/13/".
     */
    private static String getExportFileSubdirectory(String objectId) {

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final String dateDirectory = LocalDate.now().format(dateFormatter) + separator;

        // Assumption is that this id is a 24 bit mongo unique identifier, but just in case...
        if (objectId.length() < 4) {
            return dateDirectory;
        }

        final String parentDirectory = objectId.substring(objectId.length()-4, objectId.length()-2) + separator;
        final String childDirectory = objectId.substring(objectId.length()-2, objectId.length()) + separator;
        return dateDirectory + parentDirectory + childDirectory;
    }

    public ExportFilePaths getExportFilePaths(String objectId, String extension) {

        if (!valid) {
            return new ExportFilePaths(
                    false,
                    this.validMsg,
                    null,
                    null,
                    null,
                    null);
        }

        // generate filename
        final String filename = objectId + "." + extension;

        // get subdirectory path within balanced directory structure
        final String subdirectory = getExportFileSubdirectory(objectId);

        // generate server file path
        final String serverDirectoryPath = serverMountPoint + subdirectory;

        // generate share file path
        String shareFilePath;
        if (shareMountPoint != null) {
            shareFilePath = shareMountPoint + subdirectory + filename;
        } else {
            shareFilePath = serverDirectoryPath + filename;
        }

        // generate url file path
        String fileUrl = null;
        if (urlBase != null) {
            fileUrl = urlBase + subdirectory + filename;
        }

        return new ExportFilePaths(
                true,
                "",
                filename,
                serverDirectoryPath,
                shareFilePath,
                fileUrl);
    }
}
