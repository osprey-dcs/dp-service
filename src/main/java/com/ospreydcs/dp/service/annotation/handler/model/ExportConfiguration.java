package com.ospreydcs.dp.service.annotation.handler.model;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static java.io.File.separator;
import static java.io.File.separatorChar;

public class ExportConfiguration {

    public static class ExportFilePaths {

        public final String serverFilePath;
        public final String shareFilePath;
        public final String fileUrl;
        public final boolean valid;

        public ExportFilePaths(boolean valid, String serverFilePath, String shareFilePath, String fileUrl) {
            this.serverFilePath = serverFilePath;
            this.shareFilePath = shareFilePath;
            this.fileUrl = fileUrl;
            this.valid = valid;
        }
    }

    // constants
    public static final String FILE_EXTENSION_HDF5 = "h5";

    // configuration
    public static final String CFG_KEY_EXPORT_SERVER_MOUNT_POINT = "Export.serverMountPoint";
    public static final String CFG_KEY_EXPORT_SHARE_MOUNT_POINT = "Export.shareMountPoint";
    public static final String CFG_KEY_EXPORT_URL_BASE = "Export.urlBase";

    // instance variables
    public String serverMountPoint = null;
    public String shareMountPoint = null;
    public String urlBase = null;
    public boolean valid = false;

    public ExportConfiguration() {
        final String serverMountPointConfig = configMgr().getConfigString(CFG_KEY_EXPORT_SERVER_MOUNT_POINT);
        final String shareMountPointConfig = configMgr().getConfigString(CFG_KEY_EXPORT_SHARE_MOUNT_POINT);
        final String urlBaseConfig = configMgr().getConfigString(CFG_KEY_EXPORT_URL_BASE);
        initialize(serverMountPointConfig, shareMountPointConfig, urlBaseConfig);
    }

    public ExportConfiguration(
            String serverMountPoint,
            String shareMountPoint,
            String urlBase
    ) {
        initialize(serverMountPoint, shareMountPoint, urlBase);
    }

    private void initialize(
            String serverMountPoint,
            String shareMountPoint,
            String urlBase
    ) {
        if (serverMountPoint == null || serverMountPoint.isEmpty()) {
            valid = false;
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

        valid = true;
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
            return new ExportFilePaths(false, null, null, null);
        }

        // generate filename
        final String filename = objectId + "." + extension;

        // get subdirectory path within balanced directory structure
        final String subdirectory = getExportFileSubdirectory(objectId);

        // generate server file path
        final String serverFilePath = serverMountPoint + subdirectory + filename;

        // generate share file path
        String shareFilePath;
        if (shareMountPoint != null) {
            shareFilePath = shareMountPoint + subdirectory + filename;
        } else {
            shareFilePath = serverFilePath;
        }

        // generate url file path
        String fileUrl = null;
        if (urlBase != null) {
            fileUrl = urlBase + subdirectory + filename;
        }

        return new ExportFilePaths(true, serverFilePath, shareFilePath, fileUrl);
    }
}
