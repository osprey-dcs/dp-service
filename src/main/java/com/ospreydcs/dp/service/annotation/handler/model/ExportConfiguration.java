package com.ospreydcs.dp.service.annotation.handler.model;

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

    public String serverMountPoint = null;
    public String shareMountPoint = null;
    public String urlBase = null;
    public boolean valid = false;

    public ExportConfiguration(
        String serverMountPoint,
        String shareMountPoint,
        String urlBase
    ) {
        if (serverMountPoint == null || serverMountPoint.isEmpty()) {
            valid = false;
            return;
        }
        // add trailing slash if needed to each of the three
        this.serverMountPoint =
                (serverMountPoint.charAt(serverMountPoint.length()-1) != separatorChar) 
                        ? serverMountPoint : serverMountPoint + separatorChar;
        if (shareMountPoint != null && !shareMountPoint.isEmpty()) {
            this.shareMountPoint =
                    (shareMountPoint.charAt(shareMountPoint.length() - 1) != separatorChar)
                            ? shareMountPoint : shareMountPoint + separatorChar;
        }
        if (urlBase != null && !urlBase.isEmpty()) {
            this.urlBase =
                    (urlBase.charAt(urlBase.length() - 1) != separatorChar)
                            ? urlBase : urlBase + separatorChar;
        }
        valid = true;
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

        final String parentDirectory = objectId.substring(0, 2) + separator;
        final String childDirectory = objectId.substring(2, 4) + separator;
        return dateDirectory + parentDirectory + childDirectory;
    }

    public ExportFilePaths getExportFilePaths(String objectId) {

        if (!valid) {
            return new ExportFilePaths(false, null, null, null);
        }

        // get subdirectory path within balanced directory structure
        final String subdirectory = getExportFileSubdirectory(objectId);

        // generate server file path
        final String serverFilePath = serverMountPoint + subdirectory + objectId;

        // generate share file path
        String shareFilePath;
        if (shareMountPoint != null) {
            shareFilePath = shareMountPoint + subdirectory + objectId;
        } else {
            shareFilePath = serverFilePath;
        }

        // generate url file path
        String fileUrl = null;
        if (urlBase != null) {
            fileUrl = urlBase + subdirectory + objectId;
        }

        return new ExportFilePaths(true, serverFilePath, shareFilePath, fileUrl);
    }
}
