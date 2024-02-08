package com.ospreydcs.dp.service.common.config;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class ConfigurationManagerTestBase {

    protected static final String OVERRIDE_CONFIG_FILENAME = "override.dp-config.yml";
    protected static final String TARGET_FILE = "/tmp/ConfigurationManagerOverrideTest.dp-config.yaml";

    protected static class ConfigurationManagerDerived extends ConfigurationManager {

        public static void setInstance(String overrideConfigFile, Properties overrideProperties) {
            ConfigurationManagerDerived testInstance = new ConfigurationManagerDerived();
            testInstance.initialize(overrideConfigFile, overrideProperties);
            instance = testInstance;
        }

        public static void resetInstance() {
            instance = null;
        }

    }

    protected static void copyOverrideConfigFile() throws Exception {
        // copy the override config file to target location
        InputStream inputStream =
                ConfigurationManagerOverrideConfigFileCmdLineTest.class.getClassLoader().getResourceAsStream(OVERRIDE_CONFIG_FILENAME);
        File targetFile = new File(TARGET_FILE);
        Files.copy(inputStream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

}
