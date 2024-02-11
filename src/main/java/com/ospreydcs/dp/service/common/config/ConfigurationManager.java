package com.ospreydcs.dp.service.common.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

import static java.util.Collections.singletonMap;

public class ConfigurationManager {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final String CONFIG_PROPERTY_NAME = "dp.config";
    public static final String DP_PROPERTY_PREFIX = "dp.";
    public static final String CONFIG_ENVIRONMENT_NAME = "DP.CONFIG";
    public static final String DEFAULT_CONFIG_FILE = "application.yml";

    protected static volatile ConfigurationManager instance;
    private static Object mutex = new Object();

    protected Map<String, Object> configMap;

    protected ConfigurationManager() {
    }

    public static ConfigurationManager getInstance() {
        ConfigurationManager result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null) {
                    instance = result = new ConfigurationManager();
                    instance.initialize();
                }
            }
        }
        return result;
    }

    protected void initialize(String overrideFile, Properties systemProperties) {

        InputStream inputStream = null;
        if (overrideFile != null && !overrideFile.isBlank()) {
            try {
                inputStream = new FileInputStream(overrideFile);
            } catch (FileNotFoundException ex) {
                LOGGER.error("initialize failed to read override config file: {} message: {}",
                        overrideFile, ex.getMessage());
                configMap = new HashMap<>();
                return;
            }
            if (inputStream == null) {
                LOGGER.error("initialize null inputStream reading override config file: {}",
                        overrideFile);
                configMap = new HashMap<>();
                return;
            }

        } else {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE);
            if (inputStream == null) {
                LOGGER.error("initialize null inputStream reading default config file: {} message: {}",
                        DEFAULT_CONFIG_FILE);
                configMap = new HashMap<>();
                return;
            }

            LOGGER.info("initialize using default config file: {}", DEFAULT_CONFIG_FILE);
        }

        // build map of property values from config file, creating a flattened key string from the config hierarchy
        Yaml yaml = new Yaml();
        Map<String, Object> yamlMap = yaml.load(inputStream);
        Map<String, Object> resultMap = getFlattenedMap(yamlMap);
        LOGGER.debug("initialize config file properties: {}", resultMap);

        // Override config properties from command line.
        // Note that overrides passed on the command line must be set using "-D" as VM arguments so that they
        // appear on the command line before the main class.  Otherwise, they are passed as arguments in argv to main.
        // Example: "java -Ddp.GrpcServer.port=50052 com.ospreydcs.dp.ingest.server.IngestionGrpcServer".
        LOGGER.debug("initialize property overrides: {}", systemProperties);
        for (var entry : systemProperties.entrySet()) {
            final String propertyKey = (String) entry.getKey();
            if (propertyKey.startsWith(DP_PROPERTY_PREFIX) && !propertyKey.equals(CONFIG_PROPERTY_NAME)) {
                final String propertyValue = (String) entry.getValue();
                if (propertyKey.length() > DP_PROPERTY_PREFIX.length()) {
                    final String dpKey = propertyKey.substring(DP_PROPERTY_PREFIX.length());
                    resultMap.put(dpKey, propertyValue);
                    LOGGER.info("initialize overriding property from command line: {} value: {}",
                            dpKey, propertyValue);
                }
            }
        }

        LOGGER.info("initialize dp configuration: {}", resultMap);
        configMap = resultMap;
    }

    private void initialize() {

        String overrideCmdLine = System.getProperty(CONFIG_PROPERTY_NAME);
        String overrideEnv = System.getenv(CONFIG_ENVIRONMENT_NAME);
        String overrideFile = null;
        if (overrideCmdLine != null && !overrideCmdLine.isBlank()) {
            overrideFile = overrideCmdLine;
            LOGGER.info("initialize using command line config file override: {}", overrideCmdLine);
        } else if (overrideEnv != null && !overrideEnv.isBlank()) {
            overrideFile = overrideEnv;
            LOGGER.info("initialize using environment variable config file override: {}", overrideEnv);
        }

        initialize(overrideFile, System.getProperties());
    }

    private static final Map<String, Object> getFlattenedMap(Map<String, Object> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        buildFlattenedMap(result, source, null);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void buildFlattenedMap(Map<String, Object> result, Map<String, Object> source, String path) {
        source.forEach((key, value) -> {
            if (path != null && !path.isBlank())
                key = path + (key.startsWith("[") ? key : '.' + key);
            if (value instanceof String) {
                result.put(key, value);
            } else if (value instanceof Map) {
                buildFlattenedMap(result, (Map<String, Object>) value, key);
            } else if (value instanceof Collection) {
                int count = 0;
                for (Object object : (Collection<?>) value)
                    buildFlattenedMap(result, singletonMap("[" + (count++) + "]", object), key);
            } else {
                result.put(key, value != null ? "" + value : "");
            }
        });
    }

    public String getConfigString(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return value;
        }
    }

    public String getConfigString(String key, String defaultValue) {
        String configValue = getConfigString(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

    public Integer getConfigInteger(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return Integer.valueOf(value);
        }
    }

    public int getConfigInteger(String key, Integer defaultValue) {
        Integer configValue = getConfigInteger(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

    public Boolean getConfigBoolean(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return Boolean.valueOf(value);
        }
    }

    public boolean getConfigBoolean(String key, boolean defaultValue) {
        Boolean configValue = getConfigBoolean(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

    public Long getConfigLong(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return Long.valueOf(value);
        }
    }

    public long getConfigLong(String key, Long defaultValue) {
        Long configValue = getConfigLong(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

    public Float getConfigFloat(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return Float.valueOf(value);
        }
    }

    public float getConfigFloat(String key, Float defaultValue) {
        Float configValue = getConfigFloat(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

    public Double getConfigDouble(String key) {
        String value = (String) configMap.get(key);
        if (value == null || value.isBlank()) {
            return null;
        } else {
            return Double.valueOf(value);
        }
    }

    public Double getConfigDouble(String key, Double defaultValue) {
        Double configValue = getConfigDouble(key);
        if (configValue == null) {
            return defaultValue;
        } else {
            return configValue;
        }
    }

}
