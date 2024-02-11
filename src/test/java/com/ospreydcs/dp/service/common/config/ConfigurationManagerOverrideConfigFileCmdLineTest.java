package com.ospreydcs.dp.service.common.config;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConfigurationManagerOverrideConfigFileCmdLineTest extends ConfigurationManagerTestBase {

    private static ConfigurationManager configMgr = null;

    @BeforeClass
    public static void setUp() throws Exception {
        // copy the override config file to target location
        copyOverrideConfigFile();
        // simulate command line parameter by setting property for config file to target location
        Properties properties = new Properties();
        properties.setProperty(ConfigurationManager.CONFIG_PROPERTY_NAME, TARGET_FILE);
        System.out.println(properties);
        ConfigurationManagerDerived.setInstance(TARGET_FILE, properties);
        configMgr = ConfigurationManagerDerived.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        ConfigurationManagerDerived.resetInstance();
    }

    @Test
    public void test01GetConfig() {
        assertTrue("unexpected intValue",
                configMgr.getConfigInteger("Level1.Level2.intValue") == 42);
        assertTrue("unexpected stringValue",
                configMgr.getConfigString("Level1.Level2.stringValue").equals("stuff"));
        assertFalse("unexpected booleanValueTrue",
                configMgr.getConfigBoolean("Level1.Level2.booleanValueTrue"));
    }

}
