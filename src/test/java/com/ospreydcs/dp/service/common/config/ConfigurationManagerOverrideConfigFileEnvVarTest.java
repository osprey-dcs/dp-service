package com.ospreydcs.dp.service.common.config;

import org.junit.*;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConfigurationManagerOverrideConfigFileEnvVarTest extends ConfigurationManagerTestBase {

    private static ConfigurationManager configMgr = null;

    @BeforeClass
    public static void setUp() throws Exception {
        // copy the override config file to target location
        copyOverrideConfigFile();
        ConfigurationManagerTestBase.ConfigurationManagerDerived.setInstance(
                TARGET_FILE, System.getProperties());
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
