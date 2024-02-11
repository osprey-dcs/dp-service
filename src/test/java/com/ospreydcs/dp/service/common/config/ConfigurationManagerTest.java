package com.ospreydcs.dp.service.common.config;

import org.junit.*;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConfigurationManagerTest extends ConfigurationManagerTestBase {

    private static ConfigurationManager configMgr;

    @BeforeClass
    public static void setUp() throws Exception {
        configMgr = ConfigurationManagerDerived.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        ConfigurationManagerDerived.resetInstance();
    }

    @Test
    public void test01GetConfig() {
        assertTrue("unexpected intValue",
                configMgr.getConfigInteger("Level1.Level2.intValue") == 60);
        assertTrue("unexpected longValue",
                configMgr.getConfigLong("Level1.Level2.longValue") == 1698767462L);
        assertTrue("unexpected floatValue",
                configMgr.getConfigFloat("Level1.Level2.floatValue") == 42.42F);
        assertTrue("unexpected doubleValue",
                configMgr.getConfigDouble("Level1.Level2.doubleValue") == 3.14159265D);
        assertTrue("unexpected stringValue",
                configMgr.getConfigString("Level1.Level2.stringValue").equals("junk"));
        assertTrue("unexpected booleanValueTrue",
                configMgr.getConfigBoolean("Level1.Level2.booleanValueTrue"));
        assertFalse("unexpected booleanValueFalse",
                configMgr.getConfigBoolean("Level1.Level2.booleanValueFalse"));
        assertNull("unexpected noValue",
                configMgr.getConfigString("Level1.Level2.noValue"));
        assertNull("unexpected value for missing key",
                configMgr.getConfigString("Level1.Level2.missingValue"));
    }

    @Test
    public void test02GetConfigWithDefault() {
        assertTrue("unexpected defaultInt",
                configMgr.getConfigInteger("Level1.Level2.defaultInt", 42) == 42);
        assertTrue("unexpected defaultLong",
                configMgr.getConfigLong("Level1.Level2.defaultLong", 1698865250L) == 1698865250L);
        assertTrue("unexpected defaultFloat",
                configMgr.getConfigFloat("Level1.Level2.defaultFloat", 2.71828F) == 2.71828F);
        assertTrue("unexpected defaultDouble",
                configMgr.getConfigDouble("Level1.Level2.defaultDouble", 2.7182818284D) == 2.7182818284D);
        assertTrue("unexpected defaultString",
                configMgr.getConfigString("Level1.Level2.defaultString", "stuff").equals("stuff"));
        assertTrue("unexpected defaultBoolean",
                configMgr.getConfigBoolean("Level1.Level2.defaultBoolean", true));
    }

    @Test
    public void test03GetInstance() {
        assertTrue("new instance created by getInstance()",
                configMgr == ConfigurationManager.getInstance());
    }

}