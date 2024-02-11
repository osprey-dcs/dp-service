package com.ospreydcs.dp.service.common.config;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.Properties;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConfigurationManagerOverridePropertyCmdLineTest extends ConfigurationManagerTestBase {

    private static final String KEY_PREFIX = "dp.";
    private static final String KEY_INT_VALUE = "Level1.Level2.intValue";
    private static final String KEY_STRING_VALUE = "Level1.Level2.stringValue";
    private static final String KEY_NEW_VALUE = "Level1.Level2.newValue";
    private static final String OVERRIDE_INT_VALUE = "12";
    private static final String OVERRIDE_STRING_VALUE = "override";
    private static final String NEW_VALUE = "new";

    private static ConfigurationManager configMgr = null;

    @BeforeClass
    public static void setUp() throws Exception {

        // simulate setting properties via command line using System.setProperty()
        Properties properties = new Properties();
        properties.setProperty(KEY_PREFIX + KEY_INT_VALUE, OVERRIDE_INT_VALUE);
        properties.setProperty(KEY_PREFIX + KEY_STRING_VALUE, OVERRIDE_STRING_VALUE);
        properties.setProperty(KEY_PREFIX + KEY_NEW_VALUE, NEW_VALUE);
        System.out.println(properties);

        ConfigurationManagerDerived.setInstance(null, properties);
        configMgr = ConfigurationManagerDerived.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        ConfigurationManagerDerived.resetInstance();
    }

    @Test
    public void test01OverrideConfigCommandLine() {

        // test values with overrides
        assertTrue("unexpected intValue",
                configMgr.getConfigInteger(KEY_INT_VALUE) == Integer.valueOf(OVERRIDE_INT_VALUE));
        assertTrue("unexpected stringValue",
                configMgr.getConfigString(KEY_STRING_VALUE).equals(OVERRIDE_STRING_VALUE));

        // test value from command line not contained in config file
        assertTrue("unexpected newValue",
                configMgr.getConfigString(KEY_NEW_VALUE).equals(NEW_VALUE));

        // test original values without overrides
        assertTrue("unexpected booleanValueTrue",
                configMgr.getConfigBoolean("Level1.Level2.booleanValueTrue"));
        assertFalse("unexpected booleanValueFalse",
                configMgr.getConfigBoolean("Level1.Level2.booleanValueFalse"));
        assertNull("unexpected noValue",
                configMgr.getConfigString("Level1.Level2.noValue"));
        assertNull("unexpected value for missing key",
                configMgr.getConfigString("Level1.Level2.missingValue"));
    }

}
