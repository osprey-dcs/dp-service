package com.ospreydcs.dp.service.common.config;

import com.mongodb.ConnectionString;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Credentials must not reach the log (issue #316). Covers the redaction rules directly, and the
 * configuration manager's actual log output at debug level, which is where a new log line that
 * forgets the redactor would show up.
 */
public class SecretRedactorTest extends ConfigurationManagerTestBase {

    private static final String PASSWORD = "S3ntinel-Pw";

    /** Appears only as the default of an unresolved placeholder in the test's config file. */
    private static final String FILE_PASSWORD = "F1le-Default-Pw";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testUriUserinfoIsRemovedAndTheRestIsKept() {
        assertEquals(
                "mongodb://<redacted>@mongo-0.ns.svc:27017,mongo-1.ns.svc:27017/?replicaSet=rs0&authSource=admin",
                SecretRedactor.redact("mongodb://dbAdmin:" + PASSWORD
                        + "@mongo-0.ns.svc:27017,mongo-1.ns.svc:27017/?replicaSet=rs0&authSource=admin"));
        assertEquals("mongodb+srv://<redacted>@cluster.example.net/dp",
                SecretRedactor.redact("mongodb+srv://u:" + PASSWORD + "@cluster.example.net/dp"));
    }

    @Test
    public void testPasswordContainingAnAtSignIsRemovedWhole() {
        final String redacted = SecretRedactor.redact("mongodb://u:p@ss@" + PASSWORD + "@host:27017/");
        assertEquals("mongodb://<redacted>@host:27017/", redacted);
    }

    /**
     * A password the operator forgot to percent-encode. Base64-generated passwords routinely contain
     * {@code /} and {@code +}; the driver rejects such a URI, but only after the connect string has
     * been logged, so the redactor must not stop at the first {@code /} or {@code ?}.
     */
    @Test
    public void testUnescapedSlashOrQuestionMarkInPasswordIsRemovedWhole() {
        assertEquals("mongodb://<redacted>@host:27017/?authSource=admin",
                SecretRedactor.redact("mongodb://u:ab/" + PASSWORD + "+Ef==@host:27017/?authSource=admin"));
        assertEquals("mongodb://<redacted>@host:27017/",
                SecretRedactor.redact("mongodb://u:ab?" + PASSWORD + "@host:27017/"));
        assertEquals("mongodb://<redacted>@host/",
                SecretRedactor.redact("mongodb://u:" + PASSWORD + "/#@host/"));
    }

    /** An unescaped space in the password must not end the match early either. */
    @Test
    public void testUnescapedWhitespaceInPasswordIsRemovedWhole() {
        assertEquals("mongodb://<redacted>@host:27017/",
                SecretRedactor.redact("mongodb://u:ab " + PASSWORD + "@host:27017/"));
        assertEquals("mongodb://<redacted>@host:27017/",
                SecretRedactor.redact("mongodb://u:ab\t" + PASSWORD + "\n@host:27017/"));
    }

    /**
     * MONGODB-AWS carries a session token in a URI option, under the {@code MongoClient.uri} key,
     * so the secret-named-key rule cannot catch it. Ordinary options must survive.
     */
    @Test
    public void testSecretBearingUriOptionsAreRedacted() {
        assertEquals(
                "mongodb://<redacted>@cluster.example.net/?authMechanism=MONGODB-AWS"
                        + "&authMechanismProperties=<redacted>&replicaSet=rs0&authSource=$external",
                SecretRedactor.redact("mongodb://AKIAEXAMPLE:" + PASSWORD
                        + "@cluster.example.net/?authMechanism=MONGODB-AWS"
                        + "&authMechanismProperties=AWS_SESSION_TOKEN:" + PASSWORD + "/+Tk=="
                        + "&replicaSet=rs0&authSource=$external"));
        // Without userinfo (credentials from the environment), and as the first option.
        assertEquals("mongodb://host/?authMechanismProperties=<redacted>",
                SecretRedactor.redact("mongodb://host/?authMechanismProperties=SERVICE_NAME:mongodb,"
                        + "AWS_SESSION_TOKEN:" + PASSWORD));
        // Any option whose name names a secret.
        assertEquals("mongodb://host/?tls=true&tlsCertificateKeyFilePassword=<redacted>",
                SecretRedactor.redact("mongodb://host/?tls=true&tlsCertificateKeyFilePassword=" + PASSWORD));
        // Options carrying no secret are untouched.
        assertEquals("mongodb://host:27017/?replicaSet=rs0&authSource=admin&w=majority",
                SecretRedactor.redact("mongodb://host:27017/?replicaSet=rs0&authSource=admin&w=majority"));
    }

    @Test
    public void testUnresolvedPlaceholderDefaultIsRedacted() {
        final String redacted = SecretRedactor.redact("${DP_MONGO_DB_URI:mongodb://admin:" + PASSWORD + "@localhost:27017/}");
        assertFalse(redacted.contains(PASSWORD));
        assertTrue(redacted.contains("localhost:27017"));
    }

    @Test
    public void testValuesWithoutCredentialsAreUnchanged() {
        assertEquals("mongodb://localhost:27017/", SecretRedactor.redact("mongodb://localhost:27017/"));
        assertEquals("localhost:50051", SecretRedactor.redact("localhost:50051"));
        assertEquals("ops@example.com", SecretRedactor.redact("ops@example.com"));
        assertNull(SecretRedactor.redact(null));
    }

    @Test
    public void testSecretNamedKeyIsReplacedEntirely() {
        assertEquals(SecretRedactor.REDACTED, SecretRedactor.redact("Export.s3SecretKey", PASSWORD));
        assertEquals(SecretRedactor.REDACTED, SecretRedactor.redact("dp.Some.dbPassword", PASSWORD));
        assertEquals("7", SecretRedactor.redact("QueryHandler.numWorkers", 7));
    }

    @Test
    public void testRedactedCopyLeavesTheInputUntouched() {
        final Map<String, Object> input = new java.util.HashMap<>();
        input.put("MongoClient.uri", "mongodb://u:" + PASSWORD + "@h/");
        final Map<String, String> copy = SecretRedactor.redactedCopy(input);
        assertEquals("mongodb://<redacted>@h/", copy.get("MongoClient.uri"));
        assertEquals("mongodb://u:" + PASSWORD + "@h/", input.get("MongoClient.uri"));
    }

    /**
     * The whole configuration manager, logging at debug: no line may contain a credential from a
     * command-line override, from a secret-named key, or from a placeholder default in the config
     * file, while the value handed to callers stays intact.
     *
     * <p>The config file is written here rather than taken from the classpath: the test
     * {@code application.yml} shadows the main one and carries no {@code MongoClient.uri}, so the
     * main file's {@code ${DP_MONGO_DB_URI:mongodb://admin:admin@...}} default never reaches this
     * test. The file's {@code MongoClient.uri} placeholder is logged unresolved by the first debug
     * line and then overridden from the command line; {@code Other.archiveUri} is left to resolve to
     * its default, so the resolved form reaches the final info line.
     */
    @Test
    public void testConfigurationManagerLogsNoCredentials() throws Exception {
        final File configFile = tempFolder.newFile("secret-redactor-test.yml");
        Files.writeString(configFile.toPath(), String.join("\n",
                "MongoClient:",
                "  uri: \"${SECRET_REDACTOR_TEST_UNSET_URI:mongodb://fileUser:" + FILE_PASSWORD
                        + "@file-db.example.net:27017/}\"",
                "Other:",
                "  archiveUri: \"${SECRET_REDACTOR_TEST_UNSET_ARCHIVE:mongodb://fileUser:" + FILE_PASSWORD
                        + "@archive.example.net:27017/}\"",
                ""), StandardCharsets.UTF_8);

        final String uri = "mongodb://dbAdmin:" + PASSWORD + "@db.example.net:27017/?authSource=admin";
        final Properties properties = new Properties();
        properties.setProperty("dp.MongoClient.uri", uri);
        properties.setProperty("dp.Some.apiToken", "T0ken-" + PASSWORD);
        properties.setProperty("unrelated.jdbc.url", "jdbc://x:" + PASSWORD + "@other/");

        final List<String> messages = new CopyOnWriteArrayList<>();
        final String loggerName = ConfigurationManager.class.getName();
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = context.getConfiguration();
        final AbstractAppender appender =
                new AbstractAppender("secretRedactorCapture", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        appender.start();
        final LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.DEBUG, false);
        loggerConfig.addAppender(appender, Level.DEBUG, null);
        configuration.addLogger(loggerName, loggerConfig);
        context.updateLoggers();
        try {
            ConfigurationManagerDerived.setInstance(configFile.getAbsolutePath(), properties);
            assertEquals("callers must still get the real value",
                    uri, ConfigurationManager.getInstance().getConfigString("MongoClient.uri"));
        } finally {
            ConfigurationManagerDerived.resetInstance();
            configuration.removeLogger(loggerName);
            context.updateLoggers();
            appender.stop();
        }

        assertTrue("expected the debug and info configuration lines to be captured", messages.size() >= 4);
        boolean sawHost = false;
        boolean sawPlaceholder = false;
        boolean sawResolvedDefault = false;
        for (String message : messages) {
            assertFalse("log line contains the password: " + message, message.contains(PASSWORD));
            assertFalse("log line contains the config file's default credentials: " + message,
                    message.contains(FILE_PASSWORD));
            sawHost |= message.contains("@db.example.net:27017");
            sawPlaceholder |= message.contains("@file-db.example.net:27017");
            sawResolvedDefault |= message.contains("Other.archiveUri=mongodb://<redacted>@archive.example.net");
        }
        assertTrue("the redacted override URI should still show the host", sawHost);
        // Guards against the test passing vacuously: the file's credentials must have been logged
        // (redacted) in both forms, or the assertions above checked nothing.
        assertTrue("expected the unresolved placeholder to be logged", sawPlaceholder);
        assertTrue("expected the resolved placeholder default to be logged", sawResolvedDefault);
    }

    /**
     * Mongo client init failures are logged with the driver's exception message, so that message
     * must not echo the password. This pins the driver behavior the logging relies on.
     */
    @Test
    public void testDriverRejectionMessagesDoNotEchoThePassword() {
        final String[] malformed = {
                "mongodb://u:" + PASSWORD + "@host:notaport/",
                "mongodb://u:" + PASSWORD + "@host:27017/?replicaSet=rs0&w=notanumber&tls=maybe",
                "mongodb://u:" + PASSWORD + "@host:27017/db?authMechanism=NOPE",
                "mongodb://u:" + PASSWORD + "@/",
                "mongod://u:" + PASSWORD + "@host/",
        };
        for (String uri : malformed) {
            try {
                new ConnectionString(uri);
            } catch (RuntimeException ex) {
                for (Throwable t = ex; t != null; t = t.getCause()) {
                    assertFalse("driver message echoes the password for " + SecretRedactor.redact(uri)
                                    + ": " + t.getMessage(),
                            String.valueOf(t.getMessage()).contains(PASSWORD));
                }
            }
        }
    }
}
