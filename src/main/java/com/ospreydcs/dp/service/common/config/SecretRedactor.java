package com.ospreydcs.dp.service.common.config;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Removes credentials from configuration values before they are logged (issue #316).
 *
 * <p>Every service logged its resolved configuration and its MongoDB connection string at startup,
 * and the connection string carries the database username and password. Anything that logs a
 * configuration value, a system property, or a connection string must pass it through here; the
 * unredacted value is for the code that connects, never for a log line.
 *
 * <p>Two rules, applied together:
 * <ul>
 *   <li><b>URI userinfo.</b> In any value, the {@code user:password@} part of a URI authority is
 *       replaced, whatever the key is called. The match runs to the <em>last</em> {@code @} before
 *       the first {@code /}, {@code ?} or whitespace, which is how the MongoDB driver delimits
 *       userinfo, so a password containing an unescaped {@code @} is still removed whole. It also
 *       covers an unresolved {@code ${VAR:mongodb://user:password@host}} placeholder, whose default
 *       is as much a credential as a resolved value.</li>
 *   <li><b>Secret-named keys.</b> A value whose key names a secret is replaced entirely, since it
 *       has no structure to preserve.</li>
 * </ul>
 */
public final class SecretRedactor {

    public static final String REDACTED = "<redacted>";

    private static final Pattern URI_USERINFO = Pattern.compile("(://)[^/?\\s]*@");

    private static final Pattern SECRET_KEY =
            Pattern.compile("password|passwd|secret|token|credential|apikey|api[-_.]key", Pattern.CASE_INSENSITIVE);

    private SecretRedactor() {
    }

    /** Replaces the userinfo of every URI in {@code value}; null is returned as null. */
    public static String redact(String value) {
        if (value == null) {
            return null;
        }
        return URI_USERINFO.matcher(value).replaceAll("$1" + REDACTED + "@");
    }

    /** As {@link #redact(String)}, but a value under a secret-named key is replaced entirely. */
    public static String redact(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (key != null && SECRET_KEY.matcher(key).find()) {
            return REDACTED;
        }
        return redact(String.valueOf(value));
    }

    /** A sorted copy of {@code map} with every value redacted, for logging; the input is untouched. */
    public static Map<String, String> redactedCopy(Map<?, ?> map) {
        final Map<String, String> result = new TreeMap<>();
        if (map != null) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                final String key = String.valueOf(entry.getKey());
                result.put(key, redact(key, entry.getValue()));
            }
        }
        return result;
    }
}
