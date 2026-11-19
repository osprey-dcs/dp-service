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
 * <p>Three rules, applied together:
 * <ul>
 *   <li><b>URI userinfo.</b> In any value, the {@code user:password@} part of a URI authority is
 *       replaced, whatever the key is called. The match runs from {@code ://} to the <em>last</em>
 *       {@code @} in the value, deliberately not stopping at {@code /}, {@code ?} or whitespace: a
 *       password an operator forgot to percent-encode (base64-generated passwords routinely contain
 *       {@code /} and {@code +}) would otherwise end the match early and be logged whole, moments
 *       before the driver rejects the URI. A value that cannot be parsed safely is therefore
 *       over-redacted rather than under-redacted: an {@code @} later in the value, in a query option
 *       or in unrelated text after the URI, hides everything up to it. It also covers an unresolved
 *       {@code ${VAR:mongodb://user:password@host}} placeholder, whose default is as much a
 *       credential as a resolved value.</li>
 *   <li><b>Secret-bearing URI options.</b> The value of a query option named
 *       {@code authMechanismProperties}, or whose name names a secret, is replaced. A
 *       MONGODB-AWS connection string carries {@code AWS_SESSION_TOKEN:<token>} there, under the
 *       config key {@code MongoClient.uri}, which the secret-named-key rule cannot see. The whole
 *       {@code authMechanismProperties} value is replaced rather than parsed, so harmless entries
 *       such as {@code SERVICE_NAME} are hidden too.</li>
 *   <li><b>Secret-named keys.</b> A value whose key names a secret is replaced entirely, since it
 *       has no structure to preserve.</li>
 * </ul>
 */
public final class SecretRedactor {

    public static final String REDACTED = "<redacted>";

    private static final Pattern URI_USERINFO = Pattern.compile("(://).*@", Pattern.DOTALL);

    private static final String SECRET_WORDS = "password|passwd|secret|token|credential|apikey|api[-_.]key";

    private static final Pattern SECRET_KEY = Pattern.compile(SECRET_WORDS, Pattern.CASE_INSENSITIVE);

    private static final Pattern SECRET_URI_OPTION = Pattern.compile(
            "([?&;](?:authMechanismProperties|[^=&;\\s]*(?:" + SECRET_WORDS + ")[^=&;\\s]*)=)[^&;\\s]*",
            Pattern.CASE_INSENSITIVE);

    private SecretRedactor() {
    }

    /**
     * Replaces URI userinfo and secret-bearing URI option values in {@code value}; null is returned
     * as null.
     */
    public static String redact(String value) {
        if (value == null) {
            return null;
        }
        final String withoutUserinfo = URI_USERINFO.matcher(value).replaceAll("$1" + REDACTED + "@");
        return SECRET_URI_OPTION.matcher(withoutUserinfo).replaceAll("$1" + REDACTED);
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
