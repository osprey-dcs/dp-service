package com.ospreydcs.dp.service.query.handler.paging;

import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Opaque, position-only codec for Query API V2 page tokens (Q1/Q2/Q3). Encodes a
 * {@link KeysetPosition} to a URL-safe Base64 string and back. The token carries only a position —
 * never server state — so a future server-side cached-cursor implementation can accept the same
 * token as a cache-miss fallback with no proto or client change.
 *
 * <p>Wire format (before Base64): a version byte, a kind marker, then the fields. The bucket pvName
 * is length-prefixed rather than delimited so arbitrary pvName characters (including delimiters)
 * round-trip exactly. The exact byte layout is an implementation detail — callers must treat the
 * emitted string as opaque.
 */
public class PageToken {

    // Wire format version — bump if the layout changes so stale tokens are rejected, not misread.
    private static final char VERSION = '1';
    private static final char KIND_BUCKET = 'B';
    private static final char KIND_SAMPLE = 'S';
    private static final char SEP = '|';

    private PageToken() {
    }

    /** Encodes a keyset position to an opaque URL-safe Base64 token. */
    public static String encode(KeysetPosition position) {
        final StringBuilder sb = new StringBuilder();
        sb.append(VERSION).append(SEP);
        switch (position.getKind()) {
            case BUCKET -> {
                final String pvName = position.getPvName() == null ? "" : position.getPvName();
                sb.append(KIND_BUCKET).append(SEP);
                // length-prefix the pvName so it may contain any character, including SEP
                sb.append(pvName.length()).append(SEP).append(pvName).append(SEP);
                sb.append(position.getSeconds()).append(SEP).append(position.getNanos());
            }
            case SAMPLE -> {
                sb.append(KIND_SAMPLE).append(SEP);
                sb.append(position.getSeconds()).append(SEP).append(position.getNanos());
            }
        }
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes an opaque token back to a keyset position. A blank token is not a valid position and
     * callers should check {@code isEmpty()} on the raw token before calling this.
     *
     * @throws PageTokenException if the token is null/blank or cannot be parsed (malformed → reject).
     */
    public static KeysetPosition decode(String token) throws PageTokenException {
        if (token == null || token.isBlank()) {
            throw new PageTokenException("page token is empty");
        }

        final String decoded;
        try {
            decoded = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            throw new PageTokenException("page token is not valid Base64", ex);
        }

        try {
            int pos = 0;

            // version
            final int v1 = decoded.indexOf(SEP, pos);
            if (v1 < 0) {
                throw new PageTokenException("malformed page token (no version separator)");
            }
            final String version = decoded.substring(pos, v1);
            if (!version.equals(String.valueOf(VERSION))) {
                throw new PageTokenException("unsupported page token version: " + version);
            }
            pos = v1 + 1;

            // kind
            final int k1 = decoded.indexOf(SEP, pos);
            if (k1 < 0) {
                throw new PageTokenException("malformed page token (no kind separator)");
            }
            final String kind = decoded.substring(pos, k1);
            pos = k1 + 1;

            if (kind.equals(String.valueOf(KIND_BUCKET))) {
                // length-prefixed pvName
                final int len1 = decoded.indexOf(SEP, pos);
                if (len1 < 0) {
                    throw new PageTokenException("malformed bucket token (no pvName length)");
                }
                final int nameLen = Integer.parseInt(decoded.substring(pos, len1));
                pos = len1 + 1;
                if (nameLen < 0 || pos + nameLen > decoded.length()) {
                    throw new PageTokenException("malformed bucket token (bad pvName length)");
                }
                final String pvName = decoded.substring(pos, pos + nameLen);
                pos += nameLen;
                if (pos >= decoded.length() || decoded.charAt(pos) != SEP) {
                    throw new PageTokenException("malformed bucket token (pvName not terminated)");
                }
                pos += 1; // skip SEP after pvName

                final int s1 = decoded.indexOf(SEP, pos);
                if (s1 < 0) {
                    throw new PageTokenException("malformed bucket token (no seconds separator)");
                }
                final long seconds = Long.parseLong(decoded.substring(pos, s1));
                final long nanos = Long.parseLong(decoded.substring(s1 + 1));
                return KeysetPosition.ofBucket(pvName, seconds, nanos);

            } else if (kind.equals(String.valueOf(KIND_SAMPLE))) {
                final int s1 = decoded.indexOf(SEP, pos);
                if (s1 < 0) {
                    throw new PageTokenException("malformed sample token (no seconds separator)");
                }
                final long seconds = Long.parseLong(decoded.substring(pos, s1));
                final long nanos = Long.parseLong(decoded.substring(s1 + 1));
                return KeysetPosition.ofSample(seconds, nanos);

            } else {
                throw new PageTokenException("unknown page token kind: " + kind);
            }

        } catch (NumberFormatException ex) {
            throw new PageTokenException("malformed page token (non-numeric field)", ex);
        } catch (IndexOutOfBoundsException ex) {
            throw new PageTokenException("malformed page token (truncated)", ex);
        }
    }
}
