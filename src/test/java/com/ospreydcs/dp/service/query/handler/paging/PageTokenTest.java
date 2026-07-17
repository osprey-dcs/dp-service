package com.ospreydcs.dp.service.query.handler.paging;

import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link PageToken} — the opaque, position-only page-token codec (Q1/Q2/Q3).
 * Verifies round-trip for both bucket and sample positions (including a pvName containing the
 * internal delimiter) and that malformed tokens are rejected rather than silently treated as page 1.
 */
public class PageTokenTest {

    // -----------------------------------------------------------------------
    // round-trip
    // -----------------------------------------------------------------------

    @Test
    public void testBucketRoundTrip() throws PageTokenException {
        final KeysetPosition original = KeysetPosition.ofBucket("S01:BPM:X", 1_700_000_000L, 123_456_789L);
        final String token = PageToken.encode(original);
        assertEquals(original, PageToken.decode(token));
    }

    @Test
    public void testSampleRoundTrip() throws PageTokenException {
        final KeysetPosition original = KeysetPosition.ofSample(1_700_000_050L, 999_999_999L);
        final String token = PageToken.encode(original);
        assertEquals(original, PageToken.decode(token));
    }

    @Test
    public void testBucketPvNameWithDelimiterRoundTrips() throws PageTokenException {
        // the internal delimiter is '|'; length-prefixing must make an embedded '|' survive
        final KeysetPosition original = KeysetPosition.ofBucket("weird|pv|name", 5L, 6L);
        final KeysetPosition decoded = PageToken.decode(PageToken.encode(original));
        assertEquals(original, decoded);
        assertEquals("weird|pv|name", decoded.getPvName());
    }

    @Test
    public void testBucketEmptyPvNameRoundTrips() throws PageTokenException {
        final KeysetPosition original = KeysetPosition.ofBucket("", 0L, 0L);
        assertEquals(original, PageToken.decode(PageToken.encode(original)));
    }

    @Test
    public void testTokenIsOpaqueUrlSafeBase64() {
        final String token = PageToken.encode(KeysetPosition.ofSample(1L, 2L));
        // URL-safe alphabet: no '+', '/', or '=' padding
        assertFalse(token.contains("+"));
        assertFalse(token.contains("/"));
        assertFalse(token.contains("="));
    }

    // -----------------------------------------------------------------------
    // malformed → reject
    // -----------------------------------------------------------------------

    @Test(expected = PageTokenException.class)
    public void testDecodeNullRejected() throws PageTokenException {
        PageToken.decode(null);
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeBlankRejected() throws PageTokenException {
        PageToken.decode("   ");
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeNonBase64Rejected() throws PageTokenException {
        PageToken.decode("!!! not base64 !!!");
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeWrongVersionRejected() throws PageTokenException {
        final String payload = "9|S|1|2"; // version 9 unsupported
        final String token = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        PageToken.decode(token);
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeUnknownKindRejected() throws PageTokenException {
        final String payload = "1|Z|1|2"; // kind Z unknown
        final String token = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        PageToken.decode(token);
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeNonNumericFieldRejected() throws PageTokenException {
        final String payload = "1|S|abc|2"; // seconds not numeric
        final String token = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        PageToken.decode(token);
    }

    @Test(expected = PageTokenException.class)
    public void testDecodeTruncatedSampleRejected() throws PageTokenException {
        final String payload = "1|S|1"; // missing nanos
        final String token = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        PageToken.decode(token);
    }
}
