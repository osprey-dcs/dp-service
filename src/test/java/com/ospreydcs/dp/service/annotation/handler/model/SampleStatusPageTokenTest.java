package com.ospreydcs.dp.service.annotation.handler.model;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.*;

public class SampleStatusPageTokenTest {

    @Test
    public void testEncodeDecodeRoundTrip() {
        final SampleStatusPageToken token =
                new SampleStatusPageToken("pv_01", "data_quality", "layer_a", 1_700_000_000_123_456_789L);
        assertEquals(token, SampleStatusPageToken.decode(token.encode()));
    }

    @Test
    public void testRoundTripWithAwkwardStrings() {
        // names containing JSON/delimiter characters must survive the codec
        final SampleStatusPageToken token = new SampleStatusPageToken(
                "pv\"with|quotes", "domain{brace}", "layer:colon,comma", 42L);
        assertEquals(token, SampleStatusPageToken.decode(token.encode()));
    }

    @Test
    public void testSmallFirstTimeNanosRoundTrip() {
        // small longs may serialize as plain JSON ints; the decoder must still read them as long
        final SampleStatusPageToken token = new SampleStatusPageToken("pv", "d", "l", 7L);
        assertEquals(token, SampleStatusPageToken.decode(token.encode()));
    }

    @Test
    public void testDecodeGarbageReturnsNull() {
        assertNull(SampleStatusPageToken.decode("not-base64!@#"));
        assertNull(SampleStatusPageToken.decode(""));
        assertNull(SampleStatusPageToken.decode(
                Base64.getEncoder().encodeToString("not json".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testDecodeMissingFieldReturnsNull() {
        final String tokenMissingField = Base64.getEncoder().encodeToString(
                "{\"pvName\": \"pv\", \"domain\": \"d\"}".getBytes(StandardCharsets.UTF_8));
        assertNull(SampleStatusPageToken.decode(tokenMissingField));
    }

    @Test
    public void testDecodeForeignSkipOffsetTokenReturnsNull() {
        // a pvMetadata-style Base64 skip-offset token must be rejected, not misread
        final String skipOffsetToken =
                Base64.getEncoder().encodeToString("100".getBytes(StandardCharsets.UTF_8));
        assertNull(SampleStatusPageToken.decode(skipOffsetToken));
    }
}
