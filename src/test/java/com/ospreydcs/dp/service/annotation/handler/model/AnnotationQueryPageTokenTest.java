package com.ospreydcs.dp.service.annotation.handler.model;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.*;

public class AnnotationQueryPageTokenTest {

    private static final String VALID_HEX = "65f0123456789abcdef01234";

    @Test
    public void testEncodeDecodeRoundTrip() {
        final AnnotationQueryPageToken token =
                new AnnotationQueryPageToken(AnnotationQueryPageToken.QUERY_DATA_SETS, VALID_HEX);
        assertEquals(token, AnnotationQueryPageToken.decode(
                token.encode(), AnnotationQueryPageToken.QUERY_DATA_SETS));
    }

    @Test
    public void testDecodeGarbageReturnsNull() {
        assertNull(AnnotationQueryPageToken.decode(
                "not-base64!@#", AnnotationQueryPageToken.QUERY_DATA_SETS));
        assertNull(AnnotationQueryPageToken.decode(
                "", AnnotationQueryPageToken.QUERY_DATA_SETS));
        assertNull(AnnotationQueryPageToken.decode(
                Base64.getEncoder().encodeToString("not json".getBytes(StandardCharsets.UTF_8)),
                AnnotationQueryPageToken.QUERY_DATA_SETS));
    }

    @Test
    public void testDecodeMissingFieldReturnsNull() {
        final String tokenMissingLastId = Base64.getEncoder().encodeToString(
                "{\"query\": \"dataSets\"}".getBytes(StandardCharsets.UTF_8));
        assertNull(AnnotationQueryPageToken.decode(
                tokenMissingLastId, AnnotationQueryPageToken.QUERY_DATA_SETS));
    }

    @Test
    public void testDecodeNonObjectIdLastIdReturnsNull() {
        final AnnotationQueryPageToken token =
                new AnnotationQueryPageToken(AnnotationQueryPageToken.QUERY_DATA_SETS, "not-an-objectid");
        assertNull(AnnotationQueryPageToken.decode(
                token.encode(), AnnotationQueryPageToken.QUERY_DATA_SETS));
    }

    @Test
    public void testDecodeWrongQueryTokenReturnsNull() {
        // a queryDataSets token used against queryAnnotations would decode cleanly and silently
        // skip an arbitrary prefix of results; the discriminator makes it a rejection instead
        final AnnotationQueryPageToken token =
                new AnnotationQueryPageToken(AnnotationQueryPageToken.QUERY_DATA_SETS, VALID_HEX);
        assertNull(AnnotationQueryPageToken.decode(
                token.encode(), AnnotationQueryPageToken.QUERY_ANNOTATIONS));
    }

    @Test
    public void testDecodeForeignSkipOffsetTokenReturnsNull() {
        // a pvMetadata-style Base64 skip-offset token must be rejected, not misread
        final String skipOffsetToken =
                Base64.getEncoder().encodeToString("100".getBytes(StandardCharsets.UTF_8));
        assertNull(AnnotationQueryPageToken.decode(
                skipOffsetToken, AnnotationQueryPageToken.QUERY_DATA_SETS));
    }
}
