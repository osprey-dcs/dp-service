package com.ospreydcs.dp.service.query.handler.paging;

/**
 * Thrown when an inbound page token cannot be decoded. The resolver translates this into an
 * {@code ExceptionalResult} reject rather than silently treating a malformed token as "first page"
 * (Q1/Q2/Q3: malformed inbound token → reject).
 */
public class PageTokenException extends Exception {
    public PageTokenException(String message) {
        super(message);
    }

    public PageTokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
