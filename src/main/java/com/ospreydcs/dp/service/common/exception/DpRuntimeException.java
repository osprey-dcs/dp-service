package com.ospreydcs.dp.service.common.exception;

/**
 * Unchecked counterpart to {@link DpException}, for failures that callers are not expected to
 * recover from and so should not have to declare — most notably invalid deployment configuration
 * detected while reading it, where the only sound response is to fail loudly rather than continue
 * with a value that would produce silently wrong results.
 */
public class DpRuntimeException extends RuntimeException {

    public DpRuntimeException(String message) {
        super(message);
    }

    public DpRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

}
