package com.ospreydcs.dp.service.common.exception;

import java.io.IOException;

public class DpException extends Exception {

    public DpException(IOException e) {
        super(e);
    }

    public DpException(String message) {
        super(message);
    }

    /**
     * Preferred when wrapping a caught exception, so the original stack trace survives. Mirrors
     * {@link DpRuntimeException}'s constructor of the same shape, and follows the same reasoning as
     * the exception-logging convention: a message built from {@code ex.getMessage()} alone discards
     * where the failure actually came from.
     */
    public DpException(String message, Throwable cause) {
        super(message, cause);
    }

}
