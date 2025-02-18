package com.ospreydcs.dp.service.common.exception;

import java.io.IOException;

public class DpException extends Exception {

    public DpException(IOException e) {
        super(e);
    }

    public DpException(String message) {
        super(message);
    }

}
