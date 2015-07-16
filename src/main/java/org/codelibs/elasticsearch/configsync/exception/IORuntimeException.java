package org.codelibs.elasticsearch.configsync.exception;

import java.io.IOException;

public class IORuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IORuntimeException(final String msg, final IOException e) {
        super(msg, e);
    }

}
