package org.codelibs.elasticsearch.configsync.exception;

public class InvalidRequestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidRequestException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidRequestException(final String message) {
        super(message);
    }

}
