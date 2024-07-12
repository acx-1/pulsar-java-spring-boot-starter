package io.github.majusko.pulsar.error.exception;


public class TopicNotCreateException extends RuntimeException {
    public TopicNotCreateException() {
        super();
    }

    public TopicNotCreateException(String message) {
        super(message);
    }

    public TopicNotCreateException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopicNotCreateException(Throwable cause) {
        super(cause);
    }

    protected TopicNotCreateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}