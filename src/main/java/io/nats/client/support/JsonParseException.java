package io.nats.client.support;

import java.io.IOException;

/**
 * An exception when parsing JSON
 */
public class JsonParseException extends IOException {
    /**
     * construct a JsonParseException with message text
     * @param message the text
     */
    public JsonParseException(String message) {
        super(message);
    }

    /**
     * construct a JsonParseException with message text and a cause
     * @param message the text
     * @param cause the cause
     */
    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * construct a JsonParseException with a cause
     * @param cause the cause
     */
    public JsonParseException(Throwable cause) {
        super(cause);
    }
}
