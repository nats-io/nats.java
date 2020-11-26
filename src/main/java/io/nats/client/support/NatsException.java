package io.nats.client.support;

public class NatsException extends Exception {

    private int code;

    public NatsException(String message) {
        super(message);
        code = 0;
    }

    public NatsException(int code, String message) {
        super(message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
