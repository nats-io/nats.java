package io.nats.client.impl;

public enum AckType {
    // Acknowledgement protocol messages
    AckAck("+ACK"),
    AckNak("-NAK"),
    AckProgress("+WPI"),
    AckTerm("+TERM"),

    // pull only option
    AckNext("+NXT");

    public final String text;
    public final byte[] bytes;

    AckType(String text) {
        this.text = text;
        this.bytes = text.getBytes();
    }
}
