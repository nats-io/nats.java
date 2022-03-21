package io.nats.client.impl;

import java.nio.charset.StandardCharsets;

public enum AckType {
    // Acknowledgement protocol messages
    AckAck("+ACK", true),
    AckNak("-NAK", true),
    AckProgress("+WPI", false),
    AckTerm("+TERM", true),

    // pull only option
    AckNext("+NXT", false);

    public final String text;
    public final byte[] bytes;
    public final boolean terminal;

    AckType(String text, boolean terminal) {
        this.text = text;
        this.bytes = text.getBytes(StandardCharsets.US_ASCII);
        this.terminal = terminal;
    }

    public byte[] bodyBytes(Long delayNanoseconds) {
        return delayNanoseconds == null || delayNanoseconds < 1
            ? bytes
            : (text + " {\"delay\": " + delayNanoseconds + "}").getBytes(StandardCharsets.US_ASCII);
    }
}
