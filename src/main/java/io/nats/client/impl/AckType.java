package io.nats.client.impl;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

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
        this.bytes = text.getBytes(ISO_8859_1);
        this.terminal = terminal;
    }

    public byte[] bodyBytes(long delayNanoseconds) {
        return delayNanoseconds < 1 ? bytes : (text + " {\"delay\": " + delayNanoseconds + "}").getBytes(ISO_8859_1);
    }
}
