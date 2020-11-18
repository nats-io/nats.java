package io.nats.client.impl;

import static java.nio.charset.StandardCharsets.US_ASCII;

public interface NatsConstants {
    byte[] EMPTY_BODY = new byte[0];

    byte CR = 0x0D;
    byte LF = 0x0A;

    String OP_CONNECT = "CONNECT";
    String OP_INFO = "INFO";
    String OP_SUB = "SUB";
    String OP_PUB = "PUB";
    String OP_HPUB = "HPUB";
    String OP_UNSUB = "UNSUB";
    String OP_MSG = "MSG";
    String OP_HMSG = "HMSG";
    String OP_PING = "PING";
    String OP_PONG = "PONG";
    String OP_OK = "+OK";
    String OP_ERR = "-ERR";

    byte[] OP_PING_BYTES = OP_PING.getBytes();
    byte[] OP_PONG_BYTES = OP_PONG.getBytes();
    byte[] PUB_BYTES = "PUB ".getBytes(US_ASCII);
    byte[] HPUB_BYTES = "HPUB ".getBytes(US_ASCII);

    int PUB_BYTES_LEN = PUB_BYTES.length;
    int HPUB_BYTES_LEN = HPUB_BYTES.length;

    byte[] OP_CONNECT_PROTOCOL_BYTES = (OP_CONNECT + " ").getBytes();
    byte[] OP_SUB_PROTOCOL_BYTES = (OP_SUB + " ").getBytes();
    byte[] OP_UNSUB_PROTOCOL_BYTES = (OP_UNSUB + " ").getBytes();
    int OP_CONNECT_PROTOCOL_LEN = OP_CONNECT_PROTOCOL_BYTES.length;
    int OP_SUB_PROTOCOL_LEN = OP_SUB_PROTOCOL_BYTES.length;
    int OP_UNSUB_PROTOCOL_LEN = OP_UNSUB_PROTOCOL_BYTES.length;

    int MAX_PROTOCOL_OP_LENGTH = 4;
    String UNKNOWN_OP = "UNKNOWN";
    char SPACE = ' ';
    char TAB = '\t';
}
