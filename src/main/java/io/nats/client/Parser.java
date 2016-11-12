/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.NatsOp.OP_START;
import static io.nats.client.Parser.MsgArg.NatsOp.OP_START;
import static io.nats.client.Parser.NatsOp.INFO_ARG;
import static io.nats.client.Parser.NatsOp.MINUS_ERR_ARG;
import static io.nats.client.Parser.NatsOp.MSG_ARG;
import static io.nats.client.Parser.NatsOp.MSG_END;
import static io.nats.client.Parser.NatsOp.MSG_PAYLOAD;
import static io.nats.client.Parser.NatsOp.OP_I;
import static io.nats.client.Parser.NatsOp.OP_IN;
import static io.nats.client.Parser.NatsOp.OP_INF;
import static io.nats.client.Parser.NatsOp.OP_INFO;
import static io.nats.client.Parser.NatsOp.OP_INFO_SPC;
import static io.nats.client.Parser.NatsOp.OP_M;
import static io.nats.client.Parser.NatsOp.OP_MINUS;
import static io.nats.client.Parser.NatsOp.OP_MINUS_E;
import static io.nats.client.Parser.NatsOp.OP_MINUS_ER;
import static io.nats.client.Parser.NatsOp.OP_MINUS_ERR;
import static io.nats.client.Parser.NatsOp.OP_MINUS_ERR_SPC;
import static io.nats.client.Parser.NatsOp.OP_MS;
import static io.nats.client.Parser.NatsOp.OP_MSG;
import static io.nats.client.Parser.NatsOp.OP_MSG_SPC;
import static io.nats.client.Parser.NatsOp.OP_P;
import static io.nats.client.Parser.NatsOp.OP_PI;
import static io.nats.client.Parser.NatsOp.OP_PIN;
import static io.nats.client.Parser.NatsOp.OP_PING;
import static io.nats.client.Parser.NatsOp.OP_PLUS;
import static io.nats.client.Parser.NatsOp.OP_PLUS_O;
import static io.nats.client.Parser.NatsOp.OP_PLUS_OK;
import static io.nats.client.Parser.NatsOp.OP_PO;
import static io.nats.client.Parser.NatsOp.OP_PON;
import static io.nats.client.Parser.NatsOp.OP_PONG;
import static io.nats.client.Parser.NatsOp.OP_START;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.ParseException;

class Parser {
    static final Logger logger = LoggerFactory.getLogger(Parser.class);

    static final int MAX_CONTROL_LINE_SIZE = 1024;
    static final int MAX_MSG_ARGS = 4;

    final ConnectionImpl nc;

    // List<byte[]> args = new ArrayList<byte[]>();

    class MsgArg {
        // byte[] subjectBytes = new byte[MAX_CONTROL_LINE_SIZE];
        ByteBuffer subject = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
        // int subjectLength = 0;
        // byte[] replyBytes = new byte [MAX_CONTROL_LINE_SIZE];
        ByteBuffer reply = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
        // int replyLength = 0;
        long sid;
        int size;

        public String toString() {
            String subjectString = "null";
            String replyString = "null";
            byte[] subjectArray;
            byte[] replyArray;

            if (subject != null) {
                subjectArray = subject.array();
                subjectString = new String(subjectArray, 0, subject.limit());
            }

            if (reply != null) {
                replyArray = reply.array();
                replyString = new String(replyArray, 0, reply.limit());
            }
            return String.format("{subject=%s(len=%d), reply=%s(len=%d), sid=%d, size=%d}",
                    subjectString, subjectString.length(), replyString, replyString.length(), sid,
                    size);
        }
    }

    class ParseState {
        NatsOp state = OP_START;
        int as;
        int drop;
        final MsgArg ma = new MsgArg();
        final byte[] argBufStore = new byte[ConnectionImpl.DEFAULT_BUF_SIZE];
        ByteBuffer argBuf = null;
        byte[] msgBufStore = new byte[ConnectionImpl.DEFAULT_BUF_SIZE];
        ByteBuffer msgBuf = null;
        // byte[] scratch = new byte[MAX_CONTROL_LINE_SIZE];
        final ByteBuffer[] args = new ByteBuffer[MAX_MSG_ARGS];

        ParseState() {
            for (int i = 0; i < MAX_MSG_ARGS; i++) {
                args[i] = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
            }
        }
    }

    ParseState ps = new ParseState();

    static final int ascii_0 = 48;
    static final int ascii_9 = 57;

    enum NatsOp {
        OP_START, /* Start of message */
        OP_PLUS, OP_PLUS_O, OP_PLUS_OK, /* +OK */
        OP_MINUS, OP_MINUS_E, OP_MINUS_ER, OP_MINUS_ERR, /* -ERR */
        OP_MINUS_ERR_SPC, MINUS_ERR_ARG, /* -ERR '<argument>' */
        OP_M, OP_MS, OP_MSG, OP_MSG_SPC, MSG_ARG, /* MSG <args> */
        MSG_PAYLOAD, /* message payload bytes */
        MSG_END, /* end of message */
        OP_P, /* P[ING]/P[ONG] */
        OP_PI, OP_PIN, OP_PING, /* PING */
        OP_PO, OP_PON, OP_PONG, /* PONG */
        OP_I, OP_IN, OP_INF, OP_INFO, OP_INFO_SPC, INFO_ARG /* INFO {...} */
    }

    Parser(ConnectionImpl conn) {
        this.nc = conn;
    }

    void parse(byte[] buf) throws ParseException {
        parse(buf, buf.length);
    }

    void parse(byte[] buf, int len) throws ParseException {
        int i;
        byte b;
        boolean error = false;

        for (i = 0; i < len; i++) {
            b = buf[i];

            // printStatus(buf, i);

            switch (ps.state) {
                case OP_START:
                    switch (b) {
                        case 'M':
                        case 'm':
                            ps.state = OP_M;
                            break;
                        case 'P':
                        case 'p':
                            ps.state = OP_P;
                            break;
                        case '+':
                            ps.state = OP_PLUS;
                            break;
                        case '-':
                            ps.state = OP_MINUS;
                            break;
                        case 'I':
                        case 'i':
                            ps.state = OP_I;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_M:
                    switch (b) {
                        case 'S':
                        case 's':
                            ps.state = OP_MS;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MS:
                    switch (b) {
                        case 'G':
                        case 'g':
                            ps.state = OP_MSG;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MSG:
                    switch (b) {
                        case ' ':
                        case '\t':
                            ps.state = OP_MSG_SPC;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MSG_SPC:
                    switch (b) {
                        case ' ':
                        case '\t':
                            continue;
                        default:
                            ps.state = MSG_ARG;
                            ps.as = i;
                            break;
                    }
                    break;
                case MSG_ARG:
                    switch (b) {
                        case '\r':
                            ps.drop = 1;
                            break;
                        case '\n':
                            if (ps.argBuf != null) {
                                // End of args
                                ps.argBuf.flip();
                                byte[] arg = ps.argBuf.array();
                                int from = ps.argBuf.arrayOffset() + ps.argBuf.position();
                                int to = ps.argBuf.arrayOffset() + ps.argBuf.limit();
                                ps.argBuf = null;
                                int length = to - from;
                                processMsgArgs(arg, from, length);
                            } else {
                                processMsgArgs(buf, ps.as, i - ps.drop - ps.as);
                            }

                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = MSG_PAYLOAD;

                            // jump ahead with the index. If this overruns
                            // what is left we fall out and process split
                            // buffer.
                            i = ps.as + ps.ma.size - 1;
                            break;
                        default:
                            // We have a leftover argBuf we'll continuing filling
                            if (ps.argBuf != null) {
                                ps.argBuf.put(b);
                            }
                            break;
                    }
                    break;
                case MSG_PAYLOAD:
                    boolean done = false;
                    if (ps.msgBuf != null) {
                        // Already have bytes in the buffer
                        if (ps.msgBuf.position() >= ps.ma.size) {
                            ps.msgBuf.flip();
                            submitMsg(ps.msgBuf.array(), 0, ps.msgBuf.limit());
                            done = true;
                        } else {
                            // copy as much as we can to the buffer and skip ahead.
                            int toCopy = ps.ma.size - ps.msgBuf.limit();
                            int avail = len - i;

                            if (avail < toCopy) {
                                toCopy = avail;
                            }

                            if (toCopy > 0) {
                                ps.msgBuf.put(buf, i, toCopy);
                                // Update our index
                                i += toCopy - 1;
                            } else {
                                ps.msgBuf.put(b);
                            }
                        }
                    } else if (i - ps.as >= ps.ma.size) {
                        // If we are at or past the end of the payload, go ahead and process it, no
                        // buffering needed.
                        submitMsg(buf, ps.as, i - ps.as); // pass offset and length
                        done = true;
                    }

                    if (done) {
                        ps.argBuf = null;
                        // ps.argBuf.clear();
                        ps.msgBuf = null;
                        // ps.msgBuf.clear();
                        ps.state = MSG_END;
                    }

                    break;
                case MSG_END:
                    switch (b) {
                        case '\n':
                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = OP_START;
                            break;
                        default:
                            continue;
                    }
                    break;
                case OP_PLUS:
                    switch (b) {
                        case 'O':
                        case 'o':
                            ps.state = OP_PLUS_O;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PLUS_O:
                    switch (b) {
                        case 'K':
                        case 'k':
                            ps.state = OP_PLUS_OK;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PLUS_OK:
                    switch (b) {
                        case '\n':
                            nc.processOk();
                            ps.drop = 0;
                            ps.state = OP_START;
                            break;
                        default:
                            break;
                    }
                    break;
                case OP_MINUS:
                    switch (b) {
                        case 'E':
                        case 'e':
                            ps.state = OP_MINUS_E;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MINUS_E:
                    switch (b) {
                        case 'R':
                        case 'r':
                            ps.state = OP_MINUS_ER;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MINUS_ER:
                    switch (b) {
                        case 'R':
                        case 'r':
                            ps.state = OP_MINUS_ERR;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MINUS_ERR:
                    switch (b) {
                        case ' ':
                        case '\t':
                            ps.state = OP_MINUS_ERR_SPC;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_MINUS_ERR_SPC:
                    switch (b) {
                        case ' ':
                        case '\t':
                            continue;
                        default:
                            ps.state = MINUS_ERR_ARG;
                            ps.as = i;
                            break;
                    }
                    break;
                case MINUS_ERR_ARG:
                    switch (b) {
                        case '\r':
                            ps.drop = 1;
                            break;
                        case '\n':
                            ByteBuffer arg;
                            if (ps.argBuf != null) {
                                arg = ps.argBuf;
                                ps.argBuf = null;
                            } else {
                                arg = ByteBuffer.wrap(buf, ps.as, i - ps.drop - ps.as);
                            }
                            nc.processErr(arg);
                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = OP_START;
                            break;
                        default:
                            if (ps.argBuf != null) {
                                ps.argBuf.put(b);
                            }
                            break;
                    }
                    break;
                case OP_P:
                    switch (b) {
                        case 'I':
                        case 'i':
                            ps.state = OP_PI;
                            break;
                        case 'O':
                        case 'o':
                            ps.state = OP_PO;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PO:
                    switch (b) {
                        case 'N':
                        case 'n':
                            ps.state = OP_PON;
                            break;
                        default:
                            // parseError(buf, i);
                            error = true;
                            break;
                    }
                    break;
                case OP_PON:
                    switch (b) {
                        case 'G':
                        case 'g':
                            ps.state = OP_PONG;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PONG:
                    switch (b) {
                        case '\n':
                            nc.processPong();
                            ps.drop = 0;
                            ps.state = OP_START;
                            break;
                        default:
                            break;
                    }
                    break;
                case OP_PI:
                    switch (b) {
                        case 'N':
                        case 'n':
                            ps.state = OP_PIN;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PIN:
                    switch (b) {
                        case 'G':
                        case 'g':
                            ps.state = OP_PING;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PING:
                    switch (b) {
                        case '\n':
                            nc.processPing();
                            ps.drop = 0;
                            ps.state = OP_START;
                            break;
                        default:
                            break;
                    }
                    break;
                case OP_I:
                    switch (b) {
                        case 'N':
                        case 'n':
                            ps.state = OP_IN;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_IN:
                    switch (b) {
                        case 'F':
                        case 'f':
                            ps.state = OP_INF;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_INF:
                    switch (b) {
                        case 'O':
                        case 'o':
                            ps.state = OP_INFO;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_INFO:
                    switch (b) {
                        case ' ':
                        case '\t':
                            ps.state = OP_INFO_SPC;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_INFO_SPC:
                    switch (b) {
                        case ' ':
                        case '\t':
                            continue;
                        default:
                            ps.state = INFO_ARG;
                            ps.as = i;
                            break;
                    }
                    break;
                case INFO_ARG:
                    switch (b) {
                        case '\r':
                            ps.drop = 1;
                            break;
                        case '\n':
                            if (ps.argBuf != null) {
                                // End of args
                                ps.argBuf.flip();
                                byte[] arg = ps.argBuf.array();
                                int from = ps.argBuf.arrayOffset() + ps.argBuf.position();
                                int to = ps.argBuf.arrayOffset() + ps.argBuf.limit();
                                int length = to - from + 1;
                                ps.argBuf = null;
                                nc.processAsyncInfo(arg, from, length);
                            } else {
                                nc.processAsyncInfo(buf, ps.as, i - ps.drop - ps.as);
                            }

                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = OP_START;

                            break;
                        default:
                            // We have a leftover argBuf we'll continue filling
                            if (ps.argBuf != null) {
                                ps.argBuf.put(b);
                            }
                            break;
                    }
                    break;
                default:
                    error = true;
                    break;
            } // switch(ps.state)

            if (error) {
                throw new ParseException(String.format("nats: parse error [%s]: len=%d, '%s'",
                        ps.state, len - i, new String(buf, i, len - i)), i);
            }
        } // for

        // Check for split buffer scenarios
        if ((ps.state == MSG_ARG || ps.state == MINUS_ERR_ARG) || ps.state == INFO_ARG && (ps
                .argBuf == null)) {
            try {
                ps.argBuf = ByteBuffer.wrap(ps.argBufStore);
                assert (ps.as >= 0);
                assert ((i - ps.drop - ps.as) > 0);
                if (i - ps.drop - ps.as > 0) {
                    ps.argBuf.put(buf, ps.as, i - ps.drop - ps.as);
                }
                // FIXME, check max len
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
                logger.error("state = {}, i = {}, buf(len:{}) = [{}], ps.argBuf = {}, ps.as = {},"
                                + " i - ps.as = {}",
                        ps.state, i, len, new String(buf, 0, len), ps.argBuf, ps.as, i - ps.as);
                nc.processErr(ps.argBuf);
            }
        }
        // Check for split msg
        if (ps.state == MSG_PAYLOAD && ps.msgBuf == null) {
            // We need to clone the msgArg if it is still referencing the
            // read buffer and we are not able to process the msg.
            if (ps.argBuf == null) {
                cloneMsgArg();
            }

            // If we will overflow the msg buffer, create a
            // new buffer to hold the split message.
            if (ps.ma.size > ps.msgBufStore.length - ps.argBuf.limit()) {
                int lrem = len - ps.as; // portion of msg remaining in buffer
                ps.msgBufStore = new byte[ps.ma.size];
                ps.msgBuf = ByteBuffer.wrap(ps.msgBufStore);
                // copy what's left in the buffer
                try {
                    // FIXME check max len
                    if (ps.msgBuf.remaining() < lrem) {
                        String err = String.format("msgBuf remaining cap too small(%d) after "
                                + "realloc, needed: %d", ps.msgBuf.remaining(), lrem);
                        throw new ParseException(err, i);
                    } else if (buf.length < ps.as || lrem - ps.as > buf.length) {
                        String err = String.format("Programmer error; buf length = %d, trying to "
                                + "copy offset %d, length %d", buf.length, ps.as, lrem);
                        throw new ParseException(err, i);
                    }
                    // Can throw BufferOverflowException if lrem > ps.msgBuf.remaining
                    ps.msgBuf.put(buf, ps.as, lrem);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("state = {}, i = {}, buf(len:{}) = [{}], ps.msgBuf = {}, ps.as ="
                                    + " {}, lrem = {}",
                            ps.state, i, len, new String(buf, 0, len), ps.msgBuf, ps.as, lrem);
                    nc.processErr(ps.msgBuf);
                }
            } else {
                // copy args
                ps.msgBuf.put(ps.argBuf);
                // copy body
                ps.msgBuf.put(buf, ps.as, len - ps.as);
            }
        }
    }

    static String bufToString(ByteBuffer arg) {
        if (arg == null) {
            return null;
        }
        int pos = arg.position();
        int len = arg.limit() - arg.position();

        byte[] stringBytes = new byte[len];
        arg.get(stringBytes, 0, len);
        arg.position(pos);
        return new String(stringBytes);
    }

    void processMsgArgs(byte[] arg, int offset, int length) throws ParseException {
        int argLen;
        int numArgs = 0;
        int start = -1;
        byte b;
        int i;
        for (i = offset; i < offset + length; i++) {
            b = arg[i];
            switch (b) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    if (start >= 0) {
                        argLen = i - start;
                        if (argLen > ps.args[numArgs].remaining()) {
                            ps.args[numArgs] = ByteBuffer.allocate(argLen);
                        }
                        ps.args[numArgs++].put(arg, start, argLen).flip();
                        start = -1;
                    }
                    break;
                default:
                    if (start < 0) {
                        start = i;
                    }
                    break;
            }
        }
        if (start >= 0) {
            argLen = i - start;
            ps.args[numArgs++].put(arg, start, argLen).flip();
        }

        ps.ma.subject.clear();
        ps.ma.reply.clear();

        if (ps.ma.subject.remaining() < ps.args[0].limit()) {
            ps.ma.subject = ByteBuffer.allocate(ps.args[0].limit());
        }
        switch (numArgs) {
            case 3:
                ps.ma.subject.put(ps.args[0].array(), 0, ps.args[0].limit());
                ps.ma.sid = parseLong(ps.args[1].array(), ps.args[1].limit());
                ps.ma.reply.clear();
                ps.ma.size = (int) parseLong(ps.args[2].array(), ps.args[2].limit());
                break;
            case 4:
                ps.ma.subject.put(ps.args[0].array(), 0, ps.args[0].limit());
                ps.ma.sid = parseLong(ps.args[1].array(), ps.args[1].limit());
                if (ps.ma.reply.remaining() < ps.args[2].limit()) {
                    ps.ma.reply = ByteBuffer.allocate(ps.args[2].limit());
                }
                ps.ma.reply.put(ps.args[2].array(), 0, ps.args[2].limit());
                ps.ma.size = (int) parseLong(ps.args[3].array(), ps.args[3].limit());
                break;
            default:
                String msg = String.format("nats: processMsgArgs bad number of args(%d): '%s'",
                        numArgs, new String(arg, offset, length));
                throw new ParseException(msg, 0);
        }

        for (int n = 0; n < MAX_MSG_ARGS; n++) {
            ps.args[n].clear();
        }

        if (ps.ma.sid < 0) {
            String str = new String(arg, offset, length);
            throw new ParseException(
                    String.format("nats: processMsgArgs bad or missing sid: '%s'", str),
                    (int) ps.ma.sid);
        }
        if (ps.ma.size < 0) {
            String str = new String(arg, offset, length);
            throw new ParseException(
                    String.format("nats: processMsgArgs bad or missing size: '%s'", str),
                    ps.ma.size);
        }
        ps.ma.subject.flip();
        ps.ma.reply.flip();
    }

    // cloneMsgArg is used when the split buffer scenario has the pubArg in the existing read
    // buffer, but
    // we need to hold onto it into the next read.
    private void cloneMsgArg() {
        ps.argBuf = ByteBuffer.wrap(ps.argBufStore);
        ps.argBuf.put(ps.ma.subject.array(), 0, ps.ma.subject.limit());
        if (ps.ma.reply.limit() != 0) {
            ps.argBuf.put(ps.ma.reply.array(), 0, ps.ma.reply.limit());
        }
        ps.argBuf.rewind();
        ps.argBuf.get(ps.ma.subject.array(), 0, ps.ma.subject.limit());
        if (ps.ma.reply.limit() != 0) {
            ps.argBuf.get(ps.ma.reply.array(), 0, ps.ma.reply.limit());
        }
    }

    // parseInt64 expects decimal positive numbers. We
    // return -1 to signal error
    static long parseLong(byte[] data, int length) {
        long num = 0;
        if (length == 0) {
            return -1;
        }
        byte dec;
        for (int i = 0; i < length; i++) {
            dec = data[i];
            if (dec < ascii_0 || dec > ascii_9) {
                return -1;
            }
            num = (num * 10) + dec - ascii_0;
        }
        return num;
    }

    private void submitMsg(final byte[] data, final int offset, final int length) {
        nc.processMsg(data, offset, length);
    }
}
