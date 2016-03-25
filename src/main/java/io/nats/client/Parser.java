/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.ParseException;

final class Parser {
    final static Logger logger = LoggerFactory.getLogger(Parser.class);

    final static int MAX_CONTROL_LINE_SIZE = 1024;
    final static int MAX_MSG_ARGS = 4;

    private ConnectionImpl nc;

    // List<byte[]> args = new ArrayList<byte[]>();

    protected class MsgArg {
        // byte[] subjectBytes = new byte[MAX_CONTROL_LINE_SIZE];
        ByteBuffer subject = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
        // int subjectLength = 0;
        // byte[] replyBytes = new byte [MAX_CONTROL_LINE_SIZE];
        ByteBuffer reply = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
        // int replyLength = 0;
        long sid;
        int size;

        public String toString() {

            return String.format("{subject=%s(len=%d), reply=%s(len=%d), sid=%d, size=%d}",
                    new String(subject.array(), 0, subject.limit()), subject.limit(),
                    reply == null ? "null" : new String(reply.array(), 0, reply.limit()),
                    reply.limit(), sid, size);
        }
    }

    protected class ParseState {
        NatsOp state = NatsOp.OP_START;
        int as;
        int drop;
        MsgArg ma = new MsgArg();
        byte[] argBufStore = new byte[ConnectionImpl.DEFAULT_BUF_SIZE];
        ByteBuffer argBuf = null;
        byte[] msgBufStore = new byte[ConnectionImpl.DEFAULT_BUF_SIZE];
        ByteBuffer msgBuf = null;
        // byte[] scratch = new byte[MAX_CONTROL_LINE_SIZE];
        ByteBuffer[] args = new ByteBuffer[MAX_MSG_ARGS];

        ParseState() {
            for (int i = 0; i < MAX_MSG_ARGS; i++) {
                args[i] = ByteBuffer.allocate(MAX_CONTROL_LINE_SIZE);
            }
        }
    }

    ParseState ps = new ParseState();

    final static int ascii_0 = 48;
    final static int ascii_9 = 57;

    static enum NatsOp {
        OP_START, OP_PLUS, OP_PLUS_O, OP_PLUS_OK, OP_MINUS, OP_MINUS_E, OP_MINUS_ER, OP_MINUS_ERR, OP_MINUS_ERR_SPC, MINUS_ERR_ARG, OP_M, OP_MS, OP_MSG, OP_MSG_SPC, MSG_ARG, MSG_PAYLOAD, MSG_END, OP_P, OP_PI, OP_PIN, OP_PING, OP_PO, OP_PON, OP_PONG
    }

    protected Parser(ConnectionImpl conn) {
        this.nc = conn;
        this.nc.ps = conn.ps;
    }

    // protected void printStatus(byte[] buf, int i) {
    // String s = null;
    // char b = (char)buf[i];
    // if (b == '\r')
    // s = "\\r";
    // else if (b == '\n')
    // s = "\\n";
    // else if (b == '\t')
    // s = "\\t";
    // else
    // s = String.format("%c", b);
    //
    // System.err.printf("ps.state = %s, new char buf[%d] = '%s' (0x%02X) argBuf=[%s]\n",
    // ps.state, i , s,
    // (int) b < 32 ? (int) b: b,
    // bufToString(ps.argBuf));
    // System.err.printf("ps.argBuf == %s\n", ps.argBuf);
    // System.err.printf("ps.msgBuf == %s\n", ps.msgBuf);
    // }

    protected void parse(byte[] buf, int len) throws ParseException {
        int i;
        byte b;
        boolean error = false;

        // if (len > buf.length) {
        // throw new ParseException(String.format("Parse length(%d) > actual buffer length(%d)\n",
        // len, buf.length),0);
        // }
        // String tmpStr = new String(buf, 0, len);
        // System.err.printf("##### Parsing buf=[%s], ps.argBuf=[%s]\n", tmpStr.trim(), ps.argBuf);

        for (i = 0; i < len; i++) {
            b = buf[i];

            // printStatus(buf, i);

            switch (ps.state) {
                case OP_START:
                    switch (b) {
                        case 'M':
                        case 'm':
                            ps.state = NatsOp.OP_M;
                            break;
                        case 'P':
                        case 'p':
                            ps.state = NatsOp.OP_P;
                            break;
                        case '+':
                            ps.state = NatsOp.OP_PLUS;
                            break;
                        case '-':
                            ps.state = NatsOp.OP_MINUS;
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
                            ps.state = NatsOp.OP_MS;
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
                            ps.state = NatsOp.OP_MSG;
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
                            ps.state = NatsOp.OP_MSG_SPC;
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
                            ps.state = NatsOp.MSG_ARG;
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
                            ByteBuffer arg = null;
                            if (ps.argBuf != null) {
                                // End of args
                                arg = ps.argBuf;
                                arg.flip();
                                processMsgArgs(arg.array(), arg.arrayOffset(), arg.limit());
                            } else {
                                // arg = ByteBuffer.wrap(buf, ps.as, i-ps.drop-ps.as).slice();
                                processMsgArgs(buf, ps.as, i - ps.drop - ps.as);
                            }
                            // System.err.printf("arrayOffset=%d, length=%d\n", arg.arrayOffset(),
                            // arg.limit());
                            // processMsgArgs(arg.array(), arg.arrayOffset(), arg.limit());

                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = NatsOp.MSG_PAYLOAD;

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
                    // System.err.printf("MSG_PAYLOAD: ps.ma.size = %d\n", ps.ma.size);
                    // System.err.printf("ps.msgBuf.position=%d, i=%d, ps.as=%d, ps.ma.size=%d\n",
                    // ps.msgBuf.position(), i, ps.as, ps.ma.size);
                    if (ps.msgBuf != null) {
                        // System.err.printf("ps.msgBuf.position=%d, i=%d, ps.as=%d,
                        // ps.ma.size=%d\n",
                        // ps.msgBuf.position(), i, ps.as, ps.ma.size);
                        // Already have bytes in the buffer
                        if (ps.msgBuf.position() >= ps.ma.size) {
                            ps.msgBuf.flip();
                            nc.processMsg(ps.msgBuf.array(), 0, ps.msgBuf.limit());
                            done = true;
                        } else {
                            // copy as much as we can to the buffer and skip ahead.
                            int toCopy = ps.ma.size - ps.msgBuf.limit();
                            int avail = len - i;

                            if (avail < toCopy) {
                                toCopy = avail;
                            }

                            // System.err.printf("msgBuf=%s(remaining=%d), i=%d, len=%d,
                            // ps.ma.size=%d, avail = %d, toCopy=%d,"
                            // + " buf.length=%d\n",
                            // ps.msgBuf, ps.msgBuf.remaining(), i, len, ps.ma.size, avail, toCopy,
                            // buf.length);
                            if (toCopy > 0) {
                                // System.err.printf("msgBuf=%s(remaining=%d), i=%d, len=%d,
                                // ps.ma.size=%d, avail = %d, toCopy=%d,"
                                // + " buf.length=%d\n",
                                // ps.msgBuf, ps.msgBuf.remaining(), i, len, ps.ma.size, avail,
                                // toCopy, buf.length);
                                ps.msgBuf.put(buf, i, toCopy);
                                // Update our index
                                i += toCopy - 1;
                            } else {
                                ps.msgBuf.put(b);
                            }
                        }
                    } else if (i - ps.as >= ps.ma.size) {
                        // System.err.printf("i=%d, ps.as=%d, ps.ma.size=%d\n", i, ps.as,
                        // ps.ma.size);
                        // If we are at or past the end of the payload, go ahead and process it, no
                        // buffering needed.
                        nc.processMsg(buf, ps.as, i - ps.as); // pass offset and length
                        done = true;
                    }

                    if (done) {
                        ps.argBuf = null;
                        // ps.argBuf.clear();
                        ps.msgBuf = null;
                        // ps.msgBuf.clear();
                        ps.state = NatsOp.MSG_END;
                    }

                    break;
                case MSG_END:
                    switch (b) {
                        case '\n':
                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = NatsOp.OP_START;
                            break;
                        default:
                            continue;
                    }
                    break;
                case OP_PLUS:
                    switch (b) {
                        case 'O':
                        case 'o':
                            ps.state = NatsOp.OP_PLUS_O;
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
                            ps.state = NatsOp.OP_PLUS_OK;
                            break;
                        default:
                            error = true;
                            break;
                    }
                    break;
                case OP_PLUS_OK:
                    switch (b) {
                        case '\n':
                            nc.processOK();
                            ps.drop = 0;
                            ps.state = NatsOp.OP_START;
                            break;
                    }
                    break;
                case OP_MINUS:
                    switch (b) {
                        case 'E':
                        case 'e':
                            ps.state = NatsOp.OP_MINUS_E;
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
                            ps.state = NatsOp.OP_MINUS_ER;
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
                            ps.state = NatsOp.OP_MINUS_ERR;
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
                            ps.state = NatsOp.OP_MINUS_ERR_SPC;
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
                            ps.state = NatsOp.MINUS_ERR_ARG;
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
                            ByteBuffer arg = null;
                            if (ps.argBuf != null) {
                                arg = ps.argBuf;
                                ps.argBuf = null;
                            } else {
                                arg = ByteBuffer.wrap(buf, ps.as, i - ps.as);
                            }
                            nc.processErr(arg);
                            ps.drop = 0;
                            ps.as = i + 1;
                            ps.state = NatsOp.OP_START;
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
                            ps.state = NatsOp.OP_PI;
                            break;
                        case 'O':
                        case 'o':
                            ps.state = NatsOp.OP_PO;
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
                            ps.state = NatsOp.OP_PON;
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
                            ps.state = NatsOp.OP_PONG;
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
                            ps.state = NatsOp.OP_START;
                            break;
                        default:
                            break;
                    }
                    break;
                case OP_PI:
                    switch (b) {
                        case 'N':
                        case 'n':
                            ps.state = NatsOp.OP_PIN;
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
                            ps.state = NatsOp.OP_PING;
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
                            ps.state = NatsOp.OP_START;
                            break;
                    }
                    break;
                default:
                    error = true;
                    break;
            } // switch(ps.state)

            if (error) {
                error = false;
                throw new ParseException(String.format("nats: parse error [%s]: '%s'", ps.state,
                        new String(buf, i, len - i)), i);
            }
            // System.err.printf("After processing index %d, ps.state=%s\n", i, ps.state );
        } // for

        // We have processed the entire buffer
        // Check for split buffer scenarios
        if ((ps.state == NatsOp.MSG_ARG || ps.state == NatsOp.MINUS_ERR_ARG)
                && (ps.argBuf == null)) {
            ps.argBuf = ByteBuffer.wrap(ps.argBufStore);
            ps.argBuf.put(buf, ps.as, i - ps.drop - ps.as);
            // System.err.printf("split msg, no clone, ps.argBuf=%s\n", ps.argBuf);
            // FIXME, check max len
        }
        // Check for split msg
        if (ps.state == NatsOp.MSG_PAYLOAD && ps.msgBuf == null) {
            // We need to clone the msgArg if it is still referencing the
            // read buffer and we are not able to process the msg.
            if (ps.argBuf == null) {
                cloneMsgArg();
                // System.err.printf("split msg, after clone, ps.argBuf=%s\n", ps.argBuf);
            }

            // If we will overflow the scratch buffer, just create a
            // new buffer to hold the split message.
            int lrem = len - ps.as; // portion of msg remaining in buffer
            if (ps.ma.size > ps.msgBufStore.length) {
                ps.msgBufStore = new byte[ps.ma.size];
            }
            ps.msgBuf = ByteBuffer.wrap(ps.msgBufStore);
            // copy what's left in the buffer
            ps.msgBuf.put(buf, ps.as, lrem);
        }
    }

    protected static String bufToString(ByteBuffer arg) {
        if (arg == null) {
            return null;
        }
        int pos = arg.position();
        int len = arg.limit() - arg.position();

        byte[] stringBytes = new byte[len];
        arg.get(stringBytes, 0, len);
        arg.position(pos);
        String str = new String(stringBytes);
        return str;
    }

    protected void processMsgArgs(byte[] arg, int offset, int length) throws ParseException {
        // if (logger.isDebugEnabled()) {
        // logger.trace("processMsgArgs (limit={}) content=[{}]\n", buffer.limit(),
        // bufToString(buffer));
        // }
        // System.err.printf("offset=%d, length=%d\n", offset, length);
        int argLen = 0;
        int numArgs = 0;
        int start = -1;
        byte b;
        int i;
        for (i = offset; i < offset + length; i++) {
            b = arg[i];
            // System.err.println(String.format("Considering [%c]", (char)b));
            switch (b) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    if (start >= 0) {
                        argLen = i - start;
                        // System.err.printf("start = %d, len = %d, ps.args[%d]=%s\n", start,
                        // argLen, numArgs, ps.args[numArgs]);
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

        // for (i = 0; i<ps.args.length; i++) {
        // if (ps.args[i] != null) {
        // String s = new String(ps.args[i].array());
        // System.err.printf("arg[%d]=[%s], argLen=%d\n", i, s, s.length());
        // }
        // }

        // System.err.println("ps.args[0] = " + ps.args[0]);
        // System.err.println("ps.args[1] = " + ps.args[1]);
        // System.err.println("ps.args[2] = " + ps.args[2]);
        // System.err.println("ps.args[3] = " + ps.args[3]);

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
        // ps.argBuf.flip();
        // System.err.printf("ps.argBuf 4= %s\n", ps.argBuf);
    }

    // parseInt64 expects decimal positive numbers. We
    // return -1 to signal error
    static long parseLong(byte[] d, int length) {
        long n = 0;
        if (length == 0) {
            return -1;
        }
        byte dec;
        for (int i = 0; i < length; i++) {
            dec = d[i];
            if (dec < ascii_0 || dec > ascii_9) {
                return -1;
            }
            n = (n * 10) + dec - ascii_0;
        }
        return n;
    }
}
