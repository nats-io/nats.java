// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.ReadListener;
import io.nats.client.support.IncomingHeadersProcessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.support.NatsConstants.*;

class NatsConnectionReader implements Runnable {

    enum Mode {
        GATHER_OP,
        GATHER_PROTO,
        GATHER_MSG_HMSG_PROTO,
        PARSE_PROTO,
        GATHER_HEADERS,
        GATHER_DATA
    };

    private final NatsConnection connection;

    private ByteBuffer protocolBuffer; // use a byte buffer to assist character decoding

    private boolean gotCR;
    
    private String op;
    private final char[] opArray;
    private int opPos;

    private final char[] msgLineChars;
    private int msgLinePosition;

    private Mode mode;

    private IncomingMessageFactory incoming;
    private byte[] msgHeaders;
    private byte[] msgData;
    private int msgHeadersPosition;
    private int msgDataPosition;

    private final byte[] buffer;
    private int bufferPosition;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private DataPort dataPort;
    private final AtomicBoolean running;

    private final boolean utf8Mode;
    private final ReadListener readListener;

    NatsConnectionReader(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        this.protocolBuffer = ByteBuffer.allocate(this.connection.getOptions().getMaxControlLine());
        this.msgLineChars = new char[this.connection.getOptions().getMaxControlLine()];
        this.opArray = new char[MAX_PROTOCOL_RECEIVE_OP_LENGTH];
        this.buffer = new byte[connection.getOptions().getBufferSize()];
        this.bufferPosition = 0;

        this.utf8Mode = connection.getOptions().supportUTF8Subjects();
        readListener = connection.getOptions().getReadListener();
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.dataPortFuture = dataPortFuture;
        this.running.set(true);
        this.stopped = connection.getExecutor().submit(this, Boolean.TRUE);
    }

    Future<Boolean> stop() {
        return stop(true);
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop(boolean shutdownDataPort) {
        if (running.get()) {
            running.set(false);
            if (shutdownDataPort && dataPort != null) {
                try {
                    dataPort.shutdownInput();
                }
                catch (IOException e) {
                    // we don't care, we are shutting down anyway
                }
            }
        }
        return stopped;
    }

    boolean isRunning() {
        return running.get();
    }

    @Override
    public void run() {
        try {
            dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            this.mode = Mode.GATHER_OP;
            this.gotCR = false;
            this.opPos = 0;

            while (running.get() && !Thread.interrupted()) {
                this.bufferPosition = 0;
                int bytesRead = dataPort.read(this.buffer, 0, this.buffer.length);

                if (bytesRead > 0) {
                    connection.getStatisticsCollector().registerRead(bytesRead);

                    while (this.bufferPosition < bytesRead) {
                        if (this.mode == Mode.GATHER_OP) {
                            this.gatherOp(bytesRead);
                        }
                        else if (this.mode == Mode.GATHER_MSG_HMSG_PROTO) {
                            if (this.utf8Mode) {
                                this.gatherProtocol(bytesRead);
                            } else {
                                this.gatherMessageProtocol(bytesRead);
                            }
                        }
                        else if (this.mode == Mode.GATHER_PROTO) {
                            this.gatherProtocol(bytesRead);
                        }
                        else if (this.mode == Mode.GATHER_HEADERS) {
                            this.gatherHeaders(bytesRead);
                        }
                        else {  // Mode.GATHER_DATA
                            this.gatherMessageData(bytesRead);
                        }

                        if (this.mode == Mode.PARSE_PROTO) { // Could be the end of the read
                            this.parseProtocolMessage();
                            this.protocolBuffer.clear();
                        }
                    }
                } else if (bytesRead < 0) {
                    throw new IOException("Read channel closed.");
                } else {
                    this.connection.getStatisticsCollector().registerRead(bytesRead); // track the 0
                }
            }
        } catch (IOException io) {
            // if already not running, an IOE is not unreasonable in a transition state
            if (running.get()) {
                this.connection.handleCommunicationIssue(io);
            }
        } catch (CancellationException | ExecutionException ex) {
            // Exit
        } catch (InterruptedException ex) {
            // Exit
            Thread.currentThread().interrupt();
        } finally {
            this.running.set(false);
            // Clear the buffers, since they are only used inside this try/catch
            // We will reuse later
            this.protocolBuffer.clear();
        }
    }

    // Gather the op, either up to the first space or the first carriage return.
    void gatherOp(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == LF) { // Got CRLF, jump to parsing
                        this.op = opFor(opArray, opPos);
                        this.gotCR = false;
                        this.opPos = 0;
                        this.mode = Mode.PARSE_PROTO;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == SP || b == TAB) { // Got a space, get the rest of the protocol line
                    this.op = opFor(opArray, opPos);
                    this.opPos = 0;
                    if (this.op.equals(OP_MSG) || this.op.equals(OP_HMSG)) {
                        this.msgLinePosition = 0;
                        this.mode = Mode.GATHER_MSG_HMSG_PROTO;
                    } else {
                        this.mode = Mode.GATHER_PROTO;
                    }
                    break;
                } else if (b == CR) {
                    this.gotCR = true;
                } else {
                    this.opArray[opPos] = (char) b;
                    this.opPos++;
                }
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Stores the message protocol line in a char buffer that will be read for subject, reply
    void gatherMessageProtocol(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == LF) {
                        this.mode = Mode.PARSE_PROTO;
                        this.gotCR = false;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == CR) {
                    this.gotCR = true;
                } else {
                    if (this.msgLinePosition >= this.msgLineChars.length) {
                        throw new IllegalStateException("Protocol line is too long");
                    }
                    this.msgLineChars[this.msgLinePosition] = (char) b; // Assumes ascii, as per protocol doc
                    this.msgLinePosition++;
                }
            }
        } catch (IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Gather bytes for a protocol line
    void gatherProtocol(int maxPos) throws IOException {
        // protocol buffer has max capacity, shouldn't need resizing
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == LF) {
                        this.protocolBuffer.flip();
                        this.mode = Mode.PARSE_PROTO;
                        this.gotCR = false;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == CR) {
                    this.gotCR = true;
                } else {
                    if (!protocolBuffer.hasRemaining()) {
                        this.protocolBuffer = this.connection.enlargeBuffer(this.protocolBuffer); // just double it
                    }
                    this.protocolBuffer.put(b);
                }
            }
        } catch (IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    void gatherHeaders(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                int possible = maxPos - this.bufferPosition;
                int want = msgHeaders.length - msgHeadersPosition;

                // Grab all we can, until we get the necessary number of bytes
                if (want > 0 && want <= possible) {
                    System.arraycopy(this.buffer, this.bufferPosition, this.msgHeaders, this.msgHeadersPosition, want);
                    msgHeadersPosition += want;
                    this.bufferPosition += want;
                    continue;
                } else if (want > 0) {
                    System.arraycopy(this.buffer, this.bufferPosition, this.msgHeaders, this.msgHeadersPosition, possible);
                    msgHeadersPosition += possible;
                    this.bufferPosition += possible;
                    continue;
                }

                if (msgHeadersPosition == msgHeaders.length) {
                    incoming.setHeaders(new IncomingHeadersProcessor(msgHeaders));
                    msgHeaders = null;
                    msgHeadersPosition = -1;
                    this.mode = Mode.GATHER_DATA;
                    break;
                } else {
                    throw new IllegalStateException("Bad socket data, headers do not match expected length");
                }
            }
        } catch (IllegalStateException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Gather bytes for a message body into a byte array that is then
    // given to the message object
    void gatherMessageData(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                int possible = maxPos - this.bufferPosition;
                int want = msgData.length - msgDataPosition;

                // Grab all we can, until we get to the CR/LF
                if (want > 0 && want <= possible) {
                    System.arraycopy(this.buffer, this.bufferPosition, this.msgData, this.msgDataPosition, want);
                    msgDataPosition += want;
                    this.bufferPosition += want;
                    continue;
                } else if (want > 0) {
                    System.arraycopy(this.buffer, this.bufferPosition, this.msgData, this.msgDataPosition, possible);
                    msgDataPosition += possible;
                    this.bufferPosition += possible;
                    continue;
                }

                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == LF) {
                        incoming.setData(msgData);
                        NatsMessage m = incoming.getMessage();
                        this.connection.deliverMessage(m);
                        if (readListener != null) {
                            readListener.message(op, m);
                        }
                        msgData = null;
                        msgDataPosition = 0;
                        incoming = null;
                        gotCR = false;
                        this.op = UNKNOWN_OP;
                        this.mode = Mode.GATHER_OP;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == CR) {
                    gotCR = true;
                } else {
                    throw new IllegalStateException("Bad socket data, no CRLF after data");
                }
            }
        } catch (IllegalStateException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    public String grabNextMessageLineElement(int max) {
        if (this.msgLinePosition >= max) {
            return null;
        }

        int start = this.msgLinePosition;

        while (this.msgLinePosition < max) {
            char c = this.msgLineChars[this.msgLinePosition];
            this.msgLinePosition++;

            if (c == SP || c == TAB) {
                return new String(this.msgLineChars, start, this.msgLinePosition - start -1); //don't grab the space, avoid an intermediate char sequence
            }
        }

        return new String(this.msgLineChars, start, this.msgLinePosition-start);
    }

    static String opFor(char[] chars, int length) {
        if (length == 3) {
            if ((chars[0] == 'M' || chars[0] == 'm') &&
                        (chars[1] == 'S' || chars[1] == 's') && 
                        (chars[2] == 'G' || chars[2] == 'g')) {
                return OP_MSG;
            } else if (chars[0] == '+' && 
                (chars[1] == 'O' || chars[1] == 'o') && 
                (chars[2] == 'K' || chars[2] == 'k')) {
                return OP_OK;
            } else {
                return UNKNOWN_OP;
            }
        } else if (length == 4) { // do them in a unique order for uniqueness when possible to branch asap
            if ((chars[1] == 'I' || chars[1] == 'i') && 
                    (chars[0] == 'P' || chars[0] == 'p') && 
                    (chars[2] == 'N' || chars[2] == 'n') &&
                    (chars[3] == 'G' || chars[3] == 'g')) {
                return OP_PING;
            } else if ((chars[1] == 'O' || chars[1] == 'o') && 
                        (chars[0] == 'P' || chars[0] == 'p') && 
                        (chars[2] == 'N' || chars[2] == 'n') &&
                        (chars[3] == 'G' || chars[3] == 'g')) {
                return OP_PONG;
            } else if (chars[0] == '-' && 
                        (chars[1] == 'E' || chars[1] == 'e') &&
                        (chars[2] == 'R' || chars[2] == 'r') && 
                        (chars[3] == 'R' || chars[3] == 'r')) {
                return OP_ERR;
            } else if ((chars[0] == 'I' || chars[0] == 'i') &&
                    (chars[1] == 'N' || chars[1] == 'n') &&
                    (chars[2] == 'F' || chars[2] == 'f') &&
                    (chars[3] == 'O' || chars[3] == 'o')) {
                return OP_INFO;
            } else if ((chars[0] == 'H' || chars[0] == 'h') &&
                    (chars[1] == 'M' || chars[1] == 'm') &&
                    (chars[2] == 'S' || chars[2] == 's') &&
                    (chars[3] == 'G' || chars[3] == 'g')) {
                return OP_HMSG;
            }  else {
                return UNKNOWN_OP;
            }
        } else {
            return UNKNOWN_OP;
        }
    }

    private static final int[] TENS = new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

    public static int parseLength(String s) throws NumberFormatException {
        int length = s.length();
        int retVal = 0;

        if (length > TENS.length) {
            throw new NumberFormatException("Long in message length \"" + s + "\" "+length+" > "+TENS.length);
        }
        
        for (int i=length-1;i>=0;i--) {
            char c = s.charAt(i);
            int d = (c - '0');

            if (d>9) {
                throw new NumberFormatException("Invalid char in message length '" + c + "'");
            }

            retVal += d * TENS[length - i - 1];
        }

        return retVal;
    }

    void parseProtocolMessage() throws IOException {
        try {
            switch (this.op) {
                case OP_MSG:
                    int protocolLength = this.msgLinePosition; //This is just after the last character
                    int protocolLineLength = protocolLength + 4; // 4 for the "MSG "

                    if (this.utf8Mode) {
                        protocolLineLength = protocolBuffer.remaining() + 4;

                        CharBuffer buff = StandardCharsets.UTF_8.decode(protocolBuffer);
                        protocolLength = buff.remaining();
                        buff.get(this.msgLineChars, 0, protocolLength);
                    }

                    this.msgLinePosition = 0;
                    String subject = grabNextMessageLineElement(protocolLength);
                    String sid = grabNextMessageLineElement(protocolLength);
                    String replyTo = grabNextMessageLineElement(protocolLength);
                    String lengthChars = null;

                    if (this.msgLinePosition < protocolLength) {
                        lengthChars = grabNextMessageLineElement(protocolLength);
                    } else {
                        lengthChars = replyTo;
                        replyTo = null;
                    }

                    if (subject == null || subject.isEmpty() || sid == null || sid.isEmpty() || lengthChars == null) {
                        throw new IllegalStateException("Bad MSG control line, missing required fields");
                    }

                    int incomingLength = parseLength(lengthChars);

                    this.incoming = new IncomingMessageFactory(sid, subject, replyTo, protocolLineLength, utf8Mode);
                    this.mode = Mode.GATHER_DATA;
                    this.msgData = new byte[incomingLength];
                    this.msgDataPosition = 0;
                    this.msgLinePosition = 0;
                    break;
                case OP_HMSG:
                    int hProtocolLength = this.msgLinePosition; //This is just after the last character
                    int hProtocolLineLength = hProtocolLength + 5; // 5 for the "HMSG "

                    if (this.utf8Mode) {
                        hProtocolLineLength = protocolBuffer.remaining() + 5;

                        CharBuffer buff = StandardCharsets.UTF_8.decode(protocolBuffer);
                        hProtocolLength = buff.remaining();
                        buff.get(this.msgLineChars, 0, hProtocolLength);
                    }

                    this.msgLinePosition = 0;
                    String hSubject = grabNextMessageLineElement(hProtocolLength);
                    String hSid = grabNextMessageLineElement(hProtocolLength);
                    String replyToOrHdrLen = grabNextMessageLineElement(hProtocolLength);
                    String hdrLenOrTotLen = grabNextMessageLineElement(hProtocolLength);

                    String hReplyTo = null;
                    int hdrLen = -1;
                    int totLen = -1;

                    // if there is more it must be replyTo hdrLen totLen instead of just hdrLen totLen
                    if (this.msgLinePosition < hProtocolLength) {
                        hReplyTo = replyToOrHdrLen;
                        hdrLen = parseLength(hdrLenOrTotLen);
                        totLen = parseLength(grabNextMessageLineElement(hProtocolLength));
                    } else {
                        hdrLen = parseLength(replyToOrHdrLen);
                        totLen = parseLength(hdrLenOrTotLen);
                    }

                    if(hSubject==null || hSubject.isEmpty() || hSid==null || hSid.isEmpty()) {
                        throw new IllegalStateException("Bad HMSG control line, missing required fields");
                    }

                    this.incoming = new IncomingMessageFactory(hSid, hSubject, hReplyTo, hProtocolLineLength, utf8Mode);
                    this.msgHeaders = new byte[hdrLen];
                    this.msgData = new byte[totLen - hdrLen];
                    this.mode = Mode.GATHER_HEADERS;
                    this.msgHeadersPosition = 0;
                    this.msgDataPosition = 0;
                    this.msgLinePosition = 0;
                    break;
                case OP_OK:
                    this.connection.processOK();
                    if (readListener != null) {
                        readListener.protocol(op, null);
                    }
                    this.op = UNKNOWN_OP;
                    this.mode = Mode.GATHER_OP;
                    break;
                case OP_ERR:
                    String errorText = StandardCharsets.UTF_8.decode(protocolBuffer).toString().replace("'", "");
                    this.connection.processError(errorText);
                    if (readListener != null) {
                        readListener.protocol(op, errorText);
                    }
                    this.op = UNKNOWN_OP;
                    this.mode = Mode.GATHER_OP;
                    break;
                case OP_PING:
                    this.connection.sendPong();
                    if (readListener != null) {
                        readListener.protocol(op, null);
                    }
                    this.op = UNKNOWN_OP;
                    this.mode = Mode.GATHER_OP;
                    break;
                case OP_PONG:
                    this.connection.handlePong();
                    if (readListener != null) {
                        readListener.protocol(op, null);
                    }
                    this.op = UNKNOWN_OP;
                    this.mode = Mode.GATHER_OP;
                    break;
                case OP_INFO:
                    String info = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
                    this.connection.handleInfo(info);
                    if (readListener != null) {
                        readListener.protocol(op, info);
                    }
                    this.op = UNKNOWN_OP;
                    this.mode = Mode.GATHER_OP;
                    break;
                default:
                    throw new IllegalStateException("Unknown protocol operation "+op);
            }

        } catch (IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    void encounteredProtocolError(Exception ex) throws IOException {
        throw new IOException(ex);
    }

    //For testing
    void fakeReadForTest(byte[] bytes) {
        System.arraycopy(bytes, 0, this.buffer, 0, bytes.length);
        this.bufferPosition = 0;
        this.op = UNKNOWN_OP;
        this.mode = Mode.GATHER_OP;
    }

    String currentOp() {
        return this.op;
    }
}