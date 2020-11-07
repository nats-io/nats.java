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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsConnectionReader implements Runnable {
    static final int MAX_PROTOCOL_OP_LENGTH = 7;
    static final ByteBuffer UNKNOWN_OP = ByteBuffer.wrap(new byte[] { 'U', 'N', 'K', 'N', 'O', 'W', 'N' });
    static final byte SPACE = ' ';
    static final byte TAB = '\t';

    enum Mode {
        GATHER_OP,
        GATHER_PROTO,
        GATHER_MSG_PROTO,
        PARSE_PROTO,
        GATHER_DATA
    };

    private final NatsConnection connection;

    private ByteBuffer protocolBuffer; // use a byte buffer to assist character decoding

    private boolean gotCR;
    
    private ByteBuffer op;

    private ByteBuffer msgLineChars;

    private Mode mode;

    private NatsMessage incoming;
    private ByteBuffer msgData;
    
    private ByteBuffer buffer;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private final AtomicBoolean running;

    NatsConnectionReader(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        this.protocolBuffer = ByteBuffer.allocate(this.connection.getOptions().getMaxControlLine());
        this.msgLineChars = ByteBuffer.allocate(this.connection.getOptions().getMaxControlLine());
        this.buffer = ByteBuffer.allocate(connection.getOptions().getBufferSize());
        this.op = ByteBuffer.allocate(MAX_PROTOCOL_OP_LENGTH);
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.dataPortFuture = dataPortFuture;
        this.running.set(true);
        this.stopped = connection.getExecutor().submit(this, Boolean.TRUE);
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.running.set(false);
        return stopped;
    }

    @Override
    public void run() {
        try {
            DataPort dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            this.mode = Mode.GATHER_OP;
            this.gotCR = false;

            while (this.running.get()) {
                int bytesRead = dataPort.read(this.buffer).get();

                if (bytesRead > 0) {
                    this.buffer.flip();
                    connection.getNatsStatistics().registerRead(bytesRead);

                    while (this.buffer.hasRemaining()) {
                        if (this.mode == Mode.GATHER_OP) {
                            this.gatherOp(bytesRead);
                        } else if (this.mode == Mode.GATHER_MSG_PROTO) {
                            this.gatherMessageProtocol(bytesRead);
                        } else if (this.mode == Mode.GATHER_PROTO) {
                            this.gatherProtocol(bytesRead);
                        } else {
                            this.gatherMessageData(bytesRead);
                        }

                        if (this.mode == Mode.PARSE_PROTO) { // Could be the end of the read
                            this.parseProtocolMessage();
                            this.protocolBuffer.clear();
                        }
                    }
                    this.buffer.compact();
                } else if (bytesRead < 0) {
                    throw new IOException("Read channel closed.");
                } else {
                    this.connection.getNatsStatistics().registerRead(bytesRead); // track the 0
                }
            }
        } catch (IOException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (ExecutionException ex) {
            final Throwable cause = ex.getCause();
            if (cause instanceof IOException) {
                this.connection.handleCommunicationIssue((IOException) cause);
            }
        } catch (CancellationException | InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
            // Clear the buffers, since they are only used inside this try/catch
            // We will reuse later
            this.op.clear();
            this.buffer.clear();
            this.protocolBuffer.clear();
        }
    }

    // Gather the op, either up to the first space or the first carriage return.
    void gatherOp(int maxPos) throws IOException {
        try {
            while(this.buffer.hasRemaining()) {
                byte b = this.buffer.get();

                if (gotCR) {
                    if (b == NatsConnection.LF) { // Got CRLF, jump to parsing
                        this.op.flip();
                        this.gotCR = false;
                        this.mode = Mode.PARSE_PROTO;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == SPACE || b == TAB) { // Got a space, get the rest of the protocol line
                    this.op.flip();
                    if (this.op.equals(NatsConnection.OP_MSG)) {
                        this.msgLineChars.clear();
                        this.mode = Mode.GATHER_MSG_PROTO;
                    } else {
                        this.mode = Mode.GATHER_PROTO;
                    }
                    break;
                } else if (b == NatsConnection.CR) {
                    this.gotCR = true;
                } else if (!this.op.hasRemaining()) {
                    this.encounteredProtocolError(new IOException("OP > MAX_PROTOCOL_OP_LENGTH"));
                } else if (b > 0x60 && b < 0x7B) {
                    this.op.put((byte) (b - 0x20));
                } else {
                    this.op.put(b);
                }
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Stores the message protocol line in a char buffer that will be grepped for subject, reply
    void gatherMessageProtocol(int maxPos) throws IOException {
        try {
            while(this.buffer.hasRemaining()) {
                byte b = this.buffer.get();

                if (gotCR) {
                    if (b == NatsConnection.LF) {
                        this.mode = Mode.PARSE_PROTO;
                        this.gotCR = false;
                        this.msgLineChars.flip();
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == NatsConnection.CR) {
                    this.gotCR = true;
                } else {
                    if (!this.msgLineChars.hasRemaining()) {
                        throw new IllegalStateException("Protocol line is too long");
                    }
                    this.msgLineChars.put(b); // Assumes ascii, as per protocol doc
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
            while(this.buffer.hasRemaining()) {
                byte b = this.buffer.get();

                if (gotCR) {
                    if (b == NatsConnection.LF) {
                        this.protocolBuffer.flip();
                        this.mode = Mode.PARSE_PROTO;
                        this.gotCR = false;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == NatsConnection.CR) {
                    this.gotCR = true;
                } else {
                    if (!protocolBuffer.hasRemaining()) {
                        if (protocolBuffer.limit() == protocolBuffer.capacity()) {
                            this.protocolBuffer = this.connection.enlargeBuffer(this.protocolBuffer, 0); // just double it
                        } else {
                            this.protocolBuffer.limit(this.protocolBuffer.limit() + 1);
                        }
                    }
                    this.protocolBuffer.put(b);
                }
            }
        } catch (IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Gather bytes for a message body into a byte array that is then
    // given to the message object
    void gatherMessageData(int maxPos) throws IOException {
        try {
            while(this.buffer.hasRemaining()) {
                if (this.msgData.hasRemaining()) {
                    int end = this.buffer.position() + Math.min(this.buffer.remaining(), this.msgData.remaining());
                    int tmpLimit = this.buffer.limit();
                    this.buffer.limit(end);
                    this.msgData.put(this.buffer);
                    this.buffer.limit(tmpLimit);
                    continue;
                }

                byte b = this.buffer.get();

                // Grab all we can, until we get to the CR/LF
                if (gotCR) {
                    if (b == NatsConnection.LF) {
                        byte[] msgDataBytes = this.msgData.array();
                        incoming.setData(msgDataBytes);
                        this.connection.deliverMessage(incoming);
                        incoming = null;
                        gotCR = false;
                        this.op.clear();
                        this.mode = Mode.GATHER_OP;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == NatsConnection.CR) {
                    gotCR = true;
                } else {
                    throw new IllegalStateException("Bad socket data, no CRLF after data");
                }
            }
        } catch (IllegalStateException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    public ByteBuffer grabNextMessageLineElement(int max) {
        if (!this.msgLineChars.hasRemaining()) {
            return null;
        }

        int start = this.msgLineChars.position();
        while (this.msgLineChars.hasRemaining()) {
            byte c = this.msgLineChars.get();

            if (c == SPACE || c == TAB) {
                ByteBuffer msg = this.msgLineChars.duplicate();
                msg.position(start);
                msg.limit(this.msgLineChars.position() - 1);
                ByteBuffer slice = ByteBuffer.allocate(this.msgLineChars.position() - start - 1); //don't grab the space, avoid an intermediate char sequence
                slice.put(msg);
                slice.flip();
                return slice;
            }
        }

        ByteBuffer msg = this.msgLineChars.duplicate();
        msg.position(start);
        msg.limit(this.msgLineChars.position());
        ByteBuffer slice = ByteBuffer.allocate(this.msgLineChars.position() - start);
        slice.put(msg);
        slice.flip();
        return slice;
    }

    private static int[] TENS = new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

    public static int parseLength(ByteBuffer s) throws NumberFormatException {
        int length = s.remaining();
        int retVal = 0;

        if (length > TENS.length) {
            throw new NumberFormatException("Long in message length \"" + s + "\" "+length+" > "+TENS.length);
        }

        s.mark();
        while (s.hasRemaining()) {
            byte b = s.get();
            if (b > '9' || b < '0') {
                s.reset();
                throw new NumberFormatException("Invalid char in message length \'" + b + "\'");
            }
            int d = (b - '0');

            retVal += d * TENS[length - s.position()];
        }
        s.reset();

        return retVal;
    }

    void parseProtocolMessage() throws IOException {
        try {
            switch (this.op.get(0)) {
                case (byte)'M':
                    if (this.op.equals(NatsConnection.OP_MSG)) {
                        int protocolLength = this.msgLineChars.remaining(); //This is just after the last character
                        int protocolLineLength = protocolLength + 4; // 4 for the "MSG "

                        ByteBuffer subject = grabNextMessageLineElement(this.msgLineChars.remaining());
                        ByteBuffer sid = grabNextMessageLineElement(this.msgLineChars.remaining());
                        ByteBuffer replyTo = grabNextMessageLineElement(this.msgLineChars.remaining());
                        ByteBuffer lengthChars;

                        if (this.msgLineChars.hasRemaining()) {
                            lengthChars = grabNextMessageLineElement(this.msgLineChars.remaining());
                        } else {
                            lengthChars = replyTo;
                            replyTo = null;
                        }

                        if(subject==null || subject.limit() == 0 || sid==null || sid.limit() == 0 || lengthChars==null) {
                            throw new IllegalStateException("Bad MSG control line, missing required fields");
                        }

                        int incomingLength = parseLength(lengthChars);

                        this.incoming = new NatsMessage(sid, subject, replyTo, protocolLineLength);
                        this.mode = Mode.GATHER_DATA;
                        this.msgData = ByteBuffer.allocate(incomingLength);
                        this.msgLineChars.clear();
                        break;
                    }
                case (byte)'+':
                    if (this.op.equals(NatsConnection.OP_OK)) {
                        this.connection.processOK();
                        this.op.clear();
                        this.mode = Mode.GATHER_OP;
                        break;
                    }
                case (byte)'-':
                    if (this.op.equals(NatsConnection.OP_ERR)) {
                        String errorText = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
                        if (errorText != null) {
                            errorText = errorText.replace("\'", "");
                        }
                        this.connection.processError(errorText);
                        this.op.clear();
                        this.mode = Mode.GATHER_OP;
                        break;
                    }
                case (byte)'P':
                    switch (this.op.get(1)) {
                        case (byte)'I':
                            if (this.op.equals(NatsConnection.OP_PING)) {
                                this.connection.sendPong();
                                this.op.clear();
                                this.mode = Mode.GATHER_OP;
                                break;
                            }
                        case (byte)'O':
                            if (this.op.equals(NatsConnection.OP_PONG)) {
                                this.connection.handlePong();
                                this.op.clear();
                                this.mode = Mode.GATHER_OP;
                                break;
                            }
                        default:
                            throw new IllegalStateException("Unknown protocol operation "+op);
                    }
                    break;
                case (byte)'I':
                    if (this.op.equals(NatsConnection.OP_INFO)) {
                        String info = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
                        this.connection.handleInfo(info);
                        this.op.clear();
                        this.mode = Mode.GATHER_OP;
                        break;
                    }
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
        this.op.clear();
        this.buffer.clear();
        this.protocolBuffer.clear();
        this.buffer.put(bytes);
        this.buffer.flip();
        this.mode = Mode.GATHER_OP;
    }

    ByteBuffer currentOp() {
        if (this.mode == Mode.PARSE_PROTO) {
            ByteBuffer tmp = this.op.duplicate();
            try {
                parseProtocolMessage();
                return tmp;
            } catch (IOException e) {
                return UNKNOWN_OP.duplicate();
            }
        }
        return this.op.duplicate();
    }
}