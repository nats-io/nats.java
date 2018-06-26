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
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsConnectionReader implements Runnable {
    static final int MAX_PROTOCOL_OP_LENGTH = 4;
    static final String UNKNOWN_OP = "UNKNOWN";

    private enum Mode {
        GATHER_OP,
        GATHER_PROTO,
        PARSE_PROTO,
        GATHER_DATA
    };

    private final NatsConnection connection;

    private ByteBuffer protocolBuffer; // use a byte buffer to assist character decoding

    private boolean gotCR;
    
    private String op;
    private char[] opArray;
    private int opPos;

    private Mode mode;

    private NatsMessage incoming;
    private byte[] gatherer;
    private int gatherPosition;
    
    private byte[] buffer;
    private int bufferPosition;
    
    private Thread thread;
    private CompletableFuture<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private final AtomicBoolean running;

    NatsConnectionReader(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        this.stopped.complete(Boolean.TRUE); // we are stopped on creation

        this.protocolBuffer = ByteBuffer.allocate(this.connection.getOptions().getMaxControlLine());
        this.opArray = new char[MAX_PROTOCOL_OP_LENGTH];
        this.buffer = new byte[connection.getOptions().getBufferSize()];
        this.bufferPosition = 0;
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.dataPortFuture = dataPortFuture;
        this.running.set(true);
        this.stopped = new CompletableFuture<>(); // New future
        this.thread = new Thread(this);
        this.thread.start();
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.running.set(false);
        return stopped;
    }

    public void run() {
        try {
            DataPort dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            this.mode = Mode.GATHER_OP;

            while (this.running.get()) {
                this.bufferPosition = 0;
                int bytesRead = dataPort.read(this.buffer, 0, this.buffer.length);

                if (bytesRead > 0) {
                    connection.getNatsStatistics().registerRead(bytesRead);

                    while (this.bufferPosition < bytesRead) {
                        if (this.mode == Mode.GATHER_OP) {
                            this.gatherOp(bytesRead);
                        } else if (this.mode == Mode.GATHER_PROTO) {
                            this.gatherProtocol(bytesRead);
                        } else {
                            this.gatherData(bytesRead);
                        }

                        if (this.mode == Mode.PARSE_PROTO) { // Could be the end of the read
                            this.parseProtocolMessage();
                            this.protocolBuffer.clear();
                        }
                    }
                } else if (bytesRead < 0) {
                    throw new IOException("Read channel closed.");
                } else {
                    this.connection.getNatsStatistics().registerRead(bytesRead); // track the 0
                }
            }
        } catch (IOException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
            // Clear the buffers, since they are only used inside this try/catch
            // We will reuse later
            this.protocolBuffer.clear();
            this.stopped.complete(Boolean.TRUE);
            this.thread = null;
        }
    }

    // Gather the op, either up to the first space or the first carraige return.
    void gatherOp(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == NatsConnection.LF) { // Got CRLF, jump to parsing
                        this.op = opFor(opArray, opPos);
                        this.gotCR = false;
                        this.opPos = 0;
                        this.mode = Mode.PARSE_PROTO;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == ' ') { // Got a space, get the rest of the protocol line
                    this.op = opFor(opArray, opPos);
                    this.opPos = 0;
                    this.mode = Mode.GATHER_PROTO;
                    break;
                } else if (b == NatsConnection.CR) {
                    this.gotCR = true;
                } else {
                    this.opArray[opPos] = (char) b;
                    this.opPos++;
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
                    if (b == NatsConnection.LF) {
                        protocolBuffer.flip();
                        this.mode = Mode.PARSE_PROTO;
                        gotCR = false;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == NatsConnection.CR) {
                    gotCR = true;
                } else {
                    if (!protocolBuffer.hasRemaining()) {
                        this.protocolBuffer = this.connection.enlargeBuffer(this.protocolBuffer, 0); // just double it
                    }
                    protocolBuffer.put(b);
                }
            }
        } catch (IllegalStateException | NumberFormatException | NullPointerException ex) {
            this.encounteredProtocolError(ex);
        }
    }

    // Gather bytes for a message body
    void gatherData(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gatherPosition < gatherer.length) {
                    gatherer[gatherPosition] = b;
                    gatherPosition++;
                } else if (gotCR) {
                    if (b == NatsConnection.LF) {
                        incoming.setData(gatherer);
                        this.connection.deliverMessage(incoming);
                        gatherer = null;
                        gatherPosition = 0;
                        incoming = null;
                        gotCR = false;
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

    public CharSequence grabNext(CharBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }

        int start = buffer.position();

        while (buffer.hasRemaining()) {
            char c = buffer.get();

            if (c == ' ') {
                int end = buffer.position();
                buffer.position(start);
                CharBuffer slice = buffer.subSequence(0, end-start-1); //don't grab the space
                buffer.position(end);
                return slice;
            }
        }

        buffer.position(start);
        CharSequence retVal = buffer.subSequence(0, buffer.remaining());
        buffer.position(buffer.limit());
        return retVal;
    }

    public String opFor(char[] chars, int length) {
        if (length == 3) {
            if ((chars[0] == 'M' || chars[0] == 'm') &&
                        (chars[1] == 'S' || chars[1] == 's') && 
                        (chars[2] == 'G' || chars[2] == 'g')) {
                return NatsConnection.OP_MSG;
            } else if (chars[0] == '+' && 
                (chars[1] == 'O' || chars[1] == 'o') && 
                (chars[2] == 'K' || chars[2] == 'k')) {
                return NatsConnection.OP_OK;
            } else {
                return UNKNOWN_OP;
            }
        } else if (length == 4) { // do them in a unique order for uniqueness when possible to branch asap
            if ((chars[1] == 'I' || chars[1] == 'i') && 
                    (chars[0] == 'P' || chars[0] == 'p') && 
                    (chars[2] == 'N' || chars[2] == 'n') &&
                    (chars[3] == 'G' || chars[3] == 'g')) {
                return NatsConnection.OP_PING;
            } else if ((chars[1] == 'O' || chars[1] == 'o') && 
                        (chars[0] == 'P' || chars[0] == 'p') && 
                        (chars[2] == 'N' || chars[2] == 'n') &&
                        (chars[3] == 'G' || chars[3] == 'g')) {
                return NatsConnection.OP_PONG;
            } else if (chars[0] == '-' && 
                        (chars[1] == 'E' || chars[1] == 'e') &&
                        (chars[2] == 'R' || chars[2] == 'r') && 
                        (chars[3] == 'R' || chars[3] == 'R')) {
                return NatsConnection.OP_ERR;
            } else if ((chars[2] == 'F' || chars[2] == 'f') && 
                        (chars[0] == 'I' || chars[0] == 'i') && 
                        (chars[1] == 'N' || chars[1] == 'n') && 
                        (chars[3] == 'O' || chars[3] == 'o')) {
                return NatsConnection.OP_INFO;
            }  else {
                return UNKNOWN_OP;
            }
        } else {
            return UNKNOWN_OP;
        }
    }

    private static int[] TENS = new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

    public static int parseLength(CharSequence s) throws NumberFormatException {
        int length = s.length();
        int retVal = 0;

        if (length > TENS.length) {
            throw new NumberFormatException("Long in message length \"" + s + "\"");
        }
        
        for (int i=length-1;i>=0;i--) {
            char c = s.charAt(i);
            int d = (c - '0');

            if (d>9) {
                throw new NumberFormatException("Invalid char in message length \'" + c + "\'");
            }

            retVal += d * TENS[length - i - 1];
        }

        return retVal;
    }

    void parseProtocolMessage() throws IOException {
        try {
            int protocolLength;
            CharBuffer protocolChars;

            switch (this.op) {
            case NatsConnection.OP_MSG:
                protocolLength = protocolBuffer.remaining();
                protocolChars = StandardCharsets.UTF_8.decode(protocolBuffer);

                CharSequence subject = grabNext(protocolChars);
                CharSequence sid = grabNext(protocolChars);
                CharSequence replyTo = grabNext(protocolChars);
                CharSequence lengthChars = grabNext(protocolChars);

                if (lengthChars == null) {
                    lengthChars = replyTo;
                    replyTo = null;
                } else if(subject==null || sid==null || lengthChars==null) {
                    throw new IllegalStateException("Bad MSG control line, missing required fields");
                }

                int incomingLength = parseLength(lengthChars);

                incoming = new NatsMessage(sid.toString(), subject, replyTo, protocolLength);
                this.mode = Mode.GATHER_DATA;
                gatherer = new byte[incomingLength];
                gatherPosition = 0;
                break;
            case NatsConnection.OP_OK:
                this.connection.processOK();
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_ERR:
                protocolChars = StandardCharsets.UTF_8.decode(protocolBuffer);
                
                String errorText = protocolChars.toString();
                if (errorText != null) {
                    errorText = errorText.replace("\'", "");
                }
                this.connection.processError(errorText);
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_PING:
                this.connection.sendPong();
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_PONG:
                this.connection.handlePong();
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_INFO:
                protocolChars = StandardCharsets.UTF_8.decode(protocolBuffer);
                String info = protocolChars.toString();
                this.connection.handleInfo(info);
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
}