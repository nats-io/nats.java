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

import io.nats.client.Options;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsConnectionReader implements Runnable {
    static final int MAX_PROTOCOL_OP_LENGTH = 4;
    static final String UNKNOWN_OP = "UNKNOWN";
    static final char SPACE = ' ';
    static final char TAB = '\t';

    private final ExecutorService executor;

    private Headers headers;
    private int headerLen;
    private int headerStart;

    enum Mode {
        GATHER_OP,
        GATHER_PROTO,
        GATHER_MSG_PROTO,
        PARSE_PROTO,
        GATHER_DATA,
        GATHER_HEADER
    };

    //private final NatsConnection connection;

    private final ProtocolHandler connection;

    private ByteBuffer protocolBuffer; // use a byte buffer to assist character decoding

    private boolean gotCR;
    
    private String op;
    private char[] opArray;
    private int opPos;

    private char[] msgLineChars;
    private int msgLinePosition;

    private Mode mode = Mode.GATHER_OP;

    private NatsMessage.IncomingBuilder incoming;
    private byte[] msgData;
    private int msgDataPosition;
    
    private byte[] buffer;
    private int bufferPosition;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private final AtomicBoolean running;

    private final boolean utf8Mode;

    private final NatsStatistics statistics;




    NatsConnectionReader(ProtocolHandler connection, final Options options, final NatsStatistics statistics,
                         final ExecutorService executor) {
        this.connection = connection;
        this.statistics = statistics;
        this.executor = executor;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        this.protocolBuffer = ByteBuffer.allocate(options.getMaxControlLine());
        this.msgLineChars = new char[options.getMaxControlLine()];
        this.opArray = new char[MAX_PROTOCOL_OP_LENGTH];
        this.buffer = new byte[options.getBufferSize()];
        this.bufferPosition = 0;

        this.utf8Mode = options.supportUTF8Subjects();
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.dataPortFuture = dataPortFuture;
        this.running.set(true);
        this.stopped =this.executor.submit(this, Boolean.TRUE);
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
            init();

            while (this.running.get()) {
                runOnce(dataPort);
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
        }
    }

    void init() {
        this.mode = Mode.GATHER_OP;
        this.gotCR = false;
        this.opPos = 0;
    }

    void runOnce(DataPort dataPort) throws IOException {
        this.bufferPosition = 0;
        int bytesRead = dataPort.read(this.buffer, 0, this.buffer.length);

        if (bytesRead > 0) {
            statistics.registerRead(bytesRead);

            while (this.bufferPosition < bytesRead) {
                if (this.mode == Mode.GATHER_OP) {
                    this.gatherOp(bytesRead);
                } else if (this.mode == Mode.GATHER_MSG_PROTO) {
                    if (this.utf8Mode) {
                        this.gatherProtocol(bytesRead);
                    } else {
                        this.gatherMessageProtocol(bytesRead);
                    }
                } else if (this.mode == Mode.GATHER_PROTO) {
                    this.gatherProtocol(bytesRead);
                } else if (this.mode == Mode.GATHER_DATA){
                    this.gatherMessageData(bytesRead);
                } else if (this.mode == Mode.GATHER_HEADER) {
                    this.gatherHeaders(bytesRead);
                } else {
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
            statistics.registerRead(bytesRead); // track the 0
        }
    }

    private void gatherHeaders(int maxPos)  throws IOException  {

        final int donePosition = headerLen + headerStart + 2;

        if (donePosition > bufferPosition && maxPos > bufferPosition) {
            bufferPosition = donePosition;


            boolean gotCR = false;
            boolean foundKey = false;

            int startHeader = headerStart;
            String key = "";
            int startValue = 0;

            for (int i = headerStart; i < donePosition; i++) {

                byte b = this.buffer[i];

                switch (b) {

                    case ' ' :
                    case '\t':
                        if (foundKey)
                        startValue++;
                        break;

                    case ':' :
                        key = new String(buffer, startHeader, i - startHeader).intern();
                        foundKey = true;
                        startValue = i +1;
                        break;

                    case NatsConnection.LF:
                        if (gotCR && foundKey) {
                            String value = new String(buffer, startValue , (i-1) - startValue).intern();
                            headers.add(key, value);
                            gotCR = false;
                            startValue = 0;
                            key = null;
                            startHeader = i + 1;
                            foundKey = false;
                        }
                        break;

                    case NatsConnection.CR:
                        gotCR = true;
                        break;
                }
            }
        }
        this.mode = Mode.GATHER_DATA;


    }

    // Gather the op, either up to the first space or the first carriage return.
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
                } else if (b == SPACE || b == TAB) { // Got a space, get the rest of the protocol line
                    this.op = opFor(opArray, opPos);
                    this.opPos = 0;
                    if (this.op == NatsConnection.OP_MSG || this.op == NatsConnection.OP_HMSG) {
                        this.msgLinePosition = 0;
                        this.mode = Mode.GATHER_MSG_PROTO;
                    } else {
                        this.mode = Mode.GATHER_PROTO;
                    }
                    break;
                } else if (b == NatsConnection.CR) {
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


    // Stores the message protocol line in a char buffer that will be grepped for subject, reply
    void gatherMessageProtocol(int maxPos) throws IOException {
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

                if (gotCR) {
                    if (b == NatsConnection.LF) {
                        this.mode = Mode.PARSE_PROTO;
                        this.gotCR = false;
                        break;
                    } else {
                        throw new IllegalStateException("Bad socket data, no LF after CR");
                    }
                } else if (b == NatsConnection.CR) {
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

    void gatherProtocol(final int maxPos) throws IOException {
        // protocol buffer has max capacity, shouldn't need resizing
        try {
            while(this.bufferPosition < maxPos) {
                byte b = this.buffer[this.bufferPosition];
                this.bufferPosition++;

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
                        this.protocolBuffer = ByteBufferUtil.enlargeBuffer(this.protocolBuffer, 0); // just double it
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
                    if (b == NatsConnection.LF) {
System.out.println("NCR 365 " + incoming);
                        incoming.headers(this.headers);
                        this.headers = null;
                        incoming.data(msgData);
                        this.connection.deliverMessage(incoming.build());
                        msgData = null;
                        msgDataPosition = 0;
                        // TODO just for test
                        incoming = null;
                        gotCR = false;
                        this.op = UNKNOWN_OP;
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

    public String grabNextMessageLineElement(int max) {
        if (this.msgLinePosition >= max) {
            return null;
        }

        int start = this.msgLinePosition;

        while (this.msgLinePosition < max) {
            char c = this.msgLineChars[this.msgLinePosition];
            this.msgLinePosition++;

            if (c == SPACE || c == TAB) {
                String slice = new String(this.msgLineChars, start, this.msgLinePosition - start -1); //don't grab the space, avoid an intermediate char sequence
                return slice;
            }
        }

        return new String(this.msgLineChars, start, this.msgLinePosition-start);
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
                        (chars[3] == 'R' || chars[3] == 'r')) {
                return NatsConnection.OP_ERR;
            } else if ((chars[0] == 'I' || chars[0] == 'i') && 
                        (chars[1] == 'N' || chars[1] == 'n') && 
                        (chars[2] == 'F' || chars[2] == 'f') &&
                        (chars[3] == 'O' || chars[3] == 'o')) {
                return NatsConnection.OP_INFO;
            } else if (
                    (chars[0] == 'H' || chars[0] == 'h') &&
                            (chars[1] == 'M' || chars[1] == 'm') &&
                            (chars[2] == 'S' || chars[2] == 's') &&
                            (chars[3] == 'G' || chars[3] == 'g')) {
                return NatsConnection.OP_HMSG;
            }  else {
                return UNKNOWN_OP;
            }
        } else {
            return UNKNOWN_OP;
        }
    }

    private static int[] TENS = new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

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
                throw new NumberFormatException("Invalid char in message length \'" + c + "\'");
            }

            retVal += d * TENS[length - i - 1];
        }

        return retVal;
    }

    void parseProtocolMessage() throws IOException {
        try {
            switch (this.op) {
            case NatsConnection.OP_MSG:
                handleProtocolOpMsg();
                break;
            case NatsConnection.OP_HMSG:
                handleProtocolOpHMsg();
                break;
            case NatsConnection.OP_OK:
                this.connection.processOK();
                this.op = UNKNOWN_OP;
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_ERR:
                String errorText = StandardCharsets.UTF_8.decode(protocolBuffer).toString().replace("'", "");
                this.connection.processError(errorText);
                this.op = UNKNOWN_OP;
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_PING:
                this.connection.sendPong();
                this.op = UNKNOWN_OP;
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_PONG:
                this.connection.handlePong();
                this.op = UNKNOWN_OP;
                this.mode = Mode.GATHER_OP;
                break;
            case NatsConnection.OP_INFO:
                String info = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
                this.connection.handleInfo(info);
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

    private void handleProtocolOpHMsg() {
        // read headers
        // read body
        // create
        // set incoming
        // you may need to subclass or just add headers to it .. headers are more or less Map<String, List<String>>

        int protocolLength = this.msgLinePosition; //This is just after the last character
        int protocolLineLength = protocolLength + 5; // 4 for the "HMSG "

        if (this.utf8Mode) {
            protocolLineLength = protocolBuffer.remaining() + 5;

            CharBuffer buff = StandardCharsets.UTF_8.decode(protocolBuffer);
            protocolLength = buff.remaining();
            buff.get(this.msgLineChars, 0, protocolLength);
        }

        this.msgLinePosition = 0;
        String subject = grabNextMessageLineElement(protocolLength);
        String sid = grabNextMessageLineElement(protocolLength);
        String possibleReplyTo = grabNextMessageLineElement(protocolLength);
        String possibleHeaderLength = grabNextMessageLineElement(protocolLength);
        String possiblePayloadLength = null;

        if (this.msgLinePosition < protocolLength) {
            possiblePayloadLength = grabNextMessageLineElement(protocolLength);
        } else {
            possiblePayloadLength = possibleHeaderLength;
            possibleHeaderLength = possibleReplyTo;
            possibleReplyTo = null;
        }

        if (subject==null || subject.length() == 0 || sid == null || sid.length() == 0
                || possiblePayloadLength == null || possibleHeaderLength == null) {
            throw new IllegalStateException("Bad HMSG control line, missing required fields");
        }

        final String replyTo = possibleReplyTo;
        final String headerLength = possibleHeaderLength;
        final String payloadLength = possiblePayloadLength;

        int headerLen = parseLength(headerLength);
        int payloadLen = parseLength(payloadLength);

        this.incoming = new NatsMessage.IncomingBuilder()
                .sid(sid).subject(subject).replyTo(replyTo).protocolLength(protocolLineLength);
        this.mode = Mode.GATHER_HEADER;
        this.headerLen = headerLen;
        this.headerStart = this.bufferPosition;

        if (headerLen > 0) {
            this.headers = new Headers();
        }
        this.msgData = new byte[payloadLen - headerLen];
        this.msgDataPosition = 0;
        this.msgLinePosition = 0;
    }

    private void handleProtocolOpMsg() {
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

        if(subject==null || subject.length() == 0 || sid==null || sid.length() == 0 || lengthChars==null) {
            throw new IllegalStateException("Bad MSG control line, missing required fields");
        }

        int incomingLength = parseLength(lengthChars);

        this.incoming = new NatsMessage.IncomingBuilder()
                .sid(sid).subject(subject).replyTo(replyTo).protocolLength(protocolLineLength);

        this.mode = Mode.GATHER_DATA;
        this.msgData = new byte[incomingLength];
        this.msgDataPosition = 0;
        this.msgLinePosition = 0;
        return;
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

    public Mode getMode() {
        return mode;
    }

    public NatsMessage getIncoming() {
        return incoming == null ? null : incoming.build();
    }
}