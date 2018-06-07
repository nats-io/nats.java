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
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import io.nats.client.Connection.Status;

class NatsConnectionReader implements Runnable {

    private static final int MAX_PROTOCOL_LINE = 1024;
    private static final int BUFFER_SIZE = 4 * 1024;

    private static final byte CR = 0x0D;
    private static final byte LF = 0x0A;

    private static final String OP_INFO = "info";
    private static final String OP_MSG = "msg";
    private static final String OP_PING = "ping";
    private static final String OP_PONG = "pong";
    private static final String OP_OK = "+ok";
    private static final String OP_ERR = "-err";

    private final NatsConnection connection;
    private final ReentrantLock socketLock;
    private final Condition socketCondition;
    
    private Thread thread;

    private final AtomicBoolean stopped;

    private ByteBuffer gatherer;
    private ByteBuffer protocolBuffer;
    private boolean protocolMode;
    private boolean gotCR;
    private Pattern space;

    NatsConnectionReader(NatsConnection connection) {
        this.connection = connection;
        this.socketLock = new ReentrantLock();
        this.socketCondition = this.socketLock.newCondition();
        this.stopped = new AtomicBoolean(false);

        this.gatherer = ByteBuffer.allocate(BUFFER_SIZE);
        this.protocolBuffer = ByteBuffer.allocate(MAX_PROTOCOL_LINE);
        this.protocolMode = true;
        this.space = Pattern.compile(" ");

        this.thread = new Thread(this);
        this.thread.start();
    }

    ReentrantLock getLock() {
        return this.socketLock;
    }

    Condition getCondition() {
        return this.socketCondition;
    }

    void close() {
        if (this.stopped.get()) {
            return;
        }

        this.stopped.set(true);
    }

    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        
        this.socketLock.lock(); // Always hold the lock - unless waiting on a signal
        
        try {
            while(!this.stopped.get()) {
                try {
                    try {
                        Status status = this.connection.getStatus();

                        if (status == Status.DISCONNECTED || status == Status.CLOSED) {
                            this.socketCondition.await(30, TimeUnit.SECONDS);
                            continue; // Go back to the start of the loop, on signal or timeout
                        }

                        Socket socket = this.connection.getSocket();
                        SocketChannel channel = socket.getChannel();

                        while (!this.stopped.get()) {
                            int read = channel.read(buffer);
                            if (read > 0) {

                                buffer.flip(); // Get ready to read

                                while (buffer.hasRemaining()) {
                                    read = buffer.limit() - buffer.position(); // reset due to loop
                                    if (this.protocolMode) {
                                        this.gatherProtocol(buffer, read);
                                    } else {
                                        this.gather(buffer, read);
                                    }
                                }

                                buffer.clear();
                            }
                        }
                    } catch (IOException io) {
                        this.connection.readWriteIOException(io);
                    }
                } catch (InterruptedException exp) {
                    this.stopped.set(true);
                }
            }
        } finally {
            this.socketLock.unlock();
        }
    }
    
    // Gather bytes for a protocol line
    void gatherProtocol(ByteBuffer bytes, int length) {
        // protocol buffer has max capacity
        for (int i=0; i<length; i++) {
            byte b = bytes.get();
            protocolBuffer.put(b); // Always push, but wait for the end of the line

            if (gotCR) {
                if (b == LF) {
                    parseProtocolMessage();
                    gotCR = false;
                    break;
                } else {
                    //TODO(sasbury): This is a protocol error, what should we do
                }
            } else if (b == CR) {
                gotCR = true;
            }
        }
    }

    void parseProtocolMessage() {

        // TODO(sasbury): check performance with this code, especially the split, and update if necessary
        String protocolLine = new String(protocolBuffer.array(), StandardCharsets.UTF_8);
        String msg[] = space.split(protocolLine);
        String op = msg[0].toLowerCase();

        switch (op) {

            case OP_MSG:
                break;
            case OP_OK:
                break;
            case OP_ERR:
                break;
            case OP_PING:
                break;
            case OP_PONG:
                break;
            case OP_INFO:
                // Recreate the original string and parse it
                handleInfo(String.join(" ", Arrays.copyOfRange(msg, 1, msg.length)));
                break;
            default:
                // BAD OP

        }

        // put the reader in non-protocol mode when waiting for the message body, if we get that type of protocol message
        // handle err and ok messages
    }

    // Gather bytes for a message body
    void gather(ByteBuffer bytes, int length) {
        // gatherer should be set up to the right capacity and limit
    }

    void handleInfo(String infoJson) {
        NatsServerInfo serverInfo = new NatsServerInfo(infoJson);
        this.connection.setServerInfo(serverInfo);
    }
}