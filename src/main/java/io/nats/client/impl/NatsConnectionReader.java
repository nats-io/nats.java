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
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

class NatsConnectionReader implements Runnable {
    private final NatsConnection connection;

    private ByteBuffer gatherer;
    private ByteBuffer protocolBuffer;
    private boolean protocolMode;
    private boolean gotCR;
    private Pattern space;

    private Thread thread;
    private CompletableFuture<Boolean> stopped;
    private Future<SocketChannel> channelFuture;
    private final AtomicBoolean running;

    NatsConnectionReader(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        this.stopped.complete(Boolean.TRUE); // we are stopped on creation

        this.gatherer = ByteBuffer.allocate(NatsConnection.BUFFER_SIZE);
        this.protocolBuffer = ByteBuffer.allocate(NatsConnection.MAX_PROTOCOL_LINE);
        this.protocolMode = true;
        this.space = Pattern.compile(" ");
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<SocketChannel> channelFuture) {
        this.channelFuture = channelFuture;
        this.running.set(true);
        this.stopped = new CompletableFuture<>(); // New future
        this.thread = new Thread(this);
        this.thread.start();
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this method does.
    Future<Boolean> stop() {
        this.running.set(false);
        return stopped;
    }

    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(NatsConnection.BUFFER_SIZE);
        try {
            SocketChannel channel = this.channelFuture.get(); //Will wait for the future to complete
            this.protocolMode = true;

            while (this.running.get()) {
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
                } else if (read < 0) {
                    throw new IOException("Read channel closed.");
                }
            }
        } catch (IOException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException|ExecutionException|InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
            // Clear the buffers, since they are only used inside this try/catch
            // We will reuse later
            this.gatherer.clear();
            this.protocolBuffer.clear();
            stopped.complete(Boolean.TRUE);
            this.thread = null;
        }
    }
    
    // Gather bytes for a protocol line
    void gatherProtocol(ByteBuffer bytes, int length) {
        // protocol buffer has max capacity, shouldn't need resizing
        for (int i=0; i<length; i++) {
            byte b = bytes.get();
            protocolBuffer.put(b); // Always push, but wait for the end of the line

            if (gotCR) {
                if (b == NatsConnection.LF) {
                    protocolBuffer.flip();
                    parseProtocolMessage();
                    protocolBuffer.clear();
                    gotCR = false;
                    break;
                } else {
                    //TODO(sasbury): This is a protocol error, what should we do - force reconnect
                }
            } else if (b == NatsConnection.CR) {
                gotCR = true;
            }
        }
    }

    void parseProtocolMessage() {
        // TODO(sasbury): check performance with this code, and update if necessary

        String protocolLine =  StandardCharsets.UTF_8.decode(protocolBuffer).toString();
        
        protocolLine = protocolLine.trim();

        String msg[] = space.split(protocolLine);
        String op = msg[0].toUpperCase();

        switch (op) {
            case NatsConnection.OP_MSG:
                break;
            case NatsConnection.OP_OK:
                break;
            case NatsConnection.OP_ERR:
                break;
            case NatsConnection.OP_PING:
                break;
            case NatsConnection.OP_PONG:
                this.connection.handlePong();
                break;
            case NatsConnection.OP_INFO:
                // Recreate the original string and parse it
                this.connection.handleInfo(String.join(" ", Arrays.copyOfRange(msg, 1, msg.length)));
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
        // limited by the info's max payload size
    }
}