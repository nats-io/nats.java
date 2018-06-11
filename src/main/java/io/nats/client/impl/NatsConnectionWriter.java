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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsConnectionWriter implements Runnable {
    
    private final NatsConnection connection;

    private Thread thread;
    private CompletableFuture<Boolean> stopped;
    private Future<SocketChannel> channelFuture;
    private final AtomicBoolean running;
    
    private ByteBuffer sendBuffer;
    private ByteBuffer protocolBuffer;

    private MessageQueue outgoing;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        this.stopped.complete(Boolean.TRUE); // we are stopped on creation

        this.sendBuffer = ByteBuffer.allocate(NatsConnection.BUFFER_SIZE);
        this.protocolBuffer = ByteBuffer.allocate(NatsConnection.MAX_PROTOCOL_LINE);

        outgoing = new MessageQueue();
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
        // TODO(sasbury): Writer should clear pings when closed
        this.running.set(false);
        this.outgoing.interrupt();
        return stopped;
    }

    // TODO(sasbury): Handle issue with fast writer that never releases the lock
    public void run() {
        Duration waitFor = Duration.ofMinutes(1);

        try {
            SocketChannel channel = this.channelFuture.get(); //Will wait for the future to complete
            this.outgoing.reset();
            
            while (this.running.get()) {
                NatsMessage msg = this.outgoing.pop(waitFor);

                // TODO(sasbury): Batch messages!

                if (msg == null) {
                    continue;
                }

                if (msg.isProtocol()) {
                    String protocolString = msg.getProtocolMessage();
                    protocolBuffer.clear();
                    protocolBuffer.put(protocolString.getBytes(StandardCharsets.UTF_8));
                    protocolBuffer.put(NatsConnection.CR);
                    protocolBuffer.put(NatsConnection.LF);
                    protocolBuffer.flip();
                    channel.write(protocolBuffer);
                } else { // Assumes the connection checked the subject to be non-empty
                    // TODO(sasbury): Look at the performance for this, this is simple but lots of allocs

                    byte [] body = msg.getData();

                    StringBuilder protocolString = new StringBuilder();
                    protocolString.append("PUB ");
                    protocolString.append(msg.getSubject());
                    protocolString.append(" ");

                    if (msg.getReplyTo() != null) {
                        protocolString.append(msg.getReplyTo());
                        protocolString.append(" ");
                    }

                    protocolString.append(String.valueOf(body.length));

                    // Fill the buffer and go
                    protocolBuffer.clear();
                    protocolBuffer.put(protocolString.toString().getBytes(StandardCharsets.UTF_8));
                    protocolBuffer.put(NatsConnection.CR);
                    protocolBuffer.put(NatsConnection.LF);
                    protocolBuffer.flip();

                    //Fill the main buffer
                    // TODO(sasbury): Check sizes
                    sendBuffer.clear();
                    sendBuffer.put(body);
                    sendBuffer.put(NatsConnection.CR);
                    sendBuffer.put(NatsConnection.LF);
                    sendBuffer.flip();

                    //Write to the socket
                    channel.write(protocolBuffer);
                    channel.write(sendBuffer);
                }
                
            }
        } catch (IOException|BufferOverflowException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException|ExecutionException|InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
            stopped.complete(Boolean.TRUE);
            this.thread = null;
        }
    }

    void queue(NatsMessage msg) {
        outgoing.push(msg);
    }
}