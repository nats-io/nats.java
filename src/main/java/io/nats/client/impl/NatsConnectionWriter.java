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

    private MessageQueue outgoing;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        this.stopped.complete(Boolean.TRUE); // we are stopped on creation

        this.sendBuffer = ByteBuffer.allocate(NatsConnection.BUFFER_SIZE);

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
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        // TODO(sasbury): Writer should clear old pings when closed
        this.running.set(false);
        this.outgoing.interrupt();
        return stopped;
    }

    // TODO(sasbury): Do we need to do more to the Handle issue with fast writer
    // that never releases the lock (look at MessageQUeue fair lock, that may be
    // enough, but is a big perf hit)
    public void run() {
        Duration waitForMessage = Duration.ofMinutes(5); // This can be long since we aren't doing anything
        Duration waitForAccumulate = null;
        long maxMessages = 1000;

        try {
            SocketChannel channel = this.channelFuture.get(); // Will wait for the future to complete
            this.outgoing.reset();

            while (this.running.get()) {
                NatsMessage msg = this.outgoing.accumulate(NatsConnection.BUFFER_SIZE, maxMessages, waitForAccumulate,
                        waitForMessage);

                if (msg == null) {
                    continue;
                }

                // TODO(sasbury): Check max sizes
                sendBuffer.clear();

                long ma = 0;
                while (msg != null) {
                    ma++;

                    sendBuffer.put(msg.getProtocolBytes());
                    sendBuffer.put(NatsConnection.CRLF);

                    if (!msg.isProtocol()) {
                        sendBuffer.put(msg.getData());
                        sendBuffer.put(NatsConnection.CRLF);
                    }
                    msg = msg.prev; // Work backward through accumulation
                }
                connection.getNatsStatistics().registerAccumulate(ma);

                // Write to the socket
                sendBuffer.flip();
                channel.write(sendBuffer);
            }
        } catch (IOException | BufferOverflowException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
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