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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsConnectionWriter implements Runnable {

    private final NatsConnection connection;

    private Thread thread;
    private CompletableFuture<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
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
        this.outgoing.interrupt();

        // Clear old ping requests
        byte[] pingRequest = NatsConnection.OP_PING.getBytes(StandardCharsets.UTF_8);
        this.outgoing.filter((msg) -> {
            return Arrays.equals(pingRequest, msg.getProtocolBytes());
        });
        return this.stopped;
    }

    public void run() {
        Duration waitForMessage = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration waitForAccumulate = null;
        long maxMessages = 1000;

        try {
            DataPort dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            this.outgoing.reset();

            while (this.running.get()) {
                NatsMessage msg = this.outgoing.accumulate(this.sendBuffer.capacity(), maxMessages, waitForAccumulate,
                        waitForMessage);

                if (msg == null) { // Make sure we are still running
                    continue;
                }

                sendBuffer.clear();

                long accumulated = 0;
                while (msg != null) {
                    accumulated++;

                    sendBuffer.put(msg.getProtocolBytes());
                    sendBuffer.put(NatsConnection.CRLF);

                    if (!msg.isProtocol()) {
                        sendBuffer.put(msg.getData());
                        sendBuffer.put(NatsConnection.CRLF);
                    }
                    msg = msg.prev; // Work backward through accumulation
                }
                connection.getNatsStatistics().registerAccumulate(accumulated);

                // Write to the socket
                sendBuffer.flip();

                while (sendBuffer.hasRemaining()) {
                    dataPort.write(sendBuffer);
                }
            }
        } catch (IOException | BufferOverflowException io) {
            this.connection.handleCommunicationIssue(io);
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
            // Exit
        } finally {
            this.running.set(false);
            this.stopped.complete(Boolean.TRUE);
            this.thread = null;
        }
    }

    boolean canQueue(NatsMessage msg, long maxSize) {
        return (maxSize <= 0 || (outgoing.sizeInBytes() + msg.getSize()) < maxSize);
    }

    void queue(NatsMessage msg) {
        outgoing.push(msg);
    }
}