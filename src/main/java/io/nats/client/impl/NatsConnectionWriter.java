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
    private final AtomicBoolean reconnectMode;

    private byte[] sendBuffer;

    private MessageQueue outgoing;
    private MessageQueue reconnectOutgoing;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.reconnectMode = new AtomicBoolean(false);
        this.stopped = new CompletableFuture<>();
        this.stopped.complete(Boolean.TRUE); // we are stopped on creation

        this.sendBuffer = new byte[connection.getOptions().getBufferSize()];

        outgoing = new MessageQueue(true);
        reconnectOutgoing = new MessageQueue(true);
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.dataPortFuture = dataPortFuture;
        this.running.set(true);
        this.stopped = new CompletableFuture<>(); // New future

        String name = (this.connection.getOptions().getConnectionName() != null) ? this.connection.getOptions().getConnectionName() : "Nats Connection";
        this.thread = new Thread(this, name + " Writer");
        this.thread.start();
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.running.set(false);
        this.outgoing.pause();
        this.reconnectOutgoing.pause();

        // Clear old ping/pong requests
        byte[] pingRequest = NatsConnection.OP_PING.getBytes(StandardCharsets.UTF_8);
        byte[] pongRequest = NatsConnection.OP_PONG.getBytes(StandardCharsets.UTF_8);
        this.outgoing.filter((msg) -> {
            return Arrays.equals(pingRequest, msg.getProtocolBytes()) || Arrays.equals(pongRequest, msg.getProtocolBytes());
        });
        return this.stopped;
    }

    public void run() {
        Duration waitForMessage = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration reconnectWait = Duration.ofMillis(1); // This can be long since no one is sending
        long maxMessages = 1000;

        try {
            DataPort dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            NatsStatistics stats = this.connection.getNatsStatistics();
            this.outgoing.resume();
            this.reconnectOutgoing.resume();

            while (this.running.get()) {
                int sendPosition = 0;
                NatsMessage msg = null;
                
                if (reconnectMode.get()) {
                    msg = this.reconnectOutgoing.accumulate(this.sendBuffer.length, maxMessages, reconnectWait);
                } else {
                    msg = this.outgoing.accumulate(this.sendBuffer.length, maxMessages, waitForMessage);
                }

                if (msg == null) { // Make sure we are still running
                    continue;
                }

                while (msg != null) {
                    long size = msg.getSizeInBytes();

                    if (sendPosition + size > sendBuffer.length) {
                        if (sendPosition == 0) { // have to resize
                            this.sendBuffer = new byte[(int)Math.max(sendBuffer.length + size, sendBuffer.length * 2)];
                        } else { // else send and go to next message
                            dataPort.write(sendBuffer, sendPosition);
                            connection.getNatsStatistics().registerWrite(sendPosition);
                            sendPosition = 0;
                            msg = msg.next;

                            if (msg == null) {
                                break;
                            }
                        }
                    }

                    byte[] bytes = msg.getProtocolBytes();
                    System.arraycopy(bytes, 0, sendBuffer, sendPosition, bytes.length);
                    sendPosition += bytes.length;

                    sendBuffer[sendPosition++] = '\r';
                    sendBuffer[sendPosition++] = '\n';

                    if (!msg.isProtocol()) {
                        bytes = msg.getData();
                        System.arraycopy(bytes, 0, sendBuffer, sendPosition, bytes.length);
                        sendPosition += bytes.length;

                        sendBuffer[sendPosition++] = '\r';
                        sendBuffer[sendPosition++] = '\n';
                    }

                    stats.incrementOutMsgs();
                    stats.incrementOutBytes(size);

                    msg = msg.next;
                }

                dataPort.write(sendBuffer, sendPosition);
                connection.getNatsStatistics().registerWrite(sendPosition);
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

    void setReconnectMode(boolean tf) {
        reconnectMode.set(tf);
    }

    boolean getReconnectMode() {
        return reconnectMode.get();
    }

    boolean canQueue(NatsMessage msg, long maxSize) {
        return (maxSize <= 0 || (outgoing.sizeInBytes() + msg.getSizeInBytes()) < maxSize);
    }

    void queue(NatsMessage msg) {
        this.outgoing.push(msg);
    }

    void queueInternalMessage(NatsMessage msg) {
        if (this.reconnectMode.get()) {
            this.reconnectOutgoing.push(msg);
        } else {
            this.outgoing.push(msg);
        }
    }
}