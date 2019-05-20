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
import java.util.concurrent.locks.ReentrantLock;

import io.nats.client.Options;

class NatsConnectionWriter implements Runnable {

    private final NatsConnection connection;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private final AtomicBoolean running;
    private final AtomicBoolean reconnectMode;
    private final ReentrantLock startStopLock;

    private byte[] sendBuffer;

    private MessageQueue outgoing;
    private MessageQueue reconnectOutgoing;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.reconnectMode = new AtomicBoolean(false);
        this.startStopLock = new ReentrantLock();
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        int bufSize = connection.getOptions().getBufferSize();
        this.sendBuffer = new byte[bufSize];
        
        outgoing = new MessageQueue(true, Options.MAX_MESSAGES_IN_OUTGOING_QUEUE);

        // The reconnect buffer contains internal messages, and we will keep it unlimited in size
        reconnectOutgoing = new MessageQueue(true, 0);
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.startStopLock.lock();
        try {
            this.dataPortFuture = dataPortFuture;
            this.running.set(true);
            this.outgoing.resume();
            this.reconnectOutgoing.resume();
            this.stopped = connection.getExecutor().submit(this, Boolean.TRUE);
        } finally {
            this.startStopLock.unlock();
        }
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.startStopLock.lock();
        try {
            this.running.set(false);
            this.outgoing.pause();
            this.reconnectOutgoing.pause();
    
            // Clear old ping/pong requests
            byte[] pingRequest = NatsConnection.OP_PING.getBytes(StandardCharsets.UTF_8);
            byte[] pongRequest = NatsConnection.OP_PONG.getBytes(StandardCharsets.UTF_8);
            this.outgoing.filter((msg) -> {
                return Arrays.equals(pingRequest, msg.getProtocolBytes()) || Arrays.equals(pongRequest, msg.getProtocolBytes());
            });
        } finally {
            this.startStopLock.unlock();
        }
        return this.stopped;
    }

    @Override
    public void run() {
        Duration waitForMessage = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration reconnectWait = Duration.ofMillis(1); // This should be short, since we are trying to get the reconnect through

        try {
            DataPort dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            NatsStatistics stats = this.connection.getNatsStatistics();
            int maxAccumulate = Options.MAX_MESSAGES_IN_NETWORK_BUFFER;

            while (this.running.get()) {
                int sendPosition = 0;
                NatsMessage msg = null;
                
                if (this.reconnectMode.get()) {
                    msg = this.reconnectOutgoing.accumulate(this.sendBuffer.length, maxAccumulate, reconnectWait);
                } else {
                    msg = this.outgoing.accumulate(this.sendBuffer.length, maxAccumulate, waitForMessage);
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
        }
    }

    void setReconnectMode(boolean tf) {
        reconnectMode.set(tf);
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