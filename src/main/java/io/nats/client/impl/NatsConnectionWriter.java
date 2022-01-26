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
import io.nats.client.support.ByteArrayBuilder;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.support.NatsConstants.*;

class NatsConnectionWriter implements Runnable {

    private final NatsConnection connection;

    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private DataPort dataPort = null;
    private final AtomicBoolean running;
    private final AtomicBoolean reconnectMode;
    private final ReentrantLock startStopLock;

    private final int maxWriteSize;
    private final int maxWriteCount;

    private final MessageQueue outgoing;
    private final MessageQueue reconnectOutgoing;
    private final long reconnectBufferSize;

    NatsConnectionWriter(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.reconnectMode = new AtomicBoolean(false);
        this.startStopLock = new ReentrantLock();
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        Options options = connection.getOptions();
        maxWriteSize = options.getMaxWriteSize();
        maxWriteCount = options.getMaxWriteCount();

        outgoing = new MessageQueue(true,
                options.getMaxMessagesInOutgoingQueue(),
                options.isDiscardMessagesWhenOutgoingQueueFull());

        // The reconnect buffer contains internal messages, and we will keep it unlimited in size
        reconnectOutgoing = new MessageQueue(true, 0);
        reconnectBufferSize = options.getReconnectBufferSize();
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
        this.running.set(false);
        this.startStopLock.lock();
        try {
            this.outgoing.pause();
            this.reconnectOutgoing.pause();
            // Clear old ping/pong requests
            this.outgoing.filter((msg) -> {
                byte[] bytes = msg.getProtocol().toByteArray();
                return Arrays.equals(OP_PING_BYTES, bytes)
                    || Arrays.equals(OP_PONG_BYTES, bytes);
            });
        } finally {
            this.startStopLock.unlock();
        }

        return this.stopped;
    }

    void directAccumulate(NatsMessage msg, NatsStatistics stats) throws IOException {
        int size = 0;

        ByteArrayBuilder bab = msg.getProtocol();
        int len = bab.length();
        size += len + CRLF_BYTES_LEN;
        dataPort.write(bab.internalArray(), len);
        dataPort.write(CRLF_BYTES, CRLF_BYTES_LEN);

        if (!msg.isProtocol()) {
            bab = msg.getSerializedHeaderBab();
            if (bab != null) {
                len = bab.length();
                if (len > 0) {
                    size += len;
                    dataPort.write(bab.internalArray(), len);
                }
            }

            byte[] bytes = msg.getData(); // guaranteed to not be null
            if (bytes.length > 0) {
                size += bytes.length;
                dataPort.write(bytes, bytes.length);
            }

            size += CRLF_BYTES_LEN;
            dataPort.write(CRLF_BYTES, CRLF_BYTES_LEN);
        }

        stats.incrementOutMsgs();
        stats.incrementOutBytes(size);
        connection.getNatsStatistics().registerWrite(size);
    }

    @Override
    public void run() {
        Duration waitForMessage = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration reconnectWait = Duration.ofMillis(1); // This should be short, since we are trying to get the reconnect through

        try {
            dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            final NatsStatistics stats = this.connection.getNatsStatistics();

            MessageQueue.DirectAccumulator accumulator = msg -> directAccumulate(msg, stats);

            while (this.running.get()) {
                if (this.reconnectMode.get()) {
                    this.reconnectOutgoing.accumulateDirect(maxWriteSize, maxWriteCount, reconnectWait, accumulator);
                } else {
                    this.outgoing.accumulateDirect(maxWriteSize, maxWriteCount, waitForMessage, accumulator);
                }
                dataPort.flush();
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

    boolean canQueueDuringReconnect(NatsMessage msg) {
        // don't over fill the send buffer while waiting to reconnect
        return (reconnectBufferSize < 0 || (outgoing.sizeInBytes() + msg.getSizeInBytes()) < reconnectBufferSize);
    }

    boolean queue(NatsMessage msg) {
        return this.outgoing.push(msg);
    }

    void queueInternalMessage(NatsMessage msg) {
        if (this.reconnectMode.get()) {
            this.reconnectOutgoing.push(msg);
        } else {
            this.outgoing.push(msg, true);
        }
    }

    synchronized void flushBuffer() {
        // Since there is no connection level locking, we rely on synchronization
        // of the APIs here.
        try  {
            if (this.running.get()) {
                dataPort.flush();
            }
        } catch (Exception e) {
            // NOOP;
        }
    }
}
