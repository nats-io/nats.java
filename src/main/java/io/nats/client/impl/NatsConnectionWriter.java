
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
import io.nats.client.StatisticsCollector;
import io.nats.client.support.ByteArrayBuilder;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.impl.MarkerMessage.END_RECONNECT;
import static io.nats.client.support.BuilderBase.bufferAllocSize;
import static io.nats.client.support.NatsConstants.CR;
import static io.nats.client.support.NatsConstants.LF;

class NatsConnectionWriter implements Runnable {
    enum Mode {
        Normal, Reconnect, WaitingForEndReconnect
    }
    private static final int BUFFER_BLOCK_SIZE = 256;

    private final NatsConnection connection;

    private final ReentrantLock writerLock;
    private Future<Boolean> stopped;
    private Future<DataPort> dataPortFuture;
    private DataPort dataPort;
    private final AtomicBoolean running;
    private final AtomicReference<Mode> mode;
    private final ReentrantLock startStopLock;

    private byte[] sendBuffer;
    private final AtomicInteger sendBufferLength;

    private final WriterMessageQueue normalOutgoing;
    private final WriterMessageQueue reconnectOutgoing;
    private final long reconnectBufferSize;

    NatsConnectionWriter(NatsConnection connection, NatsConnectionWriter sourceWriter) {
        this.connection = connection;
        writerLock = new ReentrantLock();

        this.running = new AtomicBoolean(false);
        if (sourceWriter == null) {
            mode = new AtomicReference<>(Mode.Normal);
        }
        else {
            mode = new AtomicReference<>(Mode.Reconnect);
        }
        this.startStopLock = new ReentrantLock();
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        Options options = connection.getOptions();
        int sbl = bufferAllocSize(options.getBufferSize(), BUFFER_BLOCK_SIZE);
        sendBufferLength = new AtomicInteger(sbl);
        sendBuffer = new byte[sbl];
        reconnectBufferSize = options.getReconnectBufferSize();

        if (sourceWriter == null) {
            normalOutgoing = new WriterMessageQueue(
                options.getMaxMessagesInOutgoingQueue(),
                options.isDiscardMessagesWhenOutgoingQueueFull(),
                options.getWriteQueuePushTimeout()
            );

            // The "reconnect" buffer contains internal messages
            // Using this constructor makes it unbounded and without "discard when full"
            reconnectOutgoing = new WriterMessageQueue(options.getWriteQueuePushTimeout());
        }
        else {
            // if we are creating this from another writer,
            // it's after reconnect... just take their queues
            normalOutgoing = sourceWriter.normalOutgoing;
            normalOutgoing.resume();
            reconnectOutgoing = sourceWriter.reconnectOutgoing;
            reconnectOutgoing.resume();
        }

    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.startStopLock.lock();
        try {
            this.dataPortFuture = dataPortFuture;
            this.running.set(true);
            this.normalOutgoing.resume();
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
        if (running.get()) {
            running.set(false);
            startStopLock.lock();
            try {
                this.normalOutgoing.pause();
                this.reconnectOutgoing.pause();
                this.normalOutgoing.filter();
            }
            finally {
                this.startStopLock.unlock();
            }
        }
        return this.stopped;
    }

    boolean isRunning() {
        return running.get();
    }

    void sendMessageBatch(NatsMessage msg, DataPort dataPort, StatisticsCollector stats) throws IOException {
        writerLock.lock();
        try {
            int sendPosition = 0;
            int sbl = sendBufferLength.get();

            while (msg != null) {
                if (msg == END_RECONNECT) {
                    mode.set(Mode.Normal);
                    break;
                }
                long size = msg.getSizeInBytes();

                if (sendPosition + size > sbl) {
                    if (sendPosition > 0) {
                        dataPort.write(sendBuffer, sendPosition);
                        stats.registerWrite(sendPosition);
                        sendPosition = 0;
                    }
                    if (size > sbl) { // have to resize b/c can't fit 1 message
                        sbl = bufferAllocSize((int) size, BUFFER_BLOCK_SIZE);
                        sendBufferLength.set(sbl);
                        sendBuffer = new byte[sbl];
                    }
                }

                ByteArrayBuilder bab = msg.getProtocolBab();
                int babLen = bab.length();
                System.arraycopy(bab.internalArray(), 0, sendBuffer, sendPosition, babLen);
                sendPosition += babLen;

                sendBuffer[sendPosition++] = CR;
                sendBuffer[sendPosition++] = LF;

                if (!msg.isProtocol()) { // because a protocol message does not have headers or data
                    sendPosition += msg.copyNotEmptyHeaders(sendPosition, sendBuffer);

                    byte[] bytes = msg.getData(); // guaranteed to not be null
                    if (bytes.length > 0) {
                        System.arraycopy(bytes, 0, sendBuffer, sendPosition, bytes.length);
                        sendPosition += bytes.length;
                    }

                    sendBuffer[sendPosition++] = CR;
                    sendBuffer[sendPosition++] = LF;
                }

                stats.incrementOut(size);

                if (msg.flushImmediatelyAfterPublish) {
                    dataPort.flush();
                }
                msg = msg.next;
            }

            // no need to write if there are no bytes
            if (sendPosition > 0) {
                dataPort.write(sendBuffer, sendPosition);
                stats.registerWrite(sendPosition);
            }
        }
        finally {
            writerLock.unlock();
        }
    }

    @Override
    public void run() {
        Duration outgoingTimeout = Duration.ofMinutes(2); // This can be long since no one is sending
        Duration reconnectTimeout = Duration.ofMillis(1); // This should be short, since we are trying to get the reconnect through

        try {
            dataPort = this.dataPortFuture.get(); // Will wait for the future to complete
            StatisticsCollector stats = this.connection.getStatisticsCollector();

            while (running.get() && !Thread.interrupted()) {
                NatsMessage msg;
                if (mode.get() == Mode.Normal) {
                    msg = this.normalOutgoing.accumulate(sendBufferLength.get(), Options.MAX_MESSAGES_IN_NETWORK_BUFFER, outgoingTimeout);
                }
                else {
                    msg = this.reconnectOutgoing.accumulate(sendBufferLength.get(), Options.MAX_MESSAGES_IN_NETWORK_BUFFER, reconnectTimeout);
                }
                if (msg != null) {
                    sendMessageBatch(msg, dataPort, stats);
                }
            }
        } catch (IOException | BufferOverflowException io) {
            // if already not running, an IOE is not unreasonable in a transition state
            if (running.get()) {
                this.connection.handleCommunicationIssue(io);
            }
        } catch (CancellationException | ExecutionException ex) {
            // Exit
        } catch (InterruptedException ex) {
            // Exit
            Thread.currentThread().interrupt();
        } finally {
            this.running.set(false);
        }
    }

    void enterReconnectMode() {
        reconnectOutgoing.clear();
        mode.set(Mode.Reconnect);
    }

    void enterWaitingForEndReconnectMode() {
        reconnectOutgoing.queueMarkerMessage(END_RECONNECT);
        mode.set(Mode.WaitingForEndReconnect);
    }

    boolean canQueueDuringReconnect(NatsMessage msg) {
        // don't over fill the "send" buffer while waiting to reconnect
        return (reconnectBufferSize < 0 || (normalOutgoing.sizeInBytes() + msg.getSizeInBytes()) < reconnectBufferSize);
    }

    boolean queue(NatsMessage msg) {
        return this.normalOutgoing.push(msg);
    }

    void queueInternalMessage(NatsMessage msg) {
        if (mode.get() == Mode.Reconnect) {
            reconnectOutgoing.push(msg);
        }
        else {
            normalOutgoing.push(msg, true);
        }
    }

    void flushBuffer() {
        // Since there is no connection level locking, we rely on synchronization
        // of the APIs here.
        writerLock.lock();
        try {
            if (this.running.get()) {
                dataPort.flush();
            }
        } catch (Exception e) {
            // NOOP;
        }
        finally {
            writerLock.unlock();
        }
    }

    long outgoingPendingMessageCount() {
        writerLock.lock();
        try {
            return normalOutgoing == null ? -1 : normalOutgoing.length();
        }
        finally {
            writerLock.unlock();
        }
    }

    long outgoingPendingBytes() {
        writerLock.lock();
        try {
            return normalOutgoing == null ? -1 : normalOutgoing.sizeInBytes();
        }
        finally {
            writerLock.unlock();
        }
    }
}
