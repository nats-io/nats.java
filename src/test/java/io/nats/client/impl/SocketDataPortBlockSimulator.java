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
import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("ClassEscapesDefinedScope") // NatsConnection
public class SocketDataPortBlockSimulator extends SocketDataPort {

    private long writeTimeoutNanos;
    private long delayPeriodMillis;
    private Timer writeWatcherTimer;
    private WriteWatcherTask writeWatcherTask;
    private volatile long writeMustBeDoneBy = Long.MAX_VALUE;

    class WriteWatcherTask extends TimerTask {
        @Override
        public void run() {
            //  if now is after when it was supposed to be done by
            if (System.nanoTime() > writeMustBeDoneBy) {
                writeWatcherTimer.cancel(); // we don't need to repeat this
                connection.executeCallback((c, el) -> el.socketWriteTimeout(c));
                try {
                    out.close();
                }
                catch (IOException ignore) {}
                blocking.set(0);
                SIMULATE_SOCKET_BLOCK.set(0);
                try {
                    connection.forceReconnect();
                }
                catch (InterruptedException | IOException ignore) {}
            }
        }
    }

    @Override
    public void afterConstruct(Options options) {
        long writeTimeoutMillis;
        if (options.getSocketWriteTimeout() == null) {
            writeTimeoutMillis = Options.DEFAULT_SOCKET_WRITE_TIMEOUT.toMillis();
        }
        else {
            writeTimeoutMillis = options.getSocketWriteTimeout().toMillis();
        }
        delayPeriodMillis = writeTimeoutMillis * 51 / 100;
        writeTimeoutNanos = writeTimeoutMillis * 1_000_000;
    }

    @Override
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
        super.connect(conn, nuri, timeoutNanos);
        writeWatcherTimer = new Timer();
        writeWatcherTask = new WriteWatcherTask();
        writeWatcherTimer.schedule(writeWatcherTask, delayPeriodMillis, delayPeriodMillis);
    }

    public static AtomicLong SIMULATE_SOCKET_BLOCK = new AtomicLong();
    AtomicLong blocking = new AtomicLong();
    public void write(byte[] src, int toWrite) throws IOException {
        try {
            writeMustBeDoneBy = System.nanoTime() + writeTimeoutNanos;
            blocking.set(SIMULATE_SOCKET_BLOCK.get());
            while (blocking.get() > 0) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(100);
                    blocking.addAndGet(-100);
                }
                catch (InterruptedException ignore) {
                    return;
                }
            }
            out.write(src, 0, toWrite);
        }
        finally {
            writeMustBeDoneBy = Long.MAX_VALUE;
        }
    }

    public void close() throws IOException {
        try {
            writeWatcherTask.cancel();
        }
        catch (Exception ignore) {
            // don't want this to be passed along
        }
        try {
            writeWatcherTimer.cancel();
        }
        catch (Exception ignore) {
            // don't want this to be passed along
        }
        super.close();
    }
}
