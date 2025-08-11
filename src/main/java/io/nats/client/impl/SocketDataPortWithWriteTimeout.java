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

import io.nats.client.ForceReconnectOptions;
import io.nats.client.NatsSystemClock;
import io.nats.client.Options;
import io.nats.client.support.NatsUri;
import io.nats.client.support.ScheduledTask;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is not thread-safe.  Caller must ensure thread safety.
 */
@SuppressWarnings("ClassEscapesDefinedScope") // NatsConnection
public class SocketDataPortWithWriteTimeout extends SocketDataPort {

    private long writeTimeoutNanos;
    private long delayPeriodMillis;
    private ScheduledTask writeWatchTask;
    private final AtomicLong writeMustBeDoneBy;

    public SocketDataPortWithWriteTimeout() {
        writeMustBeDoneBy = new AtomicLong(Long.MAX_VALUE);
    }

    @Override
    public void afterConstruct(Options options) {
        super.afterConstruct(options);
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
        writeWatchTask = new ScheduledTask(conn.getScheduledExecutor(), delayPeriodMillis,
            () -> {
                //  if now is after when it was supposed to be done by
                if (NatsSystemClock.nanoTime() > writeMustBeDoneBy.get()) {
                    writeWatchTask.shutdown(); // we don't need to repeat this, the connection is going to be closed
                    connection.executeCallback((c, el) -> el.socketWriteTimeout(c));
                    try {
                        connection.forceReconnect(ForceReconnectOptions.FORCE_CLOSE_INSTANCE);
                    }
                    catch (IOException e) {
                        // retry maybe?
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // This task is going to re-run anyway, so no point in throwing
                    }
                }
            });
    }

    public void write(byte[] src, int toWrite) throws IOException {
        writeMustBeDoneBy.set(NatsSystemClock.nanoTime() + writeTimeoutNanos);
        out.write(src, 0, toWrite);
        writeMustBeDoneBy.set(Long.MAX_VALUE);
    }

    public void close() throws IOException {
        writeWatchTask.shutdown();
        super.close();
    }
}
