// Copyright 2026 The NATS Authors
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

import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A write-timeout data port whose first connect waits for the test to let it proceed, through the attempt's deadline
 * and the interrupt of the abandoned attempt, the way a socket connect that cannot be interrupted would. Once let go
 * it connects and records what came of it; the connection has abandoned that attempt by then. Later connects go
 * straight through, so the connection can reach a server with its next attempt.
 */
public class AbandonedConnectDataPort extends SocketDataPortWithWriteTimeout {
    public static volatile CountDownLatch proceed = new CountDownLatch(1);
    public static volatile CompletableFuture<String> outcome = new CompletableFuture<>();
    private static final AtomicBoolean waitOnce = new AtomicBoolean(true);

    public static void reset() {
        proceed = new CountDownLatch(1);
        outcome = new CompletableFuture<>();
        waitOnce.set(true);
    }

    @Override
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
        if (!waitOnce.compareAndSet(true, false)) {
            super.connect(conn, nuri, timeoutNanos);
            return;
        }
        CountDownLatch latch = proceed;
        boolean released = false;
        while (!released) {
            try {
                released = latch.await(20, TimeUnit.SECONDS);
                if (!released) {
                    throw new IOException("the test did not let the connect proceed");
                }
            }
            catch (InterruptedException e) {
                // the abandoned attempt interrupts its connect; this one goes on, as a socket connect would
            }
        }
        try {
            super.connect(conn, nuri, timeoutNanos);
            outcome.complete("connected, socket " + socketState());
        }
        catch (IOException | RuntimeException e) {
            outcome.complete(e.getMessage() + ", socket " + socketState());
            throw e;
        }
    }

    private String socketState() {
        return socket == null ? "null" : socket.isClosed() ? "closed" : "open";
    }
}
