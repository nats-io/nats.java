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

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.TestBase.standardCloseConnection;
import static io.nats.client.utils.TestBase.standardConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The status and the current server are two separate fields and are not published together, so
 * the order they are updated in decides whether an application sampling the connection can ever
 * see a combination that was never true.
 * <p>
 * closeSocket used to tear the socket down first and update the status afterwards. Since
 * closeSocketImpl clears the current server as its first act and then waits on the reader and
 * writer stop futures, that left a window - bounded by those stop timeouts, so up to seconds
 * under a slow reader - in which getStatus() returned CONNECTED while getConnectedUrl() returned
 * null. Any health check, metric or log line that sampled the connection during a communication
 * failure teardown could see it, with no callback delay involved.
 */
@Isolated
public class ConnectionStateConsistencyTests {

    @Test
    public void testStatusAndConnectedUrlAreNeverInconsistentDuringTeardown() throws Exception {
        int port = NatsTestServer.nextPort();
        NatsTestServer ts = new NatsTestServer(port, false);
        NatsConnection nc = null;
        AtomicBoolean run = new AtomicBoolean(true);
        Thread poller = null;

        AtomicLong samples = new AtomicLong();
        AtomicLong inconsistent = new AtomicLong();
        AtomicReference<String> firstExample = new AtomicReference<>();

        try {
            Options options = new Options.Builder()
                .server(ts.getURI())
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(50))
                .connectionTimeout(Duration.ofMillis(500))
                .errorListener(new ErrorListener() {})
                .build();

            nc = (NatsConnection)standardConnection(options);
            assertNotNull(nc.getConnectedUrl());

            final Connection conn = nc;
            poller = new Thread(() -> {
                while (run.get()) {
                    samples.incrementAndGet();
                    Connection.Status status = conn.getStatus();
                    String url = conn.getConnectedUrl();
                    if (status == Connection.Status.CONNECTED && url == null) {
                        inconsistent.incrementAndGet();
                        firstExample.compareAndSet(null, "status=" + status + " getConnectedUrl()=null");
                    }
                }
            }, "state-pair-poller");
            poller.setDaemon(true);
            poller.start();

            // Take the server away. The reader raises a communication issue, which is the path
            // that reaches closeSocket. forceReconnect would exercise forceReconnectImpl instead,
            // which already updated the status before tearing down.
            ts.close();

            long end = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < end && nc.getStatus() == Connection.Status.CONNECTED) {
                //noinspection BusyWait
                Thread.sleep(5);
            }
            Thread.sleep(1000);

            run.set(false);
            poller.join(5_000);

            assertTrue(samples.get() > 1_000_000,
                "poller took " + samples.get() + " samples, too few to conclude anything");
            assertEquals(0, inconsistent.get(),
                "observed CONNECTED with a null connected url " + inconsistent.get()
                    + " times in " + samples.get() + " samples, first: " + firstExample.get());
        }
        finally {
            run.set(false);
            if (poller != null) {
                poller.join(5_000);
            }
            ts.close();
            if (nc != null) {
                standardCloseConnection(nc);
            }
        }
    }
}
