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

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class AuthAndConnectTests extends TestBase {
    @Test
    public void testIsAuthError() {
        //noinspection resource
        NatsConnection nats = new NatsConnection(options());
        assertTrue(nats.isAuthenticationError("user authentication expired"));
        assertTrue(nats.isAuthenticationError("authorization violation"));
        assertTrue(nats.isAuthenticationError("Authorization Violation"));
        assertFalse(nats.isAuthenticationError("test"));
        assertFalse(nats.isAuthenticationError(""));
        assertFalse(nats.isAuthenticationError(null));
    }

    @Test()
    public void testConnectWhenClosed() throws Exception {
        runInSharedOwnNc(c -> {
            NatsConnection nc = (NatsConnection)c;
            closeAndConfirm(nc);
            nc.connect(false); // should do nothing
            assertClosed(nc);
            nc.reconnect(); // should do nothing
            assertClosed(nc);
        });
    }

    /**
     * Simulates an issue where {@link NatsConnection#closeSocket} is simultaneously called from multiple threads,
     * and the {@link NatsConnection#reconnect} closes the connection due to overwriting the disconnecting state.
     */
    @RepeatedTest(5)
    public void testNoCloseOnSimultaneouslyClosingSocket() throws Exception {
        // Use a custom error listener that doesn't log errors, since we'll spam exception messages otherwise.
        ErrorListener noopErrorListener = new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {
                // noop
            }
        };

        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = Options.builder()
                    .server(ts.getServerUri())
                    .maxReconnects(-1)
                    .reconnectWait(Duration.ZERO)
                    .errorListener(noopErrorListener)
                    .build();

            try (NatsConnection nc = (NatsConnection) managedConnect(options)) {

                // After we've connected, shut down, so we can attempt reconnecting.
                ts.shutdown(true);

                final AtomicBoolean running = new AtomicBoolean(true);
                Thread parallelCommunicationIssues = new Thread(() -> {
                    while (running.get()) {
                        nc.handleCommunicationIssue(new Exception());

                        // Shortly sleep, to not spam at full speed.
                        sleep(1);
                    }
                });
                parallelCommunicationIssues.start();

                // Wait for some time to allow for reconnection logic to run.
                Thread.sleep(2000);
                running.set(false);

                assertNotEquals(Connection.Status.CLOSED, nc.getStatus());
            }
        }
    }
}
