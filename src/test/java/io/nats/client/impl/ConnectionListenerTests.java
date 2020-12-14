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

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.nats.client.impl.TestMacros.assertConnected;
import static io.nats.client.impl.TestMacros.closeConnectionAssertClosed;
import static org.junit.jupiter.api.Assertions.*;

public class ConnectionListenerTests {

    @Test
    public void testToString() {
        assertEquals(ConnectionListener.Events.CLOSED.toString(), "nats: connection closed");
    }
    
    @Test
    public void testCloseCount() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            TestHandler handler = new TestHandler();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionListener(handler).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
                assertEquals(ts.getURI(), nc.getConnectedUrl());
            } finally {
                closeConnectionAssertClosed(nc);
                assertNull(nc.getConnectedUrl());
            }
            assertEquals(1, handler.getEventCount(Events.CLOSED));
        }
    }

    @Test
    public void testDiscoveredServersCountAndListenerInOptions() throws Exception {

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\",\"connect_urls\": [\""+ts.getURI()+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                TestHandler handler = new TestHandler();
                Options options = new Options.Builder().
                                    server(ts2.getURI()).
                                    maxReconnects(0).
                                    connectionListener(handler).
                                    build();
                                    
                handler.prepForStatusChange(Events.CONNECTED);
                Connection nc = Nats.connect(options);
                try {
                    handler.waitForStatusChange(5, TimeUnit.SECONDS);
                    assertConnected(nc);
                } finally {
                    closeConnectionAssertClosed(nc);
                    assertEquals(1, handler.getEventCount(Events.DISCOVERED_SERVERS));
                }
            }
        }
    }

    @Test
    public void testDisconnectReconnectCount() throws Exception {
        Connection nc = null;
        TestHandler handler = new TestHandler();
        try {
            int port;
            try (NatsTestServer ts = new NatsTestServer(false)) {
                Options options = new Options.Builder().
                        server(ts.getURI()).
                        reconnectWait(Duration.ofMillis(100)).
                        maxReconnects(-1).
                        connectionListener(handler).
                        build();
                port = ts.getPort();
                nc = Nats.connect(options);
                assertConnected(nc);
                assertEquals(ts.getURI(), nc.getConnectedUrl());
                handler.prepForStatusChange(Events.DISCONNECTED);
            }

            try { nc.flush(Duration.ofMillis(250)); } catch (Exception exp) { /* ignored */ }

            handler.waitForStatusChange(1000, TimeUnit.MILLISECONDS);
            assertTrue(handler.getEventCount(Events.DISCONNECTED) >= 1);
            assertNull(nc.getConnectedUrl());

            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                try { Thread.sleep(1000); } catch (InterruptedException e) { /* ignored */ }
                assertConnected(nc);
                assertEquals(1, handler.getEventCount(Events.RECONNECTED));
                assertEquals(ts.getURI(), nc.getConnectedUrl());
            }
        } finally {
            assertNotNull(nc);
            closeConnectionAssertClosed(nc);
            assertNull(nc.getConnectedUrl());
        }
    }

    @Test
    public void testExceptionInConnectionListener() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            BadHandler handler = new BadHandler();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionListener(handler).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeConnectionAssertClosed(nc);
            }
            assertTrue(((NatsConnection)nc).getNatsStatistics().getExceptions() > 0);
        }
    }
}