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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class ConnectionListenerTests {

    @Test
    public void testToString() {
        assertEquals("nats: connection closed", Events.CLOSED.toString());
    }
    
    @Test
    public void testCloseCount() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            ListenerForTesting listener = new ListenerForTesting();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionListener(listener).
                                build();
            Connection nc = standardConnection(options);
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            standardCloseConnection(nc);
            assertNull(nc.getConnectedUrl());
            assertEquals(1, listener.getEventCount(Events.CLOSED));
        }
    }

    @Test
    public void testDiscoveredServersCountAndListenerInOptions() throws Exception {

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\",\"connect_urls\": [\""+ts.getURI()+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                ListenerForTesting listener = new ListenerForTesting();
                Options options = new Options.Builder().
                                    server(ts2.getURI()).
                                    maxReconnects(0).
                                    connectionListener(listener).
                                    build();
                                    
                listener.prepForStatusChange(Events.CONNECTED);
                standardCloseConnection( listenerConnectionWait(options, listener) );
                assertEquals(1, listener.getEventCount(Events.DISCOVERED_SERVERS));
            }
        }
    }

    @Test
    public void testDisconnectReconnectCount() throws Exception {
        int port;
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    reconnectWait(Duration.ofMillis(100)).
                    maxReconnects(-1).
                    connectionListener(listener).
                    build();
            port = ts.getPort();
            nc = standardConnection(options);
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        try { nc.flush(Duration.ofMillis(250)); } catch (Exception exp) { /* ignored */ }

        listener.waitForStatusChange(1000, TimeUnit.MILLISECONDS);
        assertTrue(listener.getEventCount(Events.DISCONNECTED) >= 1);
        assertNull(nc.getConnectedUrl());

        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            standardConnectionWait(nc);
            assertEquals(1, listener.getEventCount(Events.RECONNECTED));
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testExceptionInConnectionListener() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            BadHandler listener = new BadHandler();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionListener(listener).
                                build();
            Connection nc = standardConnection(options);
            standardCloseConnection(nc);
            assertTrue(((NatsConnection)nc).getStatisticsCollector().getExceptions() > 0);
        }
    }

    @Test
    public void testMultipleConnectionListeners() throws Exception {
        Set<String> capturedEvents = ConcurrentHashMap.newKeySet();

        try (NatsTestServer ts = new NatsTestServer(false)) {
            ListenerForTesting listener = new ListenerForTesting();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionListener(listener).
                                build();
            Connection nc = standardConnection(options);
            assertEquals(ts.getURI(), nc.getConnectedUrl());

            //noinspection DataFlowIssue // addConnectionListener parameter is annotated as @NonNull
            assertThrows(NullPointerException.class, () -> nc.addConnectionListener(null));
            //noinspection DataFlowIssue // removeConnectionListener parameter is annotated as @NonNull
            assertThrows(NullPointerException.class, () -> nc.removeConnectionListener(null));

            ConnectionListener removedConnectionListener = (conn, event) -> capturedEvents.add("NEVER INVOKED");
            nc.addConnectionListener(removedConnectionListener);
            nc.addConnectionListener((conn, event) -> capturedEvents.add("CL1-" + event.name()));
            nc.addConnectionListener((conn, event) -> capturedEvents.add("CL2-" + event.name()));
            nc.addConnectionListener((conn, event) -> { throw new RuntimeException("should not interfere with other listeners"); });
            nc.addConnectionListener((conn, event) -> capturedEvents.add("CL3-" + event.name()));
            nc.addConnectionListener((conn, event) -> capturedEvents.add("CL4-" + event.name()));
            nc.removeConnectionListener(removedConnectionListener);

            standardCloseConnection(nc);
            assertNull(nc.getConnectedUrl());
            assertEquals(1, listener.getEventCount(Events.CLOSED));
            assertTrue(((NatsConnection)nc).getStatisticsCollector().getExceptions() > 0);
        }

        Set<String> expectedEvents = new HashSet<>(Arrays.asList(
                "CL1-CLOSED",
                "CL2-CLOSED",
                "CL3-CLOSED",
                "CL4-CLOSED"));

        assertEquals(expectedEvents, capturedEvents);
    }

    @Test
    public void testConnectionListenerEventCoverage() {
        assertTrue(Events.CONNECTED.isConnectionEvent());
        assertTrue(Events.CLOSED.isConnectionEvent());
        assertTrue(Events.DISCONNECTED.isConnectionEvent());
        assertTrue(Events.RECONNECTED.isConnectionEvent());
        assertFalse(Events.RESUBSCRIBED.isConnectionEvent());
        assertFalse(Events.DISCOVERED_SERVERS.isConnectionEvent());
        assertFalse(Events.LAME_DUCK.isConnectionEvent());

        assertEquals("opened", Events.CONNECTED.getEvent());
        assertEquals("nats: connection opened", Events.CONNECTED.getNatsEvent());
        assertEquals(Events.CONNECTED.getNatsEvent(), Events.CONNECTED.toString());

        assertEquals("lame duck mode", Events.LAME_DUCK.getEvent());
        assertEquals("nats: lame duck mode", Events.LAME_DUCK.getNatsEvent());
        assertEquals(Events.LAME_DUCK.getNatsEvent(), Events.LAME_DUCK.toString());
    }
}
