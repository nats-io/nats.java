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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class ConnectionListenerTests extends TestBase {

    @Test
    public void testToString() {
        assertEquals("nats: connection closed", Events.CLOSED.toString());
    }
    
    @Test
    public void testCloseEvent() throws Exception {
        ListenerByFutures listener = new ListenerByFutures();
        CompletableFuture<Void> fEvent = listener.prepForEvent(Events.CLOSED);
        Options.Builder builder = optionsBuilder().connectionListener(listener);
        runInSharedOwnNc(builder, nc -> {
            standardCloseConnection(nc);
            assertNull(nc.getConnectedUrl());
        });
        listener.validate(fEvent, 500, Events.CLOSED);
    }

    @Test
    public void testDiscoveredServersCountAndListenerInOptions() throws Exception {

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\",\"connect_urls\": [\""+ts.getLocalhostUri()+"\"]}";
            try (NatsServerProtocolMock mockTs2 = new NatsServerProtocolMock(null, customInfo)) {
                ListenerForTesting listener = new ListenerForTesting();
                Options options = optionsBuilder()
                    .server(mockTs2.getMockUri())
                    .maxReconnects(0)
                    .connectionListener(listener)
                    .build();
                                    
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
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .reconnectWait(Duration.ofMillis(100))
                .maxReconnects(-1)
                .connectionListener(listener)
                .build();
            port = ts.getPort();
            nc = standardConnectionWait(options);
            assertEquals(ts.getLocalhostUri(), nc.getConnectedUrl());
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        try { nc.flush(Duration.ofMillis(250)); } catch (Exception exp) { /* ignored */ }

        listener.waitForStatusChange(1000, TimeUnit.MILLISECONDS);
        assertTrue(listener.getEventCount(Events.DISCONNECTED) >= 1);
        assertNull(nc.getConnectedUrl());

        try (NatsTestServer ts = new NatsTestServer(port)) {
            standardConnectionWait(nc);
            assertEquals(1, listener.getEventCount(Events.RECONNECTED));
            assertEquals(ts.getLocalhostUri(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testExceptionInConnectionListener() throws Exception {
        BadHandler badHandler = new BadHandler();
        Options.Builder builder = optionsBuilder().connectionListener(badHandler);
        AtomicReference<Statistics> stats = new AtomicReference<>();
        runInSharedOwnNc(builder, nc -> stats.set(nc.getStatistics()));
        sleep(100); // it needs time here
        assertTrue(stats.get().getExceptions() > 0);
    }

    @Test
    public void testMultipleConnectionListeners() throws Exception {
        Set<String> capturedEvents = ConcurrentHashMap.newKeySet();
        ListenerByFutures listener = new ListenerByFutures();
        CompletableFuture<Void> fClosed = listener.prepForEvent(Events.CLOSED);
        AtomicReference<Statistics> stats = new AtomicReference<>();
        Options.Builder builder = optionsBuilder().connectionListener(listener);
        runInSharedOwnNc(builder, nc -> {
            stats.set(nc.getStatistics());

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
        });

        sleep(100); // it needs time here
        assertTrue(stats.get().getExceptions() > 0);
        listener.validate(fClosed, 500, Events.CLOSED);

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
