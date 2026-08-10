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

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.TestBase.standardCloseConnection;
import static io.nats.client.utils.TestBase.standardConnection;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Connection events are handed to listeners on a callback thread, so anything a listener
 * reads back off the Connection is a live read, describing the moment the callback ran and
 * not the moment the event was raised. A listener that reads getConnectedUrl() while
 * handling a RECONNECTED event can therefore see null, because the client has since gone
 * back into tryToConnect, which clears currentServer. That null is the truth about now, it
 * is just not an answer to the question the listener was asking.
 * <p>
 * The answer to that question is the time and uriDetails carried on the four argument
 * connectionEvent, captured when the event is raised. This test pins that contract down: it
 * parks a listener inside a connect callback, tears the connection down while it is parked,
 * and then checks that the detail delivered with the event still describes the event.
 * <p>
 * A listener implementing only the deprecated two argument method has no way to get this,
 * which is the reason to implement the newer one.
 */
@Isolated
public class ConnectionListenerStaleStateTests {

    static class GatedListener implements ConnectionListener {
        final CountDownLatch arrived = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);

        final AtomicReference<Events> gatedEvent = new AtomicReference<>();
        final AtomicReference<String> urlOnEntry = new AtomicReference<>();
        final AtomicReference<String> urlWhenCallbackRan = new AtomicReference<>();
        final AtomicReference<Connection.Status> statusWhenCallbackRan = new AtomicReference<>();
        final AtomicReference<String> uriDetailsFromEvent = new AtomicReference<>();
        final AtomicLong timeFromEvent = new AtomicLong();

        volatile boolean armed = false;

        @Override
        public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
            if (!armed || (type != Events.CONNECTED && type != Events.RECONNECTED)) {
                return;
            }
            armed = false;
            gatedEvent.set(type);
            uriDetailsFromEvent.set(uriDetails);
            timeFromEvent.set(time == null ? 0 : time);
            urlOnEntry.set(conn.getConnectedUrl());

            arrived.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            urlWhenCallbackRan.set(conn.getConnectedUrl());
            statusWhenCallbackRan.set(conn.getStatus());
            finished.countDown();
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
            // everything this test needs arrives on the four argument method
        }
    }

    @Test
    public void testConnectEventDetailsStayCorrectWhenTheCallbackIsProcessedLate() throws Exception {
        int port = NatsTestServer.nextPort();
        GatedListener listener = new GatedListener();

        NatsTestServer ts = new NatsTestServer(port, false);
        NatsConnection nc = null;
        try {
            Options options = new Options.Builder()
                .server(ts.getURI())
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(50))
                .connectionTimeout(Duration.ofMillis(500))
                .connectionListener(listener)
                .errorListener(new ErrorListener() {})
                .build();

            nc = (NatsConnection)standardConnection(options);
            String connectedUrl = nc.getConnectedUrl();
            assertNotNull(connectedUrl);

            // the next connect event is the one the listener parks in
            listener.armed = true;
            nc.forceReconnect();
            assertTrue(listener.arrived.await(20, TimeUnit.SECONDS), "listener parked in the connect callback");
            assertTrue(listener.gatedEvent.get() == Events.RECONNECTED || listener.gatedEvent.get() == Events.CONNECTED);
            assertNotNull(listener.urlOnEntry.get(), "url at the time the event was raised");

            // while the callback is parked, take the server away. the client goes back into
            // tryToConnect, which clears currentServer, and keeps failing.
            ts.close();
            assertTrue(waitForNullUrl(nc, 15_000), "currentServer cleared while the callback was parked");

            long releasedAt = System.currentTimeMillis();
            listener.release.countDown();
            assertTrue(listener.finished.await(20, TimeUnit.SECONDS), "callback finished");

            // Reading the connection tells the listener about now, not about its event. Null
            // here is correct, the server really is gone by the time this callback runs.
            assertNull(listener.urlWhenCallbackRan.get(),
                "getConnectedUrl() inside a callback is a live read");
            assertNotEquals(Connection.Status.CONNECTED, listener.statusWhenCallbackRan.get(),
                "getStatus() inside a callback is a live read");

            // The event carries its own detail, captured when it was raised, so a listener
            // processing late still knows which uri the event was about.
            assertEquals(listener.urlOnEntry.get(), listener.uriDetailsFromEvent.get(),
                "uriDetails describes the connection the event was raised for");
            assertEquals(connectedUrl, listener.uriDetailsFromEvent.get(),
                "uriDetails names the server that was actually connected");

            // And the timestamp is the event time, not the processing time.
            assertTrue(listener.timeFromEvent.get() > 0, "event time supplied");
            assertTrue(listener.timeFromEvent.get() <= releasedAt,
                "event time " + listener.timeFromEvent.get()
                    + " must predate the callback being released at " + releasedAt);
        }
        finally {
            ts.close();
            if (nc != null) {
                standardCloseConnection(nc);
            }
        }
    }

    /**
     * Tracks what an application that watches only events believes the connection state is.
     */
    static class BeliefListener implements ConnectionListener {
        final AtomicReference<Connection.Status> belief = new AtomicReference<>();
        final AtomicLong eventsSeen = new AtomicLong();
        final AtomicLong readsDisagreeingWithTheirEvent = new AtomicLong();
        volatile long workMs = 0;

        @Override
        public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
            if (workMs > 0) {
                try {
                    Thread.sleep(workMs);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Connection.Status fromEvent = beliefFor(type);
            if (fromEvent != null) {
                belief.set(fromEvent);
                if (conn.getStatus() != fromEvent) {
                    readsDisagreeingWithTheirEvent.incrementAndGet();
                }
            }
            eventsSeen.incrementAndGet();
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
        }

        static Connection.Status beliefFor(Events type) {
            if (type == Events.CONNECTED || type == Events.RECONNECTED) {
                return Connection.Status.CONNECTED;
            }
            if (type == Events.DISCONNECTED) {
                return Connection.Status.DISCONNECTED;
            }
            if (type == Events.CLOSED) {
                return Connection.Status.CLOSED;
            }
            return null;
        }
    }

    /**
     * The report also theorized that the status flag ends up wrong, leaving the application
     * believing it is disconnected when it is really connected. It does not. A callback can
     * certainly read a status that disagrees with the event it is handling - that is the same
     * live read as everywhere else in this class, and it is transient - but events are queued
     * in order and none are lost, so once the churn stops and the queue drains, an application
     * tracking connectedness purely from events believes exactly what is true.
     * <p>
     * This asserts that invariant. It is the thing that would actually hurt if it broke.
     */
    @Test
    public void testEventDerivedStateSettlesToTheTruthAfterRapidReconnects() throws Exception {
        int port = NatsTestServer.nextPort();
        BeliefListener listener = new BeliefListener();
        listener.workMs = 100;

        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            Options options = new Options.Builder()
                .server(ts.getURI())
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(50))
                .connectionTimeout(Duration.ofMillis(500))
                .connectionListener(listener)
                .errorListener(new ErrorListener() {})
                .build();

            NatsConnection nc = (NatsConnection)standardConnection(options);
            try {
                for (int i = 0; i < 10; i++) {
                    nc.forceReconnect();
                    Thread.sleep(50);
                }

                assertTrue(waitForStatus(nc, Connection.Status.CONNECTED, 15_000), "connection settled");

                // drain the callback queue, one listener work unit per queued event
                long seen = -1;
                for (int i = 0; i < 40 && seen != listener.eventsSeen.get(); i++) {
                    seen = listener.eventsSeen.get();
                    Thread.sleep(listener.workMs + 100);
                }
                assertEquals(seen, listener.eventsSeen.get(), "callback queue did not drain within timeout");

                assertEquals(nc.getStatus(), listener.belief.get(),
                    "event derived state after " + listener.eventsSeen.get() + " events, of which "
                        + listener.readsDisagreeingWithTheirEvent.get()
                        + " read a live status that disagreed with their own event");
            }
            finally {
                standardCloseConnection(nc);
            }
        }
    }

    private static boolean waitForStatus(Connection nc, Connection.Status status, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (nc.getStatus() == status) {
                return true;
            }
            //noinspection BusyWait
            Thread.sleep(5);
        }
        return false;
    }

    private static boolean waitForNullUrl(Connection nc, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (nc.getConnectedUrl() == null) {
                return true;
            }
            //noinspection BusyWait
            Thread.sleep(5);
        }
        return false;
    }
}
