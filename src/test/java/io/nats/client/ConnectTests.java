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

package io.nats.client;

import io.nats.client.ConnectionListener.Events;
import io.nats.client.NatsServerProtocolMock.ExitAt;
import io.nats.client.api.ServerInfo;
import io.nats.client.impl.ListenerForTesting;
import io.nats.client.impl.SimulateSocketDataPortException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class ConnectTests {
    @Test
    public void testDefaultConnection() throws Exception {
        try (NatsTestServer ignored = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection nc = standardConnection();
            assertEquals(Options.DEFAULT_PORT, nc.getServerInfo().getPort());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = standardConnection(ts.getURI());
            assertEquals(ts.getPort(), nc.getServerInfo().getPort());
            // coverage for getClientAddress
            InetAddress inetAddress = nc.getClientInetAddress();
            assertTrue(inetAddress.equals(InetAddress.getLoopbackAddress())
                || inetAddress.equals(InetAddress.getLocalHost()));
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testConnectionWithOptions() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testFullFakeConnect() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            assertCanConnect(ts.getURI());
        }
    }

    @Test
    public void testFullFakeConnectWithTabs() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            ts.useTabs();
            assertCanConnect(ts.getURI());
        }
    }

    @Test
    public void testConnectExitBeforeInfo() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_BEFORE_INFO)) {
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().build();
                assertCanConnect(options);
            }
        });
    }

    @Test
    public void testConnectExitAfterInfo() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_INFO)) {
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().build();
                assertCanConnect(options);
            }
        });
    }

    @Test
    public void testConnectExitAfterConnect() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_CONNECT)) {
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().build();
                assertCanConnect(options);
            }
        });
    }

    @Test
    public void testConnectExitAfterPing() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().build();
                assertCanConnect(options);
            }
        });
    }

    @Test
    public void testConnectionFailureWithFallback() throws Exception {

        try (NatsTestServer ts = new NatsTestServer(false)) {
            try (NatsServerProtocolMock fake = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options options = new Options.Builder().connectionTimeout(Duration.ofSeconds(5)).server(fake.getURI())
                        .server(ts.getURI()).build();
                assertCanConnect(options);
            }
        }
    }

    @Test
    public void testConnectWithConfig() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/simple.conf", false)) {
            assertCanConnect(ts.getURI());
        }
    }

    @Test
    public void testConnectWithCommas() throws Exception {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                assertCanConnect(ts1.getURI() + "," + ts2.getURI());
            }
        }
    }

    @Test
    public void testConnectRandomize() throws Exception {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                boolean needOne = true;
                boolean needTwo = true;
                int count = 0;
                int maxTries = 100;
                while (count++ < maxTries && (needOne || needTwo)) {
                    Connection nc = standardConnection(ts1.getURI() + "," + ts2.getURI());
                    if (nc.getConnectedUrl().equals(ts1.getURI())) {
                        needOne = false;
                    } else {
                        needTwo = false;
                    }
                    Collection<String> servers = nc.getServers();
                    assertTrue(servers.contains(ts1.getURI()));
                    assertTrue(servers.contains(ts2.getURI()));
                    standardCloseConnection(nc);
                }
                assertFalse(needOne);
                assertFalse(needTwo);
            }
        }
    }

    @Test
    public void testConnectNoRandomize() throws Exception {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                int one = 0;
                int two = 0;

                // should get at least 1 for each
                for (int i = 0; i < 10; i++) {
                    String[] servers = { ts1.getURI(), ts2.getURI() };
                    Options options = new Options.Builder().noRandomize().servers(servers).build();
                    Connection nc = standardConnection(options);
                    if (nc.getConnectedUrl().equals(ts1.getURI())) {
                        one++;
                    } else {
                        two++;
                    }
                    standardCloseConnection(nc);
                }

                assertEquals(one, 10, "always got one");
                assertEquals(two, 0, "never got two");
            }
        }
    }

    @Test
    public void testFailWithMissingLineFeedAfterInfo() {
        assertThrows(IOException.class, () -> {
            String badInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}\rmore stuff";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testFailWithStuffAfterInitialInfo() {
        assertThrows(IOException.class, () -> {
            String badInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}\r\nmore stuff";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testFailWrongInitialInfoOP() {
        assertThrows(IOException.class, () -> {
            String badInfo = "PING {\"server_id\":\"test\", \"version\":\"9.9.99\"}\r\n"; // wrong op code
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                ts.useCustomInfoAsFullInfo();
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testIncompleteInitialInfo() {
        assertThrows(IOException.class, () -> {
            String badInfo = "{\"server_id\"\r\n";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testAsyncConnection() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        Connection nc = null;

        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).connectionListener(listener).build();
            listener.prepForStatusChange(Events.CONNECTED);

            Nats.connectAsynchronously(options, false);

            listener.waitForStatusChange(1, TimeUnit.SECONDS);

            nc = listener.getLastEventConnection();
            assertNotNull(nc);
            assertConnected(nc);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testAsyncConnectionWithReconnect() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();
        Options options = new Options.Builder().server("nats://localhost:" + port).maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(100)).connectionListener(listener).build();

        Nats.connectAsynchronously(options, true);

        sleep(5000); // No server at this point, let it fail and try to start over

        Connection nc = listener.getLastEventConnection(); // will be disconnected, but should be there
        assertNotNull(nc);

        listener.prepForStatusChange(Events.RECONNECTED);
        try (NatsTestServer ignored = new NatsTestServer(port, false)) {
            standardConnectionWait(nc, listener);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testThrowOnAsyncWithoutListener() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false)) {
                Options options = new Options.Builder().server(ts.getURI()).build();
                Nats.connectAsynchronously(options, false);
            }
        });
    }

    @Test
    public void testErrorOnAsync() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        Options options = new Options.Builder().server("nats://localhost:" + NatsTestServer.nextPort())
                .connectionListener(listener).errorListener(listener).noReconnect().build();
        listener.prepForStatusChange(Events.CLOSED);
        Nats.connectAsynchronously(options, false);
        listener.waitForStatusChange(10, TimeUnit.SECONDS);

        assertTrue(listener.getExceptionCount() > 0);
        assertTrue(listener.getEventCount(Events.CLOSED) > 0);
    }

    @Test
    public void testConnectionTimeout() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) { // will sleep for 3
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().traceConnection()
                        .connectionTimeout(Duration.ofSeconds(2)). // 2 is also the default but explicit for test
                build();
                Connection nc = Nats.connect(options);
                assertNotSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            }
        });
    }

    @Test
    public void testSlowConnectionNoTimeout() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) {
            Options options = new Options.Builder().server(ts.getURI()).noReconnect()
                    .connectionTimeout(Duration.ofSeconds(6)). // longer than the sleep
                    build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testTimeCheckCoverage() throws Exception {
        List<String> traces = new ArrayList<>();
        TimeTraceLogger l = (f, a) -> traces.add(String.format(f, a));

        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).traceConnection().build();
            assertCanConnect(options);

            options = new Options.Builder().server(ts.getURI()).timeTraceLogger(l).build();
            assertCanConnect(options);
        }

        int i = 0;
        assertTrue(traces.get(i++).startsWith("creating connection object"));
        assertTrue(traces.get(i++).startsWith("creating NUID"));
        assertTrue(traces.get(i++).startsWith("creating executors"));
        assertTrue(traces.get(i++).startsWith("creating reader and writer"));
        assertTrue(traces.get(i++).startsWith("connection object created"));
        assertTrue(traces.get(i++).startsWith("starting connect loop"));
        assertTrue(traces.get(i++).startsWith("setting status to connecting"));
        assertTrue(traces.get(i++).startsWith("trying to connect"));
        assertTrue(traces.get(i++).startsWith("starting connection attempt"));
        assertTrue(traces.get(i++).startsWith("waiting for reader"));
        assertTrue(traces.get(i++).startsWith("waiting for writer"));
        assertTrue(traces.get(i++).startsWith("cleaning pong queue"));
        assertTrue(traces.get(i++).startsWith("connecting data port"));
        assertTrue(traces.get(i++).startsWith("reading info"));
        assertTrue(traces.get(i++).startsWith("starting reader"));
        assertTrue(traces.get(i++).startsWith("starting writer"));
        assertTrue(traces.get(i++).startsWith("sending connect message"));
        assertTrue(traces.get(i++).startsWith("sending initial ping"));
        assertTrue(traces.get(i++).startsWith("starting ping and cleanup timers"));
        assertTrue(traces.get(i++).startsWith("updating status to connected"));
        assertTrue(traces.get(i++).startsWith("status updated"));
        assertTrue(traces.get(i).startsWith("connect complete"));
    }

    @Test
    public void testReconnectLogging() throws Exception {
        List<String> traces = new ArrayList<>();
        TimeTraceLogger l = (f, a) -> traces.add(String.format(f, a));

        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder()
                    .server(ts.getURI())
                    .traceConnection()
                    .timeTraceLogger(l)
                    .reconnectWait(Duration.ofSeconds(1))
                    .maxReconnects(1)
                    .connectionTimeout(Duration.ofSeconds(2))
                    .build();

            try (Connection nc = Nats.connect(options)) {
                assertConnected(nc);
                ts.close();
                Thread.sleep(3000);
            }
        }

        boolean foundReconnectLog = traces.stream().anyMatch(s -> s.contains("reconnecting to server"));
        assertTrue(foundReconnectLog, "Reconnect log not found");
    }

    @Test
    public void testConnectExceptionHasURLS() {
        try {
            Nats.connect("nats://testserver.notnats:4222, nats://testserver.alsonotnats:4223");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("testserver.notnats:4222"));
            assertTrue(e.getMessage().contains("testserver.alsonotnats:4223"));
        }
    }

    @Test
    public void testFlushBuffer() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = standardConnection(ts.getURI());

            // test connected
            nc.flushBuffer();

            ts.shutdown();
            while (nc.getStatus() == Connection.Status.CONNECTED) {
                sleep(10);
            }

            // test while reconnecting
            assertThrows(IllegalStateException.class, nc::flushBuffer);
            standardCloseConnection(nc);

            // test when closed.
            assertThrows(IllegalStateException.class, nc::flushBuffer);
        }
    }

    @Test
    public void testFlushBufferThreadSafety() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = standardConnection(ts.getURI());

            // use two latches to sync the threads as close as
            // possible.
            CountDownLatch pubLatch = new CountDownLatch(1);
            CountDownLatch flushLatch = new CountDownLatch(1);
            CountDownLatch completedLatch = new CountDownLatch(1);

            Thread t = new Thread("publisher") {
                @SuppressWarnings("ResultOfMethodCallIgnored")
                public void run() {
                    byte[] payload = new byte[5];
                    pubLatch.countDown();
                    try {
                        flushLatch.await(2, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // NOOP
                    }
                    for (int i = 1; i <= 50000; i++) {
                        nc.publish("foo", payload);
                        if (i % 2000 == 0) {
                            try {
                                nc.flushBuffer();
                            } catch (IOException e) {
                                break;
                            }
                        }
                    }
                    completedLatch.countDown();
                }
            };

            t.start();

            // sync up the current thread and the publish thread
            // to get the most out of the test.
            try {
               pubLatch.await(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                // NOOP
            }
            flushLatch.countDown();

            // flush as fast as we can while the publisher
            // is publishing.

            while (t.isAlive()) {
                nc.flushBuffer();
            }

            // cleanup and doublecheck the thread is done.
            t.join(2000);

            // make sure the publisher actually completed.
            assertTrue(completedLatch.await(10, TimeUnit.SECONDS));

            standardCloseConnection(nc);
        }
    }

    @SuppressWarnings({"unused", "UnusedAssignment"})
    @Test
    public void testSocketLevelException() throws Exception {
        int port = NatsTestServer.nextPort();

        AtomicBoolean simExReceived = new AtomicBoolean();
        ListenerForTesting listener = new ListenerForTesting();
        ErrorListener el = new ErrorListener() {
            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                if (exp.getMessage().contains("Simulated Exception")) {
                    simExReceived.set(true);
                }
            }
        };

        Options options = new Options.Builder()
            .server(NatsTestServer.getNatsLocalhostUri(port))
            .dataPortType("io.nats.client.impl.SimulateSocketDataPortException")
            .connectionListener(listener)
            .errorListener(el)
            .reconnectDelayHandler(l -> Duration.ofSeconds(1))
            .build();

        Connection connection = null;

        // 1. DO NOT RECONNECT ON CONNECT
        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            try {
                SimulateSocketDataPortException.THROW_ON_CONNECT.set(true);
                connection = Nats.connect(options);
                fail();
            }
            catch (Exception ignore) {}
        }

        Thread.sleep(200); // just making sure messages get through
        assertNull(connection);
        assertTrue(simExReceived.get());
        simExReceived.set(false);

        // 2. RECONNECT ON CONNECT
        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            try {
                SimulateSocketDataPortException.THROW_ON_CONNECT.set(true);
                listener.prepForStatusChange(Events.RECONNECTED);
                connection = Nats.connectReconnectOnConnect(options);
                assertTrue(listener.waitForStatusChange(5, TimeUnit.SECONDS));
                listener.prepForStatusChange(Events.DISCONNECTED);
            }
            catch (Exception e) {
                fail("should have connected " + e);
            }
        }
        assertTrue(listener.waitForStatusChange(5, TimeUnit.SECONDS));
        assertTrue(simExReceived.get());
        simExReceived.set(false);

        // 2. NORMAL RECONNECT
        listener.prepForStatusChange(Events.RECONNECTED);
        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            SimulateSocketDataPortException.THROW_ON_CONNECT.set(true);
            try {
                assertTrue(listener.waitForStatusChange(5, TimeUnit.SECONDS));
            }
            catch (Exception e) {
                fail("should have reconnected " + e);
            }
        }
    }

    @Test
    public void testRunInJsCluster() throws Exception {
        ListenerForTesting[] listeners = new ListenerForTesting[3];
        listeners[0] = new ListenerForTesting();
        listeners[1] = new ListenerForTesting();
        listeners[2] = new ListenerForTesting();

        ThreeServerTestOptions tstOpts = new ThreeServerTestOptions() {
            @Override
            public void append(int index, Options.Builder builder) {
                builder.connectionListener(listeners[index]).errorListener(listeners[index]);
            }

            @Override
            public boolean configureAccount() {
                return true;
            }

            @Override
            public boolean includeAllServers() {
                return true;
            }
        };

        runInJsCluster(ConnectTests::validateRunInJsCluster);

        listeners[0] = new ListenerForTesting();
        listeners[1] = new ListenerForTesting();
        listeners[2] = new ListenerForTesting();

        runInJsCluster(tstOpts, ConnectTests::validateRunInJsCluster);
    }

    private static void validateRunInJsCluster(Connection nc1, Connection nc2, Connection nc3) throws InterruptedException {
        Thread.sleep(200);
        ServerInfo si1 = nc1.getServerInfo();
        ServerInfo si2 = nc2.getServerInfo();
        ServerInfo si3 = nc3.getServerInfo();
        assertEquals(si1.getCluster(), si2.getCluster());
        assertEquals(si1.getCluster(), si3.getCluster());
        String port1 = "" + si1.getPort();
        String port2 = "" + si2.getPort();
        String port3 = "" + si3.getPort();
        String urls1 = String.join(",", si1.getConnectURLs());
        String urls2 = String.join(",", si2.getConnectURLs());
        String urls3 = String.join(",", si3.getConnectURLs());
        assertTrue(urls1.contains(port1));
        assertTrue(urls1.contains(port2));
        assertTrue(urls1.contains(port3));
        assertTrue(urls2.contains(port1));
        assertTrue(urls2.contains(port2));
        assertTrue(urls2.contains(port3));
        assertTrue(urls3.contains(port1));
        assertTrue(urls3.contains(port2));
        assertTrue(urls3.contains(port3));
    }
}
