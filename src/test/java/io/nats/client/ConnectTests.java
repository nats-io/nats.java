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
import io.nats.client.impl.SimulateSocketDataPortException;
import io.nats.client.support.Listener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.TestBase.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

@Isolated
public class ConnectTests {
    @Test
    public void testConnectWithConfig() throws Exception {
        runInConfiguredServer("simple.conf", ts -> assertCanConnect(optionsBuilder(ts).build()));
    }

    @Test
    public void testConnectVariants() throws Exception {
        try (NatsTestServer ts1 = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                // commas in one server url
                Options options = optionsBuilder().server(ts1.getServerUri() + "," + ts2.getServerUri()).build();
                try (Connection nc = managedConnect(options)) {
                    // coverage for getClientAddress
                    InetAddress inetAddress = nc.getClientInetAddress();
                    assertNotNull(inetAddress);
                    assertTrue(inetAddress.equals(InetAddress.getLoopbackAddress())
                        || inetAddress.equals(InetAddress.getLocalHost()));
                }

                // Randomize
                boolean needOne = true;
                boolean needTwo = true;
                int tries = 20;
                options = options(ts1, ts2);
                while (tries-- > 0 && (needOne || needTwo)) {
                    try (Connection nc = managedConnect(options)) {
                        Collection<String> servers = nc.getServers();
                        assertTrue(servers.contains(ts1.getServerUri()));
                        assertTrue(servers.contains(ts2.getServerUri()));
                        if (ts1.getServerUri().equals(nc.getConnectedUrl())) {
                            needOne = false;
                        }
                        else {
                            needTwo = false;
                        }
                    }
                }
                assertFalse(needOne);
                assertFalse(needTwo);

                // noRandomize
                tries = 3;
                int gotOne = 0;
                int gotTwo = 0;

                // should never get a two
                options = optionsBuilder(ts1.getServerUri(), ts2.getServerUri()).noRandomize().build();
                for (int i = 0; i < tries; i++) {
                    try (Connection nc = managedConnect(options)) {
                        Collection<String> servers = nc.getServers();
                        assertTrue(servers.contains(ts1.getServerUri()));
                        assertTrue(servers.contains(ts2.getServerUri()));
                        if (ts1.getServerUri().equals(nc.getConnectedUrl())) {
                            gotOne++;
                        }
                        else {
                            gotTwo++;
                        }
                    }
                }

                assertEquals(tries, gotOne, "should always ge one");
                assertEquals(0, gotTwo, "should never get two");
            }
        }
    }

    @Test
    public void testFullFakeConnect() throws Exception {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            assertCanConnect(mockTs);
        }
    }

    @Test
    public void testFullFakeConnectWithTabs() throws Exception {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            mockTs.useTabs();
            assertCanConnect(mockTs);
        }
    }

    @Test
    public void testConnectExitBeforeInfo() throws IOException {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.EXIT_BEFORE_INFO)) {
            Options options = optionsBuilder(mockTs).noReconnect().build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testConnectExitAfterInfo() throws IOException {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_INFO)) {
            Options options = optionsBuilder(mockTs).noReconnect().build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testConnectExitAfterConnect() throws IOException {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_CONNECT)) {
            Options options = optionsBuilder(mockTs).noReconnect().build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testConnectExitAfterPing() throws IOException {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
            Options options = optionsBuilder(mockTs).noReconnect().build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testConnectionFailureWithFallback() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsServerProtocolMock mock = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options options = optionsBuilder(mock, ts)
                    .noRandomize()
                    .build();
                assertCanConnect(options);
            }
        }
    }

    @Test
    public void testFailWithMissingLineFeedAfterInfo() throws Exception {
        String badInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}\rmore stuff";
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, badInfo)) {
            Options options = optionsBuilder(mockTs).reconnectWait(Duration.ofDays(1)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testFailWithStuffAfterInitialInfo() throws Exception {
        String badInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}\r\nmore stuff";
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, badInfo)) {
            Options options = optionsBuilder(mockTs).reconnectWait(Duration.ofDays(1)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testFailWrongInitialInfoOP() throws Exception {
        String badInfo = "PING {\"server_id\":\"test\", \"version\":\"9.9.99\"}\r\n"; // wrong op code
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, badInfo)) {
            mockTs.useCustomInfoAsFullInfo();
            Options options = optionsBuilder(mockTs).reconnectWait(Duration.ofDays(1)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testIncompleteInitialInfo() throws Exception {
        String badInfo = "{\"server_id\"\r\n";
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, badInfo)) {
            Options options = optionsBuilder(mockTs).reconnectWait(Duration.ofDays(1)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testAsyncConnection() throws Exception {
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts).connectionListener(listener).build();
            listener.queueConnectionEvent(Events.CONNECTED);
            Nats.connectAsynchronously(options, false);
            listener.validate();

            Connection nc = listener.getLastConnectionEventConnection();
            assertNotNull(nc);
            assertConnected(nc);
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testAsyncConnectionWithReconnect() throws Exception {
        Listener listener = new Listener();
        int port = NatsTestServer.nextPort();
        Options options = optionsBuilder(port).maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(100)).connectionListener(listener).build();

        Nats.connectAsynchronously(options, true);

        sleep(5000); // No server at this point, let it fail and try to start over

        Connection nc = listener.getLastConnectionEventConnection(); // will be disconnected, but should be there
        assertNotNull(nc);

        listener.queueConnectionEvent(Events.RECONNECTED);
        try (NatsTestServer ignored = new NatsTestServer(port)) {
            confirmConnectedThenClosed(nc);
        }
    }

    @Test
    public void testThrowOnAsyncWithoutListener() throws Exception {
        Options options = optionsBuilder(NatsTestServer.nextPort()).build();
        assertThrows(IllegalArgumentException.class, () -> Nats.connectAsynchronously(options, false));
    }

    @Test
    public void testErrorOnAsync() throws Exception {
        Listener listener = new Listener();
        Options options = optionsBuilder(NatsTestServer.nextPort())
            .connectionListener(listener)
            .errorListener(listener)
            .noReconnect()
            .build();
        listener.queueConnectionEvent(Events.CLOSED);
        Nats.connectAsynchronously(options, false);
        listener.validate();
        assertTrue(listener.getExceptionCount() > 0);
    }

    @Test
    public void testConnectionTimeout() throws Exception {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) { // will sleep for 3
            Options options = optionsBuilder(mockTs)
                .noReconnect()
                .connectionTimeout(Duration.ofSeconds(2)) // 2 is also the default but explicit for test
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testSlowConnectionNoTimeout() throws Exception {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) {
            Options options = optionsBuilder(mockTs)
                .noReconnect()
                .connectionTimeout(Duration.ofSeconds(6)) // longer than the sleep
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testTimeCheckCoverage() throws Exception {
        List<String> traces = new ArrayList<>();
        TimeTraceLogger l = (f, a) -> traces.add(String.format(f, a));

        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts).traceConnection().build();
            assertCanConnect(options);

            options = optionsBuilder(ts).timeTraceLogger(l).build();
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

        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                    .traceConnection()
                    .timeTraceLogger(l)
                    .reconnectWait(Duration.ofSeconds(1))
                    .maxReconnects(1)
                    .connectionTimeout(Duration.ofSeconds(2))
                    .build();

            try (Connection nc = managedConnect(options)) {
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
            Nats.connect(options("nats://testserver.notnats:4222, nats://testserver.alsonotnats:4223"));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("testserver.notnats:4222"));
            assertTrue(e.getMessage().contains("testserver.alsonotnats:4223"));
        }
    }

    @Test
    public void testFlushBuffer() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Connection nc = managedConnect(options(ts));

            // test connected
            nc.flushBuffer();

            ts.shutdown();
            while (nc.getStatus() == Connection.Status.CONNECTED) {
                sleep(10);
            }

            // test while reconnecting
            assertThrows(IllegalStateException.class, nc::flushBuffer);
            closeAndConfirm(nc);

            // test when closed.
            assertThrows(IllegalStateException.class, nc::flushBuffer);
        }
    }

    @Test
    public void testFlushBufferThreadSafety() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Connection nc = managedConnect(options(ts));

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
                    String subject = random();
                    for (int i = 1; i <= 50000; i++) {
                        nc.publish(subject, payload);
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
                //noinspection ResultOfMethodCallIgnored
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

            // cleanup and double-check the thread is done.
            t.join(2000);

            // make sure the publisher actually completed.
            assertTrue(completedLatch.await(10, TimeUnit.SECONDS));

            closeAndConfirm(nc);
        }
    }

    @SuppressWarnings({"unused", "UnusedAssignment"})
    @Test
    public void testSocketLevelException() throws Exception {
        int port = NatsTestServer.nextPort();

        AtomicBoolean simExReceived = new AtomicBoolean();
        Listener listener = new Listener();
        ErrorListener el = new ErrorListener() {
            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                if (exp.getMessage().contains("Simulated Exception")) {
                    simExReceived.set(true);
                }
            }
        };

        Options options = optionsBuilder(port)
            .dataPortType("io.nats.client.impl.SimulateSocketDataPortException")
            .connectionListener(listener)
            .errorListener(el)
            .reconnectDelayHandler(l -> Duration.ofSeconds(1))
            .build();

        Connection connection = null;

        // 1. DO NOT RECONNECT ON CONNECT
        try (NatsTestServer ts = new NatsTestServer(port)) {
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
        try (NatsTestServer ts = new NatsTestServer(port)) {
            try {
                SimulateSocketDataPortException.THROW_ON_CONNECT.set(true);
                listener.queueConnectionEvent(Events.RECONNECTED);
                connection = Nats.connectReconnectOnConnect(options);
                listener.validate();
                listener.queueConnectionEvent(Events.DISCONNECTED);
            }
            catch (Exception e) {
                fail("should have connected " + e);
            }
        }
        listener.validate();
        assertTrue(simExReceived.get());
        simExReceived.set(false);

        // 2. NORMAL RECONNECT
        listener.queueConnectionEvent(Events.RECONNECTED);
        try (NatsTestServer ts = new NatsTestServer(port)) {
            SimulateSocketDataPortException.THROW_ON_CONNECT.set(true);
            listener.validate();
        }
    }

    @Test
    public void testRunInJsCluster() throws Exception {
        Listener[] listeners = new Listener[3];
        listeners[0] = new Listener();
        listeners[1] = new Listener();
        listeners[2] = new Listener();

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

            @Override
            public boolean jetStream() {
                return true;
            }
        };

        listeners[0] = new Listener();
        listeners[1] = new Listener();
        listeners[2] = new Listener();

        runInCluster(tstOpts, (nc1, nc2, nc3) -> {
            Thread.sleep(200);
            ServerInfo si1 = nc1.getServerInfo();
            ServerInfo si2 = nc2.getServerInfo();
            ServerInfo si3 = nc3.getServerInfo();
            assertTrue(si1.isJetStreamAvailable());
            assertTrue(si2.isJetStreamAvailable());
            assertTrue(si3.isJetStreamAvailable());
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
        });
    }

    // https://github.com/nats-io/nats.java/issues/1201
    @Test
    void testLowConnectionTimeoutResultsInIOException() {
        Options options = Options.builder()
                .connectionTimeout(Duration.ZERO)
                .build();
        assertThrows(IOException.class, () -> Nats.connect(options));
    }

    @Test
    void testConnectWithFastFallback() throws Exception {
        // this is pretty much a coverage test
        runInSharedOwnNc(optionsBuilder().enableFastFallback(), nc -> {});
    }

    @Test
    void testConnectPendingCountCoverage() throws Exception {
        runInOwnServer(nc -> {
            AtomicLong outgoingPendingMessageCount = new AtomicLong();
            AtomicLong outgoingPendingBytes = new AtomicLong();

            AtomicBoolean tKeepGoing = new AtomicBoolean(true);
            Thread t = new Thread(() -> {
                while (tKeepGoing.get()) {
                    outgoingPendingMessageCount.set(Math.max(outgoingPendingMessageCount.get(), nc.outgoingPendingMessageCount()));
                    outgoingPendingBytes.set(Math.max(outgoingPendingBytes.get(), nc.outgoingPendingBytes()));
                    sleep(10);
                }
            });
            t.start();

            String subject = random();
            byte[] data = new byte[8 * 1024];
            for (int x = 0; x < 5000; x++) {
                nc.publish(subject, data);
            }
            tKeepGoing.set(false);
            t.join();

            assertTrue(outgoingPendingMessageCount.get() > 0);
            assertTrue(outgoingPendingBytes.get() > outgoingPendingMessageCount.get() * 1000);
        });
    }
}
