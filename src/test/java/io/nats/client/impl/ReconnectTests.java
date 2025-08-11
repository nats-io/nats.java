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
import io.nats.client.api.ServerInfo;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.nats.client.NatsTestServer.getNatsLocalhostUri;
import static io.nats.client.support.NatsConstants.OUTPUT_QUEUE_IS_FULL;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

@Isolated
public class ReconnectTests {

    void checkReconnectingStatus(Connection nc) {
        Connection.Status status = nc.getStatus();
        assertTrue(Connection.Status.RECONNECTING == status ||
                                            Connection.Status.DISCONNECTED == status, "Reconnecting status");
    }

    @Test
    public void testSimpleReconnect() throws Exception { //Includes test for subscriptions and dispatchers across reconnect
        Function<Integer, NatsTestServer> ntsSupplier = port -> {
            try {
                return new NatsTestServer(port, false);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        _testReconnect(ntsSupplier, (ts, builder) -> builder.server(ts.getURI()));
    }

    @Test
    public void testWsReconnect() throws Exception { //Includes test for subscriptions and dispatchers across reconnect
        Function<Integer, NatsTestServer> ntsSupplier = port -> {
            try {
                return new NatsTestServer("src/test/resources/ws_operator.conf", port, false);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        _testReconnect(ntsSupplier, (ts, builder) ->
            builder.server(ts.getLocalhostUri("ws")).authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")));
    }

    private void _testReconnect(Function<Integer, NatsTestServer> ntsSupplier, BiConsumer<NatsTestServer, Options.Builder> optSetter) throws Exception {
        int port = NatsTestServer.nextPort();
        ListenerForTesting listener = new ListenerForTesting();
        NatsConnection nc;
        Subscription sub;
        long start;
        long end;
        try (NatsTestServer ts = ntsSupplier.apply(port)) {
            Options.Builder builder = new Options.Builder()
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(1000))
                .connectionListener(listener);
            optSetter.accept(ts, builder);
            Options options = builder.build();

            nc = (NatsConnection)standardConnection(options);

            sub = nc.subscribe("subsubject");

            final NatsConnection nnc = nc; // final for the lambda
            Dispatcher d = nc.createDispatcher((msg) -> nnc.publish(msg.getReplyTo(), msg.getData()) );
            d.subscribe("dispatchSubject");
            flushConnection(nc);

            Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            nc.publish("subsubject", null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);

            listener.prepForStatusChange(Events.DISCONNECTED);
            start = System.nanoTime();
        }

        flushAndWaitLong(nc, listener);
        checkReconnectingStatus(nc);

        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = ntsSupplier.apply(port)) {
            listenerConnectionWait(nc, listener, LONG_CONNECTION_WAIT_MS);

            end = System.nanoTime();

            assertTrue(1_000_000 * (end-start) > 1000, "reconnect wait");

            // Make sure dispatcher and subscription are still there
            Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);

            // make sure the subscription survived
            nc.publish("subsubject", null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
        }

        assertEquals(1, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        assertTrue(nc.getStatisticsCollector().getExceptions() > 0, "exception count");
        standardCloseConnection(nc);
    }

    @Test
    public void testSubscribeDuringReconnect() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port;
        Subscription sub;

        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(-1).
                                reconnectWait(Duration.ofMillis(20)).
                                connectionListener(listener).
                                build();
                                port = ts.getPort();
            nc = (NatsConnection) standardConnection(options);
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        flushAndWaitLong(nc, listener);
        checkReconnectingStatus(nc);

        sub = nc.subscribe("subsubject");

        final NatsConnection nnc = nc;
        Dispatcher d = nc.createDispatcher((msg) -> nnc.publish(msg.getReplyTo(), msg.getData()));
        d.subscribe("dispatchSubject");

        listener.prepForStatusChange(Events.RECONNECTED);

        try (NatsTestServer ignored = new NatsTestServer(port, false)) {
            listenerConnectionWait(nc, listener);

            // Make sure the dispatcher and subscription are still there
            Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            // make sure the subscription survived
            nc.publish("subsubject", null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
        }

        assertEquals(1, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        assertTrue(nc.getStatisticsCollector().getExceptions() > 0, "exception count");
        standardCloseConnection(nc);
    }

    @Test
    public void testReconnectBuffer() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();
        Subscription sub;
        long start;
        long end;
        String[] customArgs = {"--user","stephen","--pass","password"};

        try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(-1).
                                userInfo("stephen".toCharArray(), "password".toCharArray()).
                                reconnectWait(Duration.ofMillis(1000)).
                                connectionListener(listener).
                                build();
            nc = (NatsConnection) standardConnection(options);

            sub = nc.subscribe("subsubject");

            final NatsConnection nnc = nc;
            Dispatcher d = nc.createDispatcher((msg) -> nnc.publish(msg.getReplyTo(), msg.getData()));
            d.subscribe("dispatchSubject");
            nc.flush(Duration.ofMillis(1000));

            Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            nc.publish("subsubject", null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);

            listener.prepForStatusChange(Events.DISCONNECTED);
            start = System.nanoTime();
        }

        flushAndWaitLong(nc, listener);
        checkReconnectingStatus(nc);

        // Send a message to the dispatcher and one to the subscriber
        // These should be sent on reconnect
        Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
        nc.publish("subsubject", null);
        nc.publish("subsubject", null);

        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port, false)) {
            listenerConnectionWait(nc, listener);

            end = System.nanoTime();

            assertTrue(1_000_000 * (end-start) > 1000, "reconnect wait");

            // Check the message we sent to the dispatcher
            Message msg = inc.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);

            // Check the two we sent to subscriber
            msg = sub.nextMessage(Duration.ofMillis(500));
            assertNotNull(msg);

            msg = sub.nextMessage(Duration.ofMillis(500));
            assertNotNull(msg);
        }

        assertEquals(1, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        assertTrue(nc.getStatisticsCollector().getExceptions() > 0, "exception count");
        standardCloseConnection(nc);
    }

    @Test
    public void testMaxReconnects() throws Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();

        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(1).
                                connectionListener(listener).
                                reconnectWait(Duration.ofMillis(10)).
                                build();
            nc = standardConnection(options);
            listener.prepForStatusChange(Events.CLOSED);
        }

        flushAndWaitLong(nc, listener);
        assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
        standardCloseConnection(nc);
    }

    @Test
    public void testReconnectToSecondServer() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();

        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            server(ts.getURI()).
                                            noRandomize().
                                            connectionListener(listener).
                                            maxReconnects(-1).
                                            build();
                nc = (NatsConnection) standardConnection(options);
                assertEquals(ts2.getURI(), nc.getConnectedUrl());
                listener.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWaitLong(nc, listener);

            assertConnected(nc);
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testNoRandomizeReconnectToSecondServer() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();

        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            server(ts.getURI()).
                                            connectionListener(listener).
                                            maxReconnects(-1).
                                            noRandomize().
                                            build();
                nc = (NatsConnection) standardConnection(options);
                assertEquals(nc.getConnectedUrl(), ts2.getURI());
                listener.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWaitLong(nc, listener);

            assertConnected(nc);
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testReconnectToSecondServerFromInfo() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();

        try (NatsTestServer ts = new NatsTestServer()) {
            String striped = ts.getURI().substring("nats://".length()); // info doesn't have protocol
            String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\",\"connect_urls\": [\""+striped+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            connectionListener(listener).
                                            maxReconnects(-1).
                                            connectionTimeout(Duration.ofSeconds(5)).
                                            reconnectWait(Duration.ofSeconds(1)).
                                            build();
                nc = (NatsConnection) standardConnection(options);
                assertEquals(nc.getConnectedUrl(), ts2.getURI());
                listener.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWaitLong(nc, listener);

            assertConnected(nc);
            assertTrue(ts.getURI().endsWith(nc.getConnectedUrl()));
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testOverflowReconnectBuffer() {
        assertThrows(IllegalStateException.class, () -> {
            Connection nc;
            ListenerForTesting listener = new ListenerForTesting();

            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        maxReconnects(-1).
                                        connectionListener(listener).
                                        reconnectBufferSize(4*512).
                                        reconnectWait(Duration.ofSeconds(480)).
                                        build();
                nc = standardConnection(options);
                listener.prepForStatusChange(Events.DISCONNECTED);
            }

            flushAndWaitLong(nc, listener);
            checkReconnectingStatus(nc);

            for (int i=0;i<20;i++) {
                nc.publish("test", new byte[512]);// Should blow up by the 5th message
            }
        });
    }

    @Test
    public void testInfiniteReconnectBuffer() throws Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(5).
                                    connectionListener(listener).
                                    reconnectBufferSize(-1).
                                    reconnectWait(Duration.ofSeconds(30)).
                                    build();
            nc = standardConnection(options);
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        flushAndWaitLong(nc, listener);
        checkReconnectingStatus(nc);

        byte[] payload = new byte[1024];
        for (int i=0;i<1_000;i++) {
            nc.publish("test", payload);
        }

        standardCloseConnection(nc);
    }

    @Test
    public void testReconnectDropOnLineFeed() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();
        Duration reconnectWait = Duration.ofMillis(100); // thrash
        int thrashCount = 5;
        CompletableFuture<Boolean> gotSub = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> subRef = new AtomicReference<>(gotSub);
        CompletableFuture<Boolean> sendMsg = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> sendRef = new AtomicReference<>(sendMsg);

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String subLine;

            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for SUB ...");
            try {
                subLine = r.readLine();
            } catch(Exception e) {
                subRef.get().cancel(true);
                return;
            }

            if (subLine.startsWith("SUB")) {
                subRef.get().complete(Boolean.TRUE);
            }

            try {
                sendRef.get().get();
            } catch (Exception e) {
                //keep going
            }

            w.write("MSG\r"); // Drop the line feed
            w.flush();
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(-1).
                                reconnectWait(reconnectWait).
                                connectionListener(listener).
                                build();
                                port = ts.getPort();
            nc = (NatsConnection) Nats.connect(options);
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            nc.subscribe("test");
            subRef.get().get();
            listener.prepForStatusChange(Events.DISCONNECTED);
            sendRef.get().complete(true);
            flushAndWaitLong(nc, listener); // mock server will close so we do this inside the curly
        }

        // Thrash in and out of connect status
        // server starts thrashCount times, so we should succeed thrashCount x
        for (int i=0;i<thrashCount;i++) {
            checkReconnectingStatus(nc);

            // connect good then bad
            listener.prepForStatusChange(Events.RESUBSCRIBED);
            try (NatsTestServer ignored = new NatsTestServer(port, false)) {
                listenerConnectionWait(nc, listener);
                listener.prepForStatusChange(Events.DISCONNECTED);
            }

            flushAndWaitLong(nc, listener); // nats won't close until we tell it, so put this outside the curly
            checkReconnectingStatus(nc);

            gotSub = new CompletableFuture<>();
            subRef.set(gotSub);
            sendMsg = new CompletableFuture<>();
            sendRef.set(sendMsg);

            listener.prepForStatusChange(Events.RESUBSCRIBED);
            try (NatsServerProtocolMock ignored = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
                listenerConnectionWait(nc, listener);
                subRef.get().get();
                listener.prepForStatusChange(Events.DISCONNECTED);
                sendRef.get().complete(true);
                flushAndWaitLong(nc, listener); // mock server will close so we do this inside the curly
            }
        }


        assertEquals(2 * thrashCount, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        standardCloseConnection(nc);
    }

    @Test
    public void testReconnectNoIPTLSConnection() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();

        int tsPort = NatsTestServer.nextPort();
        int ts2Port = NatsTestServer.nextPort();
        int tsCPort = NatsTestServer.nextPort();
        int ts2CPort = NatsTestServer.nextPort();

        String[] tsInserts = {
                "cluster {",
                "name: testClusterName",
                "listen: localhost:" + tsCPort,
                "routes = [",
                "nats-route://localhost:" + ts2CPort,
                "]",
                "}"
        };
        String[] ts2Inserts = {
                "cluster {",
                "name: testClusterName",
                "listen: localhost:" + ts2CPort,
                "routes = [",
                "nats-route://127.0.0.1:" + tsCPort,
                "]",
                "}"
        };

        // Regular tls for first connection, then no ip for second
        try ( NatsTestServer ts = new NatsTestServer("src/test/resources/tls_noip.conf", tsInserts, tsPort, false);
              NatsTestServer ts2 = new NatsTestServer("src/test/resources/tls_noip.conf", ts2Inserts, ts2Port, false) ) {

            SslTestingHelper.setKeystoreSystemParameters();
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    secure().
                    connectionListener(listener).
                    maxReconnects(20). // we get multiples for some, so need enough
                    reconnectWait(Duration.ofMillis(100)).
                    connectionTimeout(Duration.ofSeconds(5)).
                    noRandomize().
                    build();

            listener.prepForStatusChange(Events.DISCOVERED_SERVERS);
            nc = (NatsConnection) longConnectionWait(options);
            assertEquals(nc.getConnectedUrl(), ts.getURI());

            flushAndWaitLong(nc, listener); // make sure we get the new server via info

            listener.prepForStatusChange(Events.RECONNECTED);

            ts.close();
            flushAndWaitLong(nc, listener);
            assertConnected(nc);

            URI uri = options.createURIForServer(nc.getConnectedUrl());
            assertEquals(ts2.getPort(), uri.getPort()); // full uri will have some ip address, just check port
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testURISchemeNoIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        SslTestingHelper.setKeystoreSystemParameters();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls_noip.conf", false)) {
            Options options = new Options.Builder()
                .server("tls://localhost:"+ts.getPort())
                .connectionTimeout(Duration.ofSeconds(5))
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testURISchemeNoIPOpenTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        SslTestingHelper.setKeystoreSystemParameters();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls_noip.conf", false)) {
            Options options = new Options.Builder().
                                server("opentls://localhost:"+ts.getPort()).
                                maxReconnects(0).
                                build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testWriterFilterTiming() throws Exception {
        NatsConnection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();

        try (NatsTestServer ts = new NatsTestServer(port, false)) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    noReconnect().
                    connectionListener(listener).
                    build();

            nc = (NatsConnection) Nats.connect(options);
            assertConnected(nc);

            for (int i = 0; i < 100; i++) {
                // stop and start in a loop without waiting for the future to complete
                nc.getWriter().stop();
                nc.getWriter().start(nc.getDataPortFuture());
            }

            nc.getWriter().stop();
            sleep(1000);
            // Should have thrown an exception if #203 isn't fixed
            standardCloseConnection(nc);
        }
    }

    private static class TestReconnectWaitHandler implements ConnectionListener {
        AtomicInteger disconnectCount = new AtomicInteger();

        public int getDisconnectCount() {
            return disconnectCount.get();
        }

        private void incrementDisconnectedCount() {
            disconnectCount.incrementAndGet();
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
            if (type == Events.DISCONNECTED) {
                // disconnect is called after every failed reconnect attempt.
                incrementDisconnectedCount();
            }
        }
    }

    @Test
    public void testReconnectWait() throws Exception {
        TestReconnectWaitHandler trwh = new TestReconnectWaitHandler();

        int port = NatsTestServer.nextPort();
        Options options = new Options.Builder().
            server("nats://localhost:"+port).
            maxReconnects(-1).
            connectionTimeout(Duration.ofSeconds(1)).
            reconnectWait(Duration.ofMillis(250)).
            connectionListener(trwh).
            build();

        NatsTestServer ts = new NatsTestServer(port, false);
        Connection c = Nats.connect(options);
        ts.close();

        sleep(250);
        assertTrue(trwh.getDisconnectCount() < 3, "disconnectCount");

        c.close();
    }

    @Test
    public void testReconnectOnConnect() throws Exception {
        int port = NatsTestServer.nextPort();
        Options options = Options.builder().server(getNatsLocalhostUri(port)).build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Connection> testConn = new AtomicReference<>();

        Thread t = getReconnectOnConnectTestThread(testConn, port, latch);

        try {
            testConn.set(Nats.connectReconnectOnConnect(options));
            latch.countDown();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        t.join(5000);
    }

    private static Thread getReconnectOnConnectTestThread(AtomicReference<Connection> testConn, int port, CountDownLatch latch) {
        Thread t = new Thread(() -> {
            assertNull(testConn.get());
            try {
                Thread.sleep(2000); // give testConn time to be alive and trying.
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertNull(testConn.get());

            // start a server that test conn can connect to
            try (NatsTestServer ignored = new NatsTestServer(port, false)) {
                //noinspection ResultOfMethodCallIgnored
                latch.await(2000, TimeUnit.MILLISECONDS);
                assertSame(Connection.Status.CONNECTED, testConn.get().getStatus());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        return t;
    }

    @Test
    public void testForceReconnectOptionsBuilder() throws Exception {
        ForceReconnectOptions fro = ForceReconnectOptions.builder().build();
        assertFalse(fro.isForceClose());
        assertFalse(fro.isFlush());
        assertNull(fro.getFlushWait());

        fro = ForceReconnectOptions.builder().forceClose().build();
        assertTrue(fro.isForceClose());
        assertFalse(fro.isFlush());
        assertNull(fro.getFlushWait());

        fro = ForceReconnectOptions.builder().flush(42).build();
        assertFalse(fro.isForceClose());
        assertTrue(fro.isFlush());
        assertNotNull(fro.getFlushWait());
        assertEquals(42, fro.getFlushWait().toMillis());

        fro = ForceReconnectOptions.builder().flush(Duration.ofMillis(42)).build();
        assertFalse(fro.isForceClose());
        assertTrue(fro.isFlush());
        assertNotNull(fro.getFlushWait());
        assertEquals(42, fro.getFlushWait().toMillis());

        fro = ForceReconnectOptions.builder().flush(null).build();
        assertFalse(fro.isForceClose());
        assertFalse(fro.isFlush());
        assertNull(fro.getFlushWait());

        fro = ForceReconnectOptions.builder().flush(-1).build();
        assertFalse(fro.isForceClose());
        assertFalse(fro.isFlush());
        assertNull(fro.getFlushWait());

        fro = ForceReconnectOptions.builder().flush(Duration.ofNanos(1)).build();
        assertFalse(fro.isForceClose());
        assertFalse(fro.isFlush());
        assertNull(fro.getFlushWait());
    }

    @Test
    public void testForceReconnect() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        ThreeServerTestOptions tstOpts = makeThreeServerTestOptions(listener, false);
        runInJsCluster(tstOpts, (nc0, nc1, nc2) -> _testForceReconnect(nc0, listener));
    }

    @Test
    public void testForceReconnectWithAccount() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        ThreeServerTestOptions tstOpts = makeThreeServerTestOptions(listener, true);
        runInJsCluster(tstOpts, (nc0, nc1, nc2) -> _testForceReconnect(nc0, listener));
    }

    private static void _testForceReconnect(Connection nc0, ListenerForTesting listener) throws IOException, InterruptedException {
        ServerInfo si = nc0.getServerInfo();
        String connectedServer = si.getServerId();

        nc0.forceReconnect();
        standardConnectionWait(nc0);

        si = nc0.getServerInfo();
        assertNotEquals(connectedServer, si.getServerId());
        assertTrue(listener.getConnectionEvents().contains(Events.DISCONNECTED));
        assertTrue(listener.getConnectionEvents().contains(Events.RECONNECTED));
    }

    private static ThreeServerTestOptions makeThreeServerTestOptions(ListenerForTesting listener, final boolean configureAccount) {
        return new ThreeServerTestOptions() {
            @Override
            public void append(int index, Options.Builder builder) {
                if (index == 0) {
                    builder.connectionListener(listener).ignoreDiscoveredServers().noRandomize();
                }
            }

            @Override
            public boolean configureAccount() {
                return configureAccount;
            }

            @Override
            public boolean includeAllServers() {
                return true;
            }
        };
    }

    @Test
    @Disabled("TODO FIGURE THIS OUT")
    public void testForceReconnectQueueBehaviorCheck() throws Exception {
        runInJsCluster((nc0, nc1, nc2) -> {
            if (atLeast2_9_0(nc0)) {
                int pubCount = 100_000;
                int subscribeTime = 5000;
                int flushWait = 2500;
                int port = nc0.getServerInfo().getPort();

                ForceReconnectQueueCheckDataPort.DELAY = 75;

                String subject = subject();
                ForceReconnectQueueCheckDataPort.WRITE_CHECK = "PUB " + subject;
                _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, false, 0);

                subject = subject();
                ForceReconnectQueueCheckDataPort.WRITE_CHECK = "PUB " + subject;
                _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, false, flushWait);

                subject = subject();
                ForceReconnectQueueCheckDataPort.WRITE_CHECK = "PUB " + subject;
                _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, true, 0);

                subject = subject();
                ForceReconnectQueueCheckDataPort.WRITE_CHECK = "PUB " + subject;
                _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, true, flushWait);
            }
        });
    }

    private static void _testForceReconnectQueueCheck(String subject, int pubCount, int subscribeTime, int port, boolean forceClose, int flushWait) throws InterruptedException {
        ReconnectQueueCheckSubscriber subscriber = new ReconnectQueueCheckSubscriber(subject, pubCount, port);
        Thread tsub = new Thread(subscriber);
        tsub.start();

        ForceReconnectOptions.Builder froBuilder = ForceReconnectOptions.builder();
        if (flushWait > 0) {
            froBuilder.flush(flushWait);
        }
        if (forceClose) {
            froBuilder.forceClose();
        }

        ReconnectQueueCheckConnectionListener listener = new ReconnectQueueCheckConnectionListener();

        Options options = Options.builder()
            .server(getNatsLocalhostUri(port))
            .connectionListener(listener)
            .dataPortType(ForceReconnectQueueCheckDataPort.class.getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(options)) {
            for (int x = 1; x <= pubCount; x++) {
                nc.publish(subject, (x + "").getBytes());
            }

            nc.forceReconnect(froBuilder.build());

            assertTrue(listener.latch.await(subscribeTime, TimeUnit.MILLISECONDS));

            long maxTime = subscribeTime;
            while (!subscriber.subscriberDone.get() && maxTime > 0) {
                //noinspection BusyWait
                Thread.sleep(50);
                maxTime -= 50;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        subscriber.subscriberDone.set(true);
        tsub.join();

        if (flushWait > 0) {
            assertEquals(pubCount, subscriber.lastNotSkipped);
        }
    }

    static class ReconnectQueueCheckConnectionListener implements ConnectionListener {
        public CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void connectionEvent(Connection conn, Events type) {
            if (type == Events.RECONNECTED) {
                latch.countDown();
            }
        }
    }

    static class ReconnectQueueCheckSubscriber implements Runnable {
        final AtomicBoolean subscriberDone;
        final String subject;
        final int pubCount;
        final int port;
        boolean completed;
        int lastNotSkipped;
        int firstAfterSkip;

        public ReconnectQueueCheckSubscriber(String subject, int pubCount, int port) {
            this.subscriberDone = new AtomicBoolean(false);
            this.subject = subject;
            this.pubCount = pubCount;
            this.port = port;
            lastNotSkipped = 0;
            firstAfterSkip = -1;
            completed = false;
        }

        @Override
        public void run() {
            Options options = Options.builder().server(getNatsLocalhostUri(port)).build();
            try (Connection nc = Nats.connect(options)) {
                Subscription sub = nc.subscribe(subject);
                while (!subscriberDone.get()) {
                    Message m = sub.nextMessage(100);
                    if (m != null) {
                        String next = "" + (lastNotSkipped + 1);
                        String md = new String(m.getData());
                        if (md.equals(next)) {
                            if (++lastNotSkipped >= pubCount) {
                                completed = true;
                                subscriberDone.set(true);
                            }
                        }
                        else {
                            firstAfterSkip = Integer.parseInt(md);
                            subscriberDone.set(true);
                        }
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSocketDataPortTimeout() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        Options.Builder builder = Options.builder()
            .socketWriteTimeout(5000)
            .pingInterval(Duration.ofSeconds(1))
            .maxMessagesInOutgoingQueue(100)
            .dataPortType(SocketDataPortBlockSimulator.class.getCanonicalName())
            .connectionListener(listener)
            .errorListener(listener);

        AtomicBoolean gotOutputQueueIsFull = new AtomicBoolean();
        runInJsServer(nc1 -> runInServer(nc2 -> {
            int port1 = nc1.getServerInfo().getPort();
            int port2 = nc2.getServerInfo().getPort();

            String[] servers = new String[]{
                getNatsLocalhostUri(port1),
                getNatsLocalhostUri(port2)
            };
            Connection nc = standardConnection(builder.servers(servers).build());
            String subject = subject();
            int connectedPort = nc.getServerInfo().getPort();
            AtomicInteger pubId = new AtomicInteger();
            while (pubId.get() < 50000) {
                try {
                    nc.publish(subject, ("" + pubId.incrementAndGet()).getBytes());
                    if (pubId.get() == 10) {
                        SocketDataPortBlockSimulator.SIMULATE_SOCKET_BLOCK.set(60000);
                    }
                }
                catch (Exception e) {
                    if (e.getMessage().contains(OUTPUT_QUEUE_IS_FULL)) {
                        gotOutputQueueIsFull.set(true);
                    }
                }
            }
            assertNotEquals(connectedPort, nc.getServerInfo().getPort());
            nc.close();
        }));

        assertTrue(gotOutputQueueIsFull.get());
        assertTrue(listener.getSocketWriteTimeoutCount() > 0);
        assertTrue(listener.getConnectionEvents().contains(Events.DISCONNECTED));
        assertTrue(listener.getConnectionEvents().contains(Events.RECONNECTED));
    }
}
