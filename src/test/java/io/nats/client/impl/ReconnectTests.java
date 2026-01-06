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

import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.api.ServerInfo;
import io.nats.client.support.Listener;
import io.nats.client.utils.ConnectionUtils;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.nats.client.AuthTests.getUserCredsAuthHander;
import static io.nats.client.NatsTestServer.configFileBuilder;
import static io.nats.client.support.Listener.*;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.*;
import static io.nats.client.utils.TestBase.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

@Isolated
public class ReconnectTests {

    void checkNotConnected(Connection nc) {
        Connection.Status status = nc.getStatus();
        assertTrue(Connection.Status.RECONNECTING == status || Connection.Status.DISCONNECTED == status, "Reconnecting status");
    }

    @Test
    public void testSimpleReconnect() throws Exception { //Includes test for subscriptions and dispatchers across reconnect
        _testReconnect(NatsServerRunner.builder(), (ts, optionsBuilder) -> optionsBuilder.server(ts.getServerUri()));
    }

    @Test
    public void testWsReconnect() throws Exception { //Includes test for subscriptions and dispatchers across reconnect
        _testReconnect(configFileBuilder("ws_operator.conf"),
            (ts, optionsBuilder) -> optionsBuilder.server(ts.getLocalhostUri(WS)).authHandler(getUserCredsAuthHander()));
    }

    private void _testReconnect(NatsServerRunner.Builder nsrb, BiConsumer<NatsTestServer, Options.Builder> optSetter) throws Exception {
        int port = NatsTestServer.nextPort();
        nsrb.port(port); // set the port into the builder
        Listener listener = new Listener();
        NatsConnection nc;
        Subscription sub;
        long start;
        long end;
        String subsubject = random();
        String dispatchSubject = random();
        try (NatsTestServer ts = new NatsTestServer(nsrb)) {
            Options.Builder builder = optionsBuilder() // server intentionally not set
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(1000))
                .connectionListener(listener);
            optSetter.accept(ts, builder);
            Options options = builder.build();

            nc = (NatsConnection) managedConnect(options);

            sub = nc.subscribe(subsubject);

            final NatsConnection nnc = nc; // final for the lambda
            Dispatcher d = nc.createDispatcher(msg -> nnc.publish(msg.getReplyTo(), msg.getData()) );
            d.subscribe(dispatchSubject);
            flushConnection(nc);

            Future<Message> inc = nc.request(dispatchSubject, "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            nc.publish(subsubject, null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);

            listener.queueConnectionEvent(Events.DISCONNECTED);
            start = System.nanoTime();
        }

        flushConnection(nc);
        listener.validate();

        listener.queueConnectionEvent(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(nsrb)) {
            confirmConnected(nc); // wait for reconnect
            listener.validate();

            end = System.nanoTime();

            assertTrue(1_000_000 * (end-start) > 1000, "reconnect wait");

            // Make sure dispatcher and subscription are still there
            Future<Message> inc = nc.request(dispatchSubject, "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);

            // make sure the subscription survived
            nc.publish(subsubject, null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
        }

        assertEquals(1, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        assertTrue(nc.getStatisticsCollector().getExceptions() > 0, "exception count");
        closeAndConfirm(nc);
    }

    @Test
    public void testSubscribeDuringReconnect() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        int port;
        Subscription sub;

        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(20))
                .connectionListener(listener)
                .build();
            port = ts.getPort();
            nc = (NatsConnection) managedConnect(options);
            listener.queueConnectionEvent(Events.DISCONNECTED);
        }

        flushConnection(nc);
        listener.validate();

        String subsubject = random();
        String dispatchSubject = random();
        sub = nc.subscribe(subsubject);

        final NatsConnection nnc = nc;
        Dispatcher d = nc.createDispatcher(msg -> nnc.publish(msg.getReplyTo(), msg.getData()));
        d.subscribe(dispatchSubject);

        listener.queueConnectionEvent(Events.RECONNECTED);

        try (NatsTestServer ignored = new NatsTestServer(port)) {
            confirmConnected(nc); // wait for reconnect
            listener.validate();

            // Make sure the dispatcher and subscription are still there
            Future<Message> inc = nc.request(dispatchSubject, "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            // make sure the subscription survived
            nc.publish(subsubject, null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
        }

        assertEquals(1, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        assertTrue(nc.getStatisticsCollector().getExceptions() > 0, "exception count");
        closeAndConfirm(nc);
    }

    @Test
    public void testReconnectBuffer() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        int port = NatsTestServer.nextPort();
        Subscription sub;
        long start;
        long end;
        String[] customArgs = {"--user","stephen","--pass","password"};
        String subsubject = random();
        String dispatchSubject = random();

        try (NatsTestServer ts = new NatsTestServer(customArgs, port)) {
            Options options = optionsBuilder(ts)
                .maxReconnects(-1)
                .userInfo("stephen".toCharArray(), "password".toCharArray())
                .reconnectWait(Duration.ofMillis(1000))
                .connectionListener(listener)
                .build();
            nc = (NatsConnection) managedConnect(options);

            sub = nc.subscribe(subsubject);

            final NatsConnection nnc = nc;
            Dispatcher d = nc.createDispatcher(msg -> nnc.publish(msg.getReplyTo(), msg.getData()));
            d.subscribe(dispatchSubject);
            nc.flush(Duration.ofMillis(1000));

            Future<Message> inc = nc.request(dispatchSubject, "test".getBytes(StandardCharsets.UTF_8));
            Message msg = inc.get();
            assertNotNull(msg);

            nc.publish(subsubject, null);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);

            listener.queueConnectionEvent(Events.DISCONNECTED);
            start = System.nanoTime();
        }

        flushConnection(nc);
        listener.validate();

        // Send a message to the dispatcher and one to the subscriber
        // These should be sent on reconnect
        Future<Message> inc = nc.request(dispatchSubject, "test".getBytes(StandardCharsets.UTF_8));
        nc.publish(subsubject, null);
        nc.publish(subsubject, null);

        listener.queueConnectionEvent(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port)) {
            confirmConnected(nc); // wait for reconnect
            listener.validate();

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
        closeAndConfirm(nc);
    }

    @Test
    public void testMaxReconnects() throws Exception {
        Connection nc;
        Listener listener = new Listener();
        int port = NatsTestServer.nextPort();

        try (NatsTestServer ts = new NatsTestServer(port)) {
            Options options = optionsBuilder(ts)
                .maxReconnects(1)
                .connectionListener(listener)
                .reconnectWait(Duration.ofMillis(10))
                .build();
            nc = managedConnect(options);
            listener.queueConnectionEvent(Events.CLOSED);
        }
        flushConnection(nc);
        listener.validate();
    }

    @Test
    public void testReconnectToSecondServerInBootstrap() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        try (NatsTestServer ts1 = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                // need both in bootstrap b/c these are not clustered
                Options options = optionsBuilder(ts2.getServerUri(), ts1.getServerUri())
                    .noRandomize()
                    .connectionListener(listener)
                    .maxReconnects(-1)
                    .build();
                nc = (NatsConnection) managedConnect(options);
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
                listener.queueConnectionEvent(Events.RECONNECTED);
            }

            flushConnection(nc);
            listener.validate();
            assertConnected(nc);
            assertEquals(ts1.getServerUri(), nc.getConnectedUrl());
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testNoRandomizeReconnectToSecondServer() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = optionsBuilder(ts2.getServerUri(), ts.getServerUri())
                    .noRandomize()
                    .connectionListener(listener)
                    .maxReconnects(-1)
                    .build();
                nc = (NatsConnection) managedConnect(options);
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
                listener.queueConnectionEvent(Events.RECONNECTED);
            }

            flushConnection(nc);
            listener.validate();
            assertConnected(nc);
            assertEquals(ts.getServerUri(), nc.getConnectedUrl());
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testReconnectToSecondServerFromInfo() throws Exception {
        Listener listener = new Listener();
        runInSharedServer(ts -> {
            Connection nc;
            String striped = ts.getServerUri().substring("nats://".length()); // info doesn't have protocol
            String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\",\"connect_urls\": [\""+striped+"\"]}";
            try (NatsServerProtocolMock mockTs2 = new NatsServerProtocolMock(null, customInfo)) {
                Options options = optionsBuilder(mockTs2)
                    .connectionListener(listener)
                    .maxReconnects(-1)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .reconnectWait(Duration.ofSeconds(1))
                    .build();
                nc = standardConnect(options);
                assertEquals(mockTs2.getServerUri(), nc.getConnectedUrl());
                listener.queueConnectionEvent(Events.RECONNECTED);
            }

            flushConnection(nc);
            listener.validate();
            assertConnected(nc);
            assertEquals(ts.getServerUri(), nc.getConnectedUrl());
            closeAndConfirm(nc);
        });
    }

    @Test
    public void testOverflowReconnectBuffer() throws Exception {
        Connection nc;
        Listener listener = new Listener();
        listener.queueConnectionEvent(Events.DISCONNECTED);
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .connectionListener(listener)
                .reconnectBufferSize(4*512)
                .reconnectWait(Duration.ofSeconds(480))
                .build();
            nc = managedConnect(options);
        }

        listener.validate();

        String subject = random();
        assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < 20; i++) {
                nc.publish(subject, new byte[512]);// Should be full by the 5th message
            }
        });

        closeAndConfirm(nc);
    }

    @Test
    public void testInfiniteReconnectBuffer() throws Exception {
        Connection nc;
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .maxReconnects(5)
                .connectionListener(listener)
                .reconnectBufferSize(-1)
                .reconnectWait(Duration.ofSeconds(30))
                .build();
            nc = managedConnect(options);
            listener.queueConnectionEvent(Events.DISCONNECTED);
        }

        flushConnection(nc);
        listener.validate();

        byte[] payload = new byte[1024];
        for (int i=0;i<1_000;i++) {
            nc.publish("test", payload);
        }

        checkNotConnected(nc);
        closeAndConfirm(nc);
    }

    @Test
    public void testReconnectDropOnLineFeed() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        int port = NatsTestServer.nextPort();
        Duration reconnectWait = Duration.ofMillis(100); // thrash
        int thrashCount = 5;
        CompletableFuture<Boolean> gotSub = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> subRef = new AtomicReference<>(gotSub);
        CompletableFuture<Boolean> sendMsg = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> sendRef = new AtomicReference<>(sendMsg);

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String subLine;

            // System.out.println("*** Mock Server @" + ts.getPort() + " waiting for SUB ...");
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

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
            Options options = optionsBuilder(mockTs)
                .maxReconnects(-1)
                .reconnectWait(reconnectWait)
                .connectionListener(listener)
                .build();
            port = mockTs.getPort();
            nc = (NatsConnection) standardConnect(options);
            listener.queueConnectionEvent(Events.DISCONNECTED);
            nc.subscribe("test");
            subRef.get().get();
            sendRef.get().complete(true);
            flushConnection(nc); // mock server will close so we do this inside the curly
            listener.validate();
        }

        // Thrash in and out of connect status
        // server starts thrashCount times, so we should succeed thrashCount x
        for (int i=0;i<thrashCount;i++) {
            checkNotConnected(nc);

            // connect good then bad
            listener.queueConnectionEvent(Events.RESUBSCRIBED);
            try (NatsTestServer ignored = new NatsTestServer(port)) {
                confirmConnected(nc); // wait for reconnect
                listener.validate();
                listener.queueConnectionEvent(Events.DISCONNECTED); // do it here because we are about to disconnect
            }

            flushConnection(nc); // client won't close until we tell it, so put this outside the curly
            listener.validate();

            gotSub = new CompletableFuture<>();
            subRef.set(gotSub);
            sendMsg = new CompletableFuture<>();
            sendRef.set(sendMsg);

            listener.queueConnectionEvent(Events.RESUBSCRIBED);
            try (NatsServerProtocolMock ignored = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
                confirmConnected(nc); // wait for reconnect
                listener.validate();
                subRef.get().get();
                listener.queueConnectionEvent(Events.DISCONNECTED);
                sendRef.get().complete(true);
                flushConnection(nc); // mock server will close so we do this inside the curly
                listener.validate();
            }
        }

        assertEquals(2 * thrashCount, nc.getStatisticsCollector().getReconnects(), "reconnect count");
        closeAndConfirm(nc);
    }

    @Test
    public void testTlsNoIpConnection() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();

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

        SslTestingHelper.setKeystoreSystemParameters();

        // Regular tls for first connection, then no ip for second
        try (NatsTestServer ts = new NatsTestServer( "tls_noip.conf", tsInserts, tsPort);
             NatsTestServer ts2 = new NatsTestServer( "tls_noip.conf", ts2Inserts, ts2Port) ) {

            // Test 1. tls Scheme
            Options options = optionsBuilder(ts, "tls")
                .connectionTimeout(Duration.ofSeconds(5))
                .maxReconnects(0)
                .build();
            assertCanConnect(options);

            // Test 2. opentls Scheme
            options = optionsBuilder(ts, "opentls")
                .maxReconnects(0)
                .build();
            assertCanConnect(options);

            // Test 3. Reconnect
            options = optionsBuilder(ts)
                .secure()
                .connectionListener(listener)
                .maxReconnects(20)
                .reconnectWait(Duration.ofMillis(100))
                .connectionTimeout(Duration.ofSeconds(5))
                .noRandomize()
                .build();

            listener.queueConnectionEvent(Events.DISCOVERED_SERVERS);
            nc = (NatsConnection) ConnectionUtils.managedConnect(options);
            assertEquals(ts.getServerUri(), nc.getConnectedUrl());

            flushConnection(nc); // make sure we get the new server via info
            listener.validate();

            listener.queueConnectionEvent(Events.RECONNECTED, VERY_LONG_VALIDATE_TIMEOUT);

            ts.close();

            flushConnection(nc);

            listener.validate();

            URI uri = options.createURIForServer(nc.getConnectedUrl());
            assertEquals(ts2.getPort(), uri.getPort()); // full uri will have some ip address, just check port
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testWriterFilterTiming() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        int port = NatsTestServer.nextPort();

        try (NatsTestServer ts = new NatsTestServer(port)) {
            Options options = optionsBuilder(ts)
                .noReconnect()
                .connectionListener(listener)
                .build();

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
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testReconnectWait() throws Exception {
        Listener listener = new Listener();

        int port = NatsTestServer.nextPort();

        try (NatsTestServer ts = new NatsTestServer(port)) {
            Options options = optionsBuilder(ts)
                .maxReconnects(-1)
                .connectionTimeout(Duration.ofSeconds(1))
                .reconnectWait(Duration.ofMillis(250))
                .connectionListener(listener)
                .build();

            //noinspection unused
            try (Connection nc = Nats.connect(options)) {
                ts.close();
                sleep(250);
                assertTrue(listener.getConnectionEventCount(Events.DISCONNECTED) < 3, "disconnectCount");
            }
        }
    }

    @Test
    public void testReconnectOnConnect() throws Exception {
        int port = NatsTestServer.nextPort();
        Options options = options(port);

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
            try (NatsTestServer ignored = new NatsTestServer(port)) {
                //noinspection ResultOfMethodCallIgnored
                latch.await(2000, TimeUnit.MILLISECONDS);
                assertConnected(testConn.get());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        return t;
    }

    @Test
    public void testForceReconnectOptionsBuilder() {
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
        Listener listener = new Listener();
        ThreeServerTestOptions tstOpts = makeThreeServerTestOptions(listener, false);
        runInCluster(tstOpts, (nc0, nc1, nc2) -> _testForceReconnect(nc0, listener));
    }

    @Test
    public void testForceReconnectWithAccount() throws Exception {
        Listener listener = new Listener();
        ThreeServerTestOptions tstOpts = makeThreeServerTestOptions(listener, true);
        runInCluster(tstOpts, (nc0, nc1, nc2) -> _testForceReconnect(nc0, listener));
    }

    private static void _testForceReconnect(Connection nc0, Listener listener) throws IOException, InterruptedException {
        ServerInfo si = nc0.getServerInfo();
        String connectedServer = si.getServerId();

        listener.queueConnectionEvent(Events.DISCONNECTED);
        listener.queueConnectionEvent(Events.RECONNECTED);
        nc0.forceReconnect();
        confirmConnected(nc0); // wait for reconnect

        si = nc0.getServerInfo();
        assertNotEquals(connectedServer, si.getServerId());
        listener.validateAll();
    }

    private static ThreeServerTestOptions makeThreeServerTestOptions(Listener listener, final boolean configureAccount) {
        return new ThreeServerTestOptions() {
            @Override
            public void append(int index, Options.Builder builder) {
                if (index == 0) {
                    builder
                        .connectionListener(listener)
                        .errorListener(NOOP_EL)
                        .ignoreDiscoveredServers()
                        .noRandomize();
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
    public void testForceReconnectQueueBehaviorCheck() throws Exception {
        runInCluster((nc0, nc1, nc2) -> {
            int pubCount = 100_000;
            int subscribeTime = 5000;
            int flushWait = 2500;
            int port = nc0.getServerInfo().getPort();

            ForceReconnectQueueCheckDataPort.DELAY = 75;

            String subject = random();
            ForceReconnectQueueCheckDataPort.setCheck("PUB " + subject);
            _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, false, 0);

            subject = random();
            ForceReconnectQueueCheckDataPort.setCheck("PUB " + subject);
            _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, false, flushWait);

            subject = random();
            ForceReconnectQueueCheckDataPort.setCheck("PUB " + subject);
            _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, true, 0);

            subject = random();
            ForceReconnectQueueCheckDataPort.setCheck("PUB " + subject);
            _testForceReconnectQueueCheck(subject, pubCount, subscribeTime, port, true, flushWait);
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

        Listener listener = new Listener();

        Options options = optionsBuilder(port)
            .connectionListener(listener)
            .dataPortType(ForceReconnectQueueCheckDataPort.class.getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(options)) {
            for (int x = 1; x <= pubCount; x++) {
                nc.publish(subject, (x + "").getBytes());
            }

            listener.queueConnectionEvent(Events.RECONNECTED);
            nc.forceReconnect(froBuilder.build());

            listener.validate();

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
            Options options = options(port);
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
        Listener listener = new Listener();
        Options.Builder builder = Options.builder()
            .noRandomize()
            .socketWriteTimeout(5000) // long time vital! to get OUTPUT_QUEUE_IS_FULL
            .pingInterval(Duration.ofSeconds(1))
            .maxMessagesInOutgoingQueue(100)
            .dataPortType(SocketDataPortBlockSimulator.class.getCanonicalName())
            .connectionListener(listener)
            .errorListener(listener);

        // 1. The socket port gets blocked, the queue fills up before the socketWriteTimeout()
        // 2. The socket write times out
        // 3. That causes a disconnect
        // 4. Eventually reconnected. Give it a long timeout, it takes a while on GH machine
        listener.queueException(IllegalStateException.class, "Output queue", MEDIUM_VALIDATE_TIMEOUT);
        listener.queueSocketWriteTimeout(LONG_VALIDATE_TIMEOUT);
        listener.queueConnectionEvent(Events.DISCONNECTED);
        listener.queueConnectionEvent(Events.RECONNECTED, MEDIUM_VALIDATE_TIMEOUT);

        try (NatsTestServer ts1 = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                String[] servers = new String[]{
                    ts1.getNatsLocalhostUri(),
                    ts2.getNatsLocalhostUri()
                };
                try (Connection nc = standardConnect(builder.servers(servers).build())) {
                    int connectedPort = nc.getServerInfo().getPort();

                    String subject = random();
                    int pubId = 0;
                    while (pubId++ < 1000) {
                        if (pubId == 10) {
                            SocketDataPortBlockSimulator.simulateBlock();
                        }
                        try {
                            nc.publish(subject, null);
                        }
                        catch (IllegalStateException e) {
                            if (e.getMessage().contains("Output queue")) {
                                break;
                            }
                        }
                    }
                    listener.validateAll();
                    assertNotEquals(connectedPort, nc.getServerInfo().getPort());
                }
            }
        }
    }
}
