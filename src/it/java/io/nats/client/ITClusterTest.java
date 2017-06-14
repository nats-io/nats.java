/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.ConnState.RECONNECTING;
import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runServerOnPort;
import static io.nats.client.UnitTestUtilities.runServerWithConfig;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Category(IntegrationTest.class)
public class ITClusterTest {

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    private static final String[] testServers = new String[] {"nats://localhost:1222",
            "nats://localhost:1223", "nats://localhost:1224", "nats://localhost:1225",
            "nats://localhost:1226", "nats://localhost:1227", "nats://localhost:1228"};

    static final String servers = StringUtils.join(testServers, ",");

    static final String[] testServersShortList =
            new String[] {"nats://localhost:1222", "nats://localhost:1223"};

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testServersOption() throws Exception {

        Options opts = new Options.Builder(defaultOptions())
                .dontRandomize()
                .build();
        opts.servers = Nats.processUrlArray(testServers);

        // Make sure we can connect to first server if running
        try (NatsServer ns = runServerOnPort(1222)) {
            try (Connection c = opts.connect()) {
                assertTrue(String.format("%s != %s", testServers[0], c.getConnectedUrl()),
                        testServers[0].equals(c.getConnectedUrl()));
            }
        }

        // make sure we can connect to a non-first server.
        try (NatsServer ns = runServerOnPort(1227)) {
            try (Connection c = opts.connect()) {
                assertTrue(testServers[5] + " != " + c.getConnectedUrl(),
                        testServers[5].equals(c.getConnectedUrl()));
            }
        }
    }

    @Test
    public void testAuthServers() throws Exception {
        String[] plainServers = new String[] {"nats://localhost:1222", "nats://localhost:1224"};

        Options opts = new Options.Builder(Nats.defaultOptions())
                .dontRandomize()
                .timeout(5, TimeUnit.SECONDS).build();
        opts.servers = Nats.processUrlArray(plainServers);

        try (NatsServer as1 = runServerWithConfig("auth_1222.conf")) {
            try (NatsServer as2 = runServerWithConfig("auth_1224.conf")) {
                boolean exThrown = false;
                try (Connection c = opts.connect()) {
                    fail("Expect Auth failure, got no error");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertNotNull(e.getMessage());
                    assertEquals(Nats.ERR_AUTHORIZATION, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expect Auth failure, got no error", exThrown);

                // Test that we can connect to a subsequent correct server.
                String[] authServers = new String[] {"nats://localhost:1222",
                        "nats://username:password@localhost:1224"};

                opts = defaultOptions();
                opts.servers = Nats.processUrlArray(authServers);

                try (Connection c = opts.connect()) {
                    assertTrue(c.getConnectedUrl().equals(authServers[1]));
                } catch (IOException e) {
                    if (e.getMessage().contains("Authorization")) {
                        // ignore Authorization Failure
                    } else {
                        fail("Expected to connect properly: " + e.getMessage());
                    }
                }
            } // as2
        } // as1
    }

    @Test
    public void testBasicClusterReconnect() throws Exception {
        try (NatsServer s1 = runServerOnPort(1222)) {
            try (NatsServer s2 = runServerOnPort(1224)) {


                Options opts = new Options.Builder(Nats.defaultOptions())
                        .dontRandomize()
                        .build();

                final AtomicBoolean dcbCalled = new AtomicBoolean(false);
                final CountDownLatch dcLatch = new CountDownLatch(1);
                opts.disconnectedCb = new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        // Suppress any additional calls
                        if (dcbCalled.get()) {
                            return;
                        }
                        dcbCalled.set(true);
                        dcLatch.countDown();
                    }
                };

                final CountDownLatch rcLatch = new CountDownLatch(1);
                opts.reconnectedCb = new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rcLatch.countDown();
                    }
                };

                try (Connection c = Nats.connect(servers, opts)) {
                    assertNotNull(c.getConnectedUrl());

                    s1.shutdown();

                    // wait for disconnect
                    assertTrue("Did not receive a disconnect callback message",
                            await(dcLatch, 2, TimeUnit.SECONDS));

                    long reconnectTimeStart = System.nanoTime();

                    assertTrue("Did not receive a reconnect callback message: ",
                            await(rcLatch, 2, TimeUnit.SECONDS));

                    assertTrue(c.getConnectedUrl().equals(testServers[2]));

                    // Make sure we did not wait on reconnect for default time.
                    // Reconnect should be fast since it will be a switch to the
                    // second server and not be dependent on server restart time.
                    // assertTrue(reconElapsed.get() <= cf.getReconnectWait());

                    long maxDuration = 100;
                    long reconnectTime = System.nanoTime() - reconnectTimeStart;
                    assertFalse(
                            String.format("Took longer than expected to reconnect: %dms\n",
                                    TimeUnit.NANOSECONDS.toMillis(reconnectTime)),
                            TimeUnit.NANOSECONDS.toMillis(reconnectTime) > maxDuration);
                }
            }
        }
    }


    @Test
    public void testHotSpotReconnect() throws InterruptedException {
        int numClients = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numClients,
                new NatsThreadFactory("testhotspotreconnect"));

        final BlockingQueue<String> rch = new LinkedBlockingQueue<String>();
        final BlockingQueue<Integer> dch = new LinkedBlockingQueue<Integer>();
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        try (NatsServer s1 = runServerOnPort(1222)) {
            try (NatsServer s2 = runServerOnPort(1224)) {
                try (NatsServer s3 = runServerOnPort(1226)) {

                    final class NATSClient implements Runnable {
                        Connection nc = null;
                        final AtomicInteger numReconnects = new AtomicInteger(0);
                        final AtomicInteger numDisconnects = new AtomicInteger(0);
                        String currentUrl = null;
                        final AtomicInteger instance = new AtomicInteger(-1);

                        final Options opts;

                        NATSClient(int inst) {
                            this.instance.set(inst);
                            opts = defaultOptions();
                            opts.servers = Nats.processUrlArray(testServers);

                            opts.disconnectedCb = new DisconnectedCallback() {
                                public void onDisconnect(ConnectionEvent event) {
                                    numDisconnects.incrementAndGet();
                                    try {
                                        dch.put(instance.get());
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    nc.setDisconnectedCallback(null);
                                }
                            };
                            opts.reconnectedCb = new ReconnectedCallback() {
                                public void onReconnect(ConnectionEvent event) {
                                    numReconnects.incrementAndGet();
                                    currentUrl = nc.getConnectedUrl();
                                    try {
                                        rch.put(currentUrl);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            };
                        }

                        @Override
                        public void run() {
                            try {
                                nc = opts.connect();
                                assertTrue(!nc.isClosed());
                                assertNotNull(nc.getConnectedUrl());
                                currentUrl = nc.getConnectedUrl();
                                // System.err.println("Instance " + instance + " connected to " +
                                // currentUrl);
                                while (!shutdown.get()) {
                                    sleep(10);
                                }
                                nc.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        public synchronized boolean isConnected() {
                            return (nc != null && !nc.isClosed());
                        }

                        public void shutdown() {
                            shutdown.set(true);
                        }
                    }

                    List<NATSClient> tasks = new ArrayList<NATSClient>(numClients);
                    for (int i = 0; i < numClients; i++) {
                        NATSClient task = new NATSClient(i);
                        tasks.add(task);
                        executor.submit(task);
                    }

                    Map<String, Integer> cs = new HashMap<String, Integer>();

                    int numReady = 0;
                    while (numReady < numClients) {
                        numReady = 0;
                        for (NATSClient cli : tasks) {
                            if (cli.isConnected()) {
                                numReady++;
                            }
                        }
                        sleep(100);
                    }

                    s1.shutdown();
                    sleep(1000);

                    int disconnected = 0;
                    // wait for disconnects
                    while (dch.size() > 0 && disconnected < numClients) {
                        Integer instance = -1;
                        instance = dch.poll(5, TimeUnit.SECONDS);
                        assertNotNull("timed out waiting for disconnect signal", instance);
                        disconnected++;
                    }
                    assertTrue(disconnected > 0);


                    int reconnected = 0;
                    // wait for reconnects
                    for (int i = 0; i < disconnected; i++) {
                        String url = null;
                        while (rch.size() == 0) {
                            sleep(50);
                        }
                        url = rch.poll(5, TimeUnit.SECONDS);
                        assertNotNull("timed out waiting for reconnect signal", url);
                        reconnected++;
                        Integer count = cs.get(url);
                        if (count != null) {
                            cs.put(url, ++count);
                        } else {
                            cs.put(url, 1);
                        }
                    }

                    for (NATSClient client : tasks) {
                        client.shutdown();
                    }
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));

                    assertEquals(disconnected, reconnected);

                    int numServers = 2;

                    assertEquals(numServers, cs.size());

                    int expected = numClients / numServers;
                    // We expect a 40 percent variance
                    int var = (int) ((float) expected * 0.40);

                    int delta = Math.abs(cs.get(testServers[2]) - cs.get(testServers[4]));
                    // System.err.printf("var = %d, delta = %d\n", var, delta);
                    if (delta > var) {
                        String str = String.format(
                                "Connected clients to servers out of range: %d/%d", delta, var);
                        fail(str);
                    }
                }
            }
        }
    }

    @Test
    public void testProperReconnectDelay() throws Exception {
        try (NatsServer s1 = runServerOnPort(1222)) {
            Options opts = new Options.Builder(defaultOptions())
                    .dontRandomize()
                    .build();
            opts.servers = Nats.processUrlArray(testServers);

            final CountDownLatch latch = new CountDownLatch(1);
            opts.disconnectedCb = new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    event.getConnection().setDisconnectedCallback(null);
                    latch.countDown();
                }
            };

            final AtomicBoolean ccbCalled = new AtomicBoolean(false);
            opts.closedCb = new ClosedCallback() {
                public void onClose(ConnectionEvent event) {
                    ccbCalled.set(true);
                }
            };

            try (Connection c = opts.connect()) {
                assertFalse(c.isClosed());

                s1.shutdown();
                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(latch, 2, TimeUnit.SECONDS));

                // Wait, want to make sure we don't spin on reconnect to non-existent servers.
                sleep(1, TimeUnit.SECONDS);

                assertFalse("Closed CB was triggered, should not have been.", ccbCalled.get());
                assertEquals("Wrong state: " + c.getState(), c.getState(), RECONNECTING);
            }
        }
    }

    @Test
    public void testProperFalloutAfterMaxAttempts() throws Exception {
        Options opts = new Options.Builder(defaultOptions())
                .dontRandomize()
                .maxReconnect(5)
                .reconnectWait(25, TimeUnit.MILLISECONDS)
                .build();
        opts.servers = Nats.processUrlArray(testServers);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        opts.disconnectedCb = new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        };

        final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
        final CountDownLatch ccLatch = new CountDownLatch(1);
        opts.closedCb = new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                closedCbCalled.set(true);
                ccLatch.countDown();
            }
        };

        try (NatsServer s1 = runServerOnPort(1222)) {
            try (Connection c = opts.connect()) {
                s1.shutdown();

                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(dcLatch, 2, TimeUnit.SECONDS));

                // Wait for ClosedCB
                assertTrue("Did not receive a closed callback message",
                        await(ccLatch, 2, TimeUnit.SECONDS));

                // Make sure we are not still reconnecting.
                assertTrue("Closed CB was not triggered, should have been.", closedCbCalled.get());

                // Expect connection to be closed...
                assertTrue("Wrong status: " + c.getState(), c.isClosed());
            }
        }
    }

    @Test
    public void testProperFalloutAfterMaxAttemptsWithAuthMismatch()
            throws Exception {
        final String[] myServers = {"nats://localhost:1222", "nats://localhost:4443"};

        Options opts = new Options.Builder(defaultOptions())
                .dontRandomize()
                .maxReconnect(5)
                .reconnectWait(25, TimeUnit.MILLISECONDS)
                .build();
        opts.servers = Nats.processUrlArray(myServers);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        opts.disconnectedCb = new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        };

        final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
        final CountDownLatch ccLatch = new CountDownLatch(1);
        opts.closedCb = new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                closedCbCalled.set(true);
                ccLatch.countDown();
            }
        };

        try (NatsServer s1 = runServerOnPort(1222)) {
            try (NatsServer s2 = runServerWithConfig("tlsverify.conf")) {
                try (ConnectionImpl nc = (ConnectionImpl) opts.connect()) {
                    s1.shutdown();

                    // wait for disconnect
                    assertTrue("Did not receive a disconnect callback message",
                            await(dcLatch, 5, TimeUnit.SECONDS));

                    // Wait for ClosedCB
                    assertTrue("Did not receive a closed callback message",
                            await(ccLatch, 5, TimeUnit.SECONDS));

                    // Make sure we are not still reconnecting.
                    assertTrue("Closed CB was not triggered, should have been.",
                            closedCbCalled.get());

                    // Make sure we have not exceeded MaxReconnect
                    assertEquals("Wrong number of reconnects", nc.getOptions().getMaxReconnect(),
                            nc.getStats().getReconnects());

                    // Make sure we are not still reconnecting
                    assertTrue("Closed CB was not triggered, should have been.",
                            closedCbCalled.get());

                    // Expect connection to be closed...
                    assertTrue("Wrong status: " + nc.getState(), nc.isClosed());
                }
            }
        }
    }

    @Test
    public void testTimeoutOnNoServers() throws Exception {
        int maxRecon = 10;
        int reconWait = 100;
        String[] servers = testServers;
        final String hostOs = System.getProperty("os.name").toLowerCase();
        boolean windows = (hostOs.contains("win"));
        windows = true;
        if (windows) {
            servers = Arrays.copyOf(testServers, 2);
            maxRecon = 2;
        }

        Options opts = new Options.Builder(defaultOptions())
                .dontRandomize()
                // 1 second total time wait
                .maxReconnect(maxRecon)
                .reconnectWait(reconWait)
                .build();
        opts.servers = Nats.processUrlArray(servers);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        opts.disconnectedCb = new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent ev) {
                ev.getConnection().setDisconnectedCallback(null);
                dcLatch.countDown();
            }
        };

        final CountDownLatch ccLatch = new CountDownLatch(1);
        opts.closedCb = new ClosedCallback() {
            public void onClose(ConnectionEvent ev) {
                ccLatch.countDown();
            }
        };

        try (NatsServer s1 = runServerOnPort(1222)) {
            try (Connection c = opts.connect()) {
                s1.shutdown();

                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        dcLatch.await(5, TimeUnit.SECONDS));

                long t0 = System.nanoTime();

                // Wait for ClosedCB
                assertTrue("Did not receive a closed callback signal",
                        ccLatch.await(5, TimeUnit.SECONDS));

                if (windows) {
                    long elapsedMsec = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

                    // Use 500ms as variable time delta
                    long variable = 500;
                    long expected = opts.getMaxReconnect() * opts.getReconnectWait();

                    assertFalse("Waited too long for Closed state: " + elapsedMsec,
                            elapsedMsec > (expected + variable));
                }
            }
        }
    }

    /**
     * Ensures that if a ping is not ponged within the pingInterval, that a disconnect/reconnect
     * takes place.
     * <p>
     * <p>We test this by setting maxPingsOut < 0 and setting the pingInterval very small. After
     * the first
     * disconnect, we measure the reconnect-to-disconnect time to ensure it isn't greater than 2
     * * pingInterval.
     *
     * @throws Exception if anything goes wrong
     */
    @Test
    public void testPingReconnect() throws Exception {
        final int reconnects = 4;
        final AtomicInteger timesReconnected = new AtomicInteger();
//        setLogLevel(Level.DEBUG);
        try (NatsServer s1 = runServerOnPort(1222)) {
            Options opts = new Options.Builder(defaultOptions())
                    .dontRandomize()
                    .reconnectWait(200)
                    .pingInterval(50)
                    .maxPingsOut(-1)
                    .timeout(1000).build();
            opts.servers = Nats.processUrlArray(testServers);

            final CountDownLatch wg = new CountDownLatch(reconnects);
            final BlockingQueue<Long> rch = new LinkedBlockingQueue<Long>(reconnects);
            final BlockingQueue<Long> dch = new LinkedBlockingQueue<Long>(reconnects);

            opts.disconnectedCb = new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dch.add(System.nanoTime());
                }
            };

            opts.reconnectedCb = new ReconnectedCallback() {
                @Override
                public void onReconnect(ConnectionEvent event) {
                    rch.add(System.nanoTime());
                    wg.countDown();
                }
            };

            try (ConnectionImpl c = (ConnectionImpl) opts.connect()) {
                wg.await();
                s1.shutdown();

                // Throw away the first one
                dch.take();
                for (int i = 0; i < reconnects - 1; i++) {
                    Long disconnectedAt = dch.take();
                    Long reconnectedAt = rch.take();
                    Long pingCycle = TimeUnit.NANOSECONDS.toMillis(disconnectedAt - reconnectedAt);
                    assertFalse(String.format("Reconnect due to ping took %d msec", pingCycle),
                            pingCycle > 2 * c.getOptions().getPingInterval());
                }
            }
        }
    }
}
