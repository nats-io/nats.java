/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runServerOnPort;
import static io.nats.client.UnitTestUtilities.runServerWithConfig;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.Constants.ConnState;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Category(IntegrationTest.class)
public class ITClusterTest {
    static final Logger logger = LoggerFactory.getLogger(ITClusterTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        // s = util.createServerWithConfig("auth_1222.conf");
    }

    @After
    public void tearDown() throws Exception {
        // s.shutdown();
    }

    static final String[] testServers = new String[] { "nats://localhost:1222",
            "nats://localhost:1223", "nats://localhost:1224", "nats://localhost:1225",
            "nats://localhost:1226", "nats://localhost:1227", "nats://localhost:1228" };

    static final String[] testServersShortList =
            new String[] { "nats://localhost:1222", "nats://localhost:1223" };

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testServersOption() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setNoRandomize(true);

        cf.setServers(testServers);

        // Make sure we can connect to first server if running
        try (NATSServer ns = runServerOnPort(1222)) {
            try (Connection c = cf.createConnection()) {
                assertTrue(String.format("%s != %s", testServers[0], c.getConnectedUrl()),
                        testServers[0].equals(c.getConnectedUrl()));
            } catch (IOException | TimeoutException e) {
                throw e;
            }
        } catch (Exception e1) {
            fail(e1.getMessage());
        }

        // make sure we can connect to a non-first server.
        try (NATSServer ns = runServerOnPort(1227)) {
            try (Connection c = cf.createConnection()) {
                assertTrue(testServers[5] + " != " + c.getConnectedUrl(),
                        testServers[5].equals(c.getConnectedUrl()));
            } catch (IOException | TimeoutException e) {
                throw e;
            }
        } catch (Exception e1) {
            fail(e1.getMessage());
        }
    }

    @Test
    public void testAuthServers() throws IOException, TimeoutException {
        String[] plainServers = new String[] { "nats://localhost:1222", "nats://localhost:1224" };

        ConnectionFactory cf = new ConnectionFactory();
        cf.setNoRandomize(true);
        cf.setServers(plainServers);
        cf.setConnectionTimeout(5000);

        try (NATSServer as1 = runServerWithConfig("auth_1222.conf")) {
            try (NATSServer as2 = runServerWithConfig("auth_1224.conf")) {
                boolean exThrown = false;
                try (Connection c = cf.createConnection()) {
                    fail("Expect Auth failure, got no error");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertNotNull(e.getMessage());
                    assertEquals(Constants.ERR_AUTHORIZATION, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expect Auth failure, got no error", exThrown);


                // Test that we can connect to a subsequent correct server.
                String[] authServers = new String[] { "nats://localhost:1222",
                        "nats://username:password@localhost:1224" };

                cf.setServers(authServers);
                System.err.println("second connection");
                try (Connection c = cf.createConnection()) {
                    assertTrue(c.getConnectedUrl().equals(authServers[1]));
                } catch (IOException | TimeoutException e) {
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
    public void testBasicClusterReconnect() throws IOException, TimeoutException {
        try (NATSServer s1 = runServerOnPort(1222)) {
            try (NATSServer s2 = runServerOnPort(1224)) {

                final ConnectionFactory cf = new ConnectionFactory(testServers);
                final AtomicBoolean dcbCalled = new AtomicBoolean(false);
                cf.setMaxReconnect(2);
                cf.setReconnectWait(1000);
                cf.setNoRandomize(true);

                final CountDownLatch dcLatch = new CountDownLatch(1);
                cf.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        // Suppress any additional calls
                        if (dcbCalled.get()) {
                            return;
                        }
                        dcbCalled.set(true);
                        dcLatch.countDown();
                    }

                });

                final CountDownLatch rcLatch = new CountDownLatch(1);
                cf.setReconnectedCallback(new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rcLatch.countDown();
                    }
                });

                try (Connection c = cf.createConnection()) {
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
                new NATSThreadFactory("testhotspotreconnect"));

        final BlockingQueue<String> rch = new LinkedBlockingQueue<String>();
        final BlockingQueue<Integer> dch = new LinkedBlockingQueue<Integer>();
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        try (NATSServer s1 = runServerOnPort(1222)) {
            try (NATSServer s2 = runServerOnPort(1224)) {
                try (NATSServer s3 = runServerOnPort(1226)) {

                    final class NATSClient implements Runnable {
                        ConnectionFactory cf = new ConnectionFactory();
                        Connection nc = null;
                        final AtomicInteger numReconnects = new AtomicInteger(0);
                        final AtomicInteger numDisconnects = new AtomicInteger(0);
                        String currentUrl = null;
                        final AtomicInteger instance = new AtomicInteger(-1);

                        NATSClient(int inst) {
                            this.instance.set(inst);
                            cf.setServers(testServers);
                            cf.setDisconnectedCallback(new DisconnectedCallback() {
                                public void onDisconnect(ConnectionEvent event) {
                                    numDisconnects.incrementAndGet();
                                    try {
                                        dch.put(instance.get());
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    nc.setDisconnectedCallback(null);
                                }
                            });
                            cf.setReconnectedCallback(new ReconnectedCallback() {
                                public void onReconnect(ConnectionEvent event) {
                                    numReconnects.incrementAndGet();
                                    currentUrl = nc.getConnectedUrl();
                                    try {
                                        rch.put(currentUrl);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }

                        @Override
                        public void run() {
                            try {
                                nc = cf.createConnection();
                                assertTrue(!nc.isClosed());
                                assertNotNull(nc.getConnectedUrl());
                                currentUrl = nc.getConnectedUrl();
                                // System.err.println("Instance " + instance + " connected to " +
                                // currentUrl);
                                while (!shutdown.get()) {
                                    sleep(10);
                                }
                                nc.close();
                            } catch (IOException | TimeoutException e) {
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
                            cs.put(url, new Integer(1));
                        }
                    }

                    for (NATSClient client : tasks) {
                        client.shutdown();
                    }
                    executor.shutdown();
                    try {
                        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

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
        try (NATSServer s1 = runServerOnPort(1222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setServers(testServers);
            cf.setNoRandomize(true);

            final CountDownLatch latch = new CountDownLatch(1);
            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    event.getConnection().setDisconnectedCallback(null);
                    latch.countDown();
                }
            });

            final AtomicBoolean ccbCalled = new AtomicBoolean(false);
            cf.setClosedCallback(new ClosedCallback() {
                public void onClose(ConnectionEvent event) {
                    ccbCalled.set(true);
                }
            });

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());

                s1.shutdown();
                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(latch, 2, TimeUnit.SECONDS));

                // Wait, want to make sure we don't spin on reconnect to non-existent servers.
                sleep(1, TimeUnit.SECONDS);

                assertFalse("Closed CB was triggered, should not have been.", ccbCalled.get());
                assertEquals("Wrong state: " + c.getState(), c.getState(), ConnState.RECONNECTING);
            }
        }
    }

    @Test
    public void testProperFalloutAfterMaxAttempts() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();

        // cf.setServers(testServersShortList);
        cf.setServers(testServers);
        cf.setNoRandomize(true);
        cf.setMaxReconnect(5);
        cf.setReconnectWait(25); // millis

        final CountDownLatch dcLatch = new CountDownLatch(1);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        });

        final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
        final CountDownLatch ccLatch = new CountDownLatch(1);
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                closedCbCalled.set(true);
                ccLatch.countDown();
            }
        });

        try (NATSServer s1 = runServerOnPort(1222)) {
            try (Connection c = cf.createConnection()) {
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
            throws IOException, TimeoutException {
        final String[] myServers = { "nats://localhost:1222", "nats://localhost:4443" };

        ConnectionFactory cf = new ConnectionFactory();
        cf.setServers(myServers);
        cf.setNoRandomize(true);
        cf.setMaxReconnect(5);
        cf.setReconnectWait(25); // millis

        final CountDownLatch dcLatch = new CountDownLatch(1);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        });

        final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
        final CountDownLatch ccLatch = new CountDownLatch(1);
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                closedCbCalled.set(true);
                ccLatch.countDown();
            }
        });

        try (NATSServer s1 = runServerOnPort(1222)) {
            try (NATSServer s2 = runServerWithConfig("tlsverify.conf")) {
                try (ConnectionImpl nc = (ConnectionImpl) cf.createConnection()) {
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
                    assertEquals("Wrong number of reconnects", nc.opts.getMaxReconnect(),
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
    public void testTimeoutOnNoServers() {
        final ConnectionFactory cf = new ConnectionFactory();
        cf.setServers(testServers);
        cf.setNoRandomize(true);

        // 1 second total time wait
        cf.setMaxReconnect(10);
        cf.setReconnectWait(100);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent ev) {
                ev.getConnection().setDisconnectedCallback(null);
                dcLatch.countDown();
            }
        });

        final CountDownLatch ccLatch = new CountDownLatch(1);
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent ev) {
                ccLatch.countDown();
            }
        });

        try (NATSServer s1 = new NATSServer(1222)) {
            sleep(100);

            try (Connection c = cf.createConnection()) {
                assertNotNull(c.getDisconnectedCallback());
                s1.shutdown();
                // while(dch.getCount()!=1) {
                // UnitTestUtilities.sleep(50);
                // }
                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(dcLatch, 2, TimeUnit.SECONDS));

                long t0 = System.nanoTime();

                // Wait for ClosedCB
                assertTrue("Did not receive a closed callback signal",
                        await(ccLatch, 2, TimeUnit.SECONDS));

                long elapsedMsec = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

                // Use 500ms as variable time delta
                long variable = 500;
                long expected = cf.getMaxReconnect() * cf.getReconnectWait();

                assertFalse("Waited too long for Closed state: " + elapsedMsec,
                        elapsedMsec > (expected + variable));
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testPingReconnect() throws Exception {
        final int reconnects = 4;
        try (NATSServer s1 = runServerOnPort(1222)) {
            ConnectionFactory cf = new ConnectionFactory();

            cf.setServers(testServers);
            cf.setNoRandomize(true);
            cf.setReconnectWait(200);
            cf.setPingInterval(50);
            cf.setMaxPingsOut(-1);
            cf.setConnectionTimeout(1000);

            final CountDownLatch wg = new CountDownLatch(1);
            final BlockingQueue<Long> rch = new LinkedBlockingQueue<Long>(reconnects);
            final BlockingQueue<Long> dch = new LinkedBlockingQueue<Long>(reconnects);

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    if (dch.size() < reconnects) {
                        dch.add(System.nanoTime());
                    }
                }
            });

            cf.setReconnectedCallback(new ReconnectedCallback() {
                @Override
                public void onReconnect(ConnectionEvent event) {
                    rch.add(System.nanoTime());
                    if (rch.size() == reconnects) {
                        wg.countDown();
                    }
                }
            });

            try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
                wg.await();
                s1.shutdown();

                while (rch.size() < reconnects) {
                    sleep(10);
                }
                for (int i = 0; i < reconnects - 1; i++) {
                    Long disconnectedAt = dch.poll(5, TimeUnit.SECONDS);
                    assertNotNull("Didn't get all our disconnected signals", disconnectedAt);
                    long reconnectedAt = rch.poll(1, TimeUnit.SECONDS);
                    assertNotNull("Didn't get all our resconnected signals", reconnectedAt);
                    long pingCycle = TimeUnit.NANOSECONDS.toMillis(disconnectedAt - reconnectedAt);
                    assertFalse(pingCycle > 2 * c.opts.getPingInterval());
                }
            }
        }
    }
}
