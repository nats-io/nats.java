/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Nats.ConnState.CONNECTED;
import static io.nats.client.Nats.ConnState.RECONNECTING;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runServerOnPort;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Category(IntegrationTest.class)
public class ITReconnectTest {
    static final Logger logger = LoggerFactory.getLogger(ITReconnectTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    private Properties reconnectOptions = getReconnectOptions();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    private static Properties getReconnectOptions() {
        Properties props = new Properties();

        props.setProperty(Nats.PROP_URL, "nats://localhost:22222");
        props.setProperty(Nats.PROP_RECONNECT_ALLOWED, Boolean.toString(true));
        props.setProperty(Nats.PROP_MAX_RECONNECT, Integer.toString(10));
        props.setProperty(Nats.PROP_RECONNECT_WAIT, Integer.toString(100));

        return props;
    }

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testReconnectDisallowedFlags() {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        cf.setUrl("nats://localhost:22222");
        cf.setReconnectAllowed(false);

        final CountDownLatch latch = new CountDownLatch(1);
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                latch.countDown();
            }
        });

        try (NatsServer ts = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                sleep(500);
                ts.shutdown();
                assertTrue(await(latch));
            } catch (IOException | TimeoutException e1) {
                fail(e1.getMessage());
            }
        }
    }

    @Test
    public void testReconnectAllowedFlags() {
        final int maxRecon = 2;
        final long reconWait = TimeUnit.SECONDS.toMillis(1);
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setReconnectAllowed(true);
        cf.setMaxReconnect(maxRecon);
        cf.setReconnectWait(reconWait);

        final CountDownLatch ccLatch = new CountDownLatch(1);
        final CountDownLatch dcLatch = new CountDownLatch(1);

        try (NatsServer ts = runServerOnPort(22222)) {
            final AtomicInteger ccbCount = new AtomicInteger(0);
            final AtomicLong ccbTime = new AtomicLong(0L);
            final AtomicLong dcbTime = new AtomicLong(0L);
            try (final ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        ccbTime.set(System.nanoTime());
                        long closeInterval =
                                TimeUnit.NANOSECONDS.toMillis(ccbTime.get() - dcbTime.get());
                        // Check to ensure that the disconnect cb fired first
                        assertTrue((dcbTime.get() - ccbTime.get()) < 0);
                        assertNotEquals("ClosedCB triggered prematurely.", 0L, dcbTime);
                        ccbCount.incrementAndGet();
                        ccLatch.countDown();
                        assertTrue(c.isClosed());
                    }
                });

                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        // Check to ensure that the closed cb didn't fire first
                        dcbTime.set(System.nanoTime());
                        assertTrue((dcbTime.get() - ccbTime.get()) > 0);
                        assertEquals("ClosedCB triggered prematurely.", 0, ccbCount.get());
                        dcLatch.countDown();
                        // assertFalse(c.isClosed());
                        logger.debug("Signaled disconnect");
                    }
                });

                assertFalse(c.isClosed());

                ts.shutdown();

                // We want wait to timeout here, and the connection
                // should not trigger the Close CB.
                assertEquals(0, ccbCount.get());
                assertFalse(await(ccLatch, 500, TimeUnit.MILLISECONDS));


                // We should wait to get the disconnected callback to ensure
                // that we are in the process of reconnecting.
                assertTrue("DisconnectedCB should have been triggered.", await(dcLatch));

                assertTrue("Expected to be in a reconnecting state", c.isReconnecting());

                // assertEquals(1, cch.getCount());
                // assertTrue(UnitTestUtilities.waitTime(cch, 1000, TimeUnit.MILLISECONDS));

            } // Connection
            catch (IOException | TimeoutException e1) {
                fail("Should have connected OK: " + e1.getMessage());
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        } // NatsServer
    }

    @Test
    public void testBasicReconnectFunctionality() {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setMaxReconnect(10);
        cf.setReconnectWait(100);

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch dcLatch = new CountDownLatch(1);

        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
                logger.debug("dcb triggered");
            }
        });
        final String testString = "bar";

        try (NatsServer ns1 = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        String s = new String(msg.getData());
                        if (!s.equals(testString)) {
                            fail("String doesn't match");
                        }
                        latch.countDown();
                    }
                });

                try {
                    c.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }

                logger.debug("Shutting down ns1");
                ns1.shutdown();
                // server is stopped here...

                logger.debug("Waiting for disconnect callback");
                assertTrue("Did not get the disconnected callback on time", await(dcLatch));

                UnitTestUtilities.sleep(50);
                logger.debug("Publishing test message");
                c.publish("foo", testString.getBytes());

                // restart the server.
                logger.debug("Spinning up ns2");
                try (NatsServer ns2 = runServerOnPort(22222)) {
                    c.setClosedCallback(null);
                    c.setDisconnectedCallback(null);
                    logger.debug("Flushing connection");
                    try {
                        c.flush(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    assertTrue("Did not receive our message", await(latch));
                    assertEquals("Wrong number of reconnects.", 1, c.getStats().getReconnects());
                } // ns2
            } // Connection
            catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } // ns1
    }

    @Test
    public void testExtendedReconnectFunctionality() throws Exception {
        Properties opts = reconnectOptions;
        ConnectionFactory cf = new ConnectionFactory(opts);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        });

        final CountDownLatch rcLatch = new CountDownLatch(1);
        cf.setReconnectedCallback(new ReconnectedCallback() {
            public void onReconnect(ConnectionEvent event) {
                rcLatch.countDown();
            }
        });

        final AtomicInteger received = new AtomicInteger(0);

        final MessageHandler mh = new MessageHandler() {
            public void onMessage(Message msg) {
                received.incrementAndGet();
            }
        };

        byte[] payload = "bar".getBytes();
        try (NatsServer ns1 = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                c.subscribe("foo", mh);
                final Subscription foobarSub = c.subscribe("foobar", mh);

                c.publish("foo", payload);
                c.flush();

                ns1.shutdown();
                // server is stopped here.

                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(dcLatch, 2, TimeUnit.SECONDS));

                // sub while disconnected.
                Subscription barSub = c.subscribe("bar", mh);

                // Unsub foobar while disconnected
                foobarSub.unsubscribe();

                c.publish("foo", payload);
                c.publish("bar", payload);

                try (NatsServer ns2 = runServerOnPort(22222)) {
                    // server is restarted here...
                    // wait for reconnect
                    assertTrue("Did not receive a reconnect callback message",
                            await(rcLatch, 2, TimeUnit.SECONDS));

                    c.publish("foobar", payload);
                    c.publish("foo", payload);

                    final CountDownLatch latch = new CountDownLatch(1);
                    c.subscribe("done", new MessageHandler() {
                        public void onMessage(Message msg) {
                            latch.countDown();
                        }
                    });
                    sleep(500, TimeUnit.MILLISECONDS);
                    c.publish("done", null);

                    assertTrue("Did not receive our 'done' message", await(latch));

                    // Sleep a bit to guarantee exec runs and processes all subs
                    sleep(50, TimeUnit.MILLISECONDS);

                    assertEquals(4, received.get());

                    c.setDisconnectedCallback(null);
                }
            } // Connection c
            catch (IOException | TimeoutException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                fail(e1.getMessage());
            }
        } // NatsServer ns
    }

    final Object mu = new Object();
    final Map<Integer, Integer> results = new ConcurrentHashMap<Integer, Integer>();

    @Test
    public void testQueueSubsOnReconnect() throws IllegalStateException, Exception {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        try (NatsServer ts = runServerOnPort(22222)) {

            final CountDownLatch latch = new CountDownLatch(1);
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    latch.countDown();
                }
            });

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());
                // TODO create encoded connection

                // Make sure we got what we needed, 1 msg only and all seqnos accounted for..
                MessageHandler cb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        String num = new String(msg.getData());
                        Integer seqno = Integer.parseInt(num);
                        if (results.containsKey(seqno)) {
                            Integer val = results.get(seqno);
                            results.put(seqno, val++);
                        } else {
                            results.put(seqno, 1);
                        }
                    } // onMessage
                };


                String subj = "foo.bar";
                String qgroup = "workers";

                c.subscribe(subj, qgroup, cb);
                c.subscribe(subj, qgroup, cb);
                c.flush();

                // Base test
                sendAndCheckMsgs(c, subj, 10);

                // Stop and restart server
                ts.shutdown();

                // start back up
                try (NatsServer ts2 = runServerOnPort(22222)) {
                    sleep(500);
                    assertTrue("Did not fire ReconnectedCB", await(latch, 3, TimeUnit.SECONDS));
                    sendAndCheckMsgs(c, subj, 10);
                } // ts2
            } // connection
        } // ts

    }

    void checkResults(int numSent) {
        for (int i = 0; i < numSent; i++) {
            if (results.containsKey(i)) {
                assertEquals("results count should have been 1 for " + String.valueOf(i), (int) 1,
                        (int) results.get(i));
            } else {
                fail("results doesn't contain seqno: " + i);
            }
        }
        results.clear();
    }

    void sendAndCheckMsgs(Connection conn, String subj, int numToSend) {
        int numSent = 0;
        for (int i = 0; i < numToSend; i++) {
            try {
                conn.publish(subj, Integer.toString(i).getBytes());
            } catch (IllegalStateException | IOException e) {
                fail(e.getMessage());
            }
            numSent++;
        }
        // Wait for processing
        try {
            conn.flush();
        } catch (Exception e) {
            fail(e.getMessage());
        }
        sleep(50);

        checkResults(numSent);
    }

    @Test
    public void testIsClosed() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setReconnectAllowed(true);
        cf.setMaxReconnect(60);

        Connection c = null;
        try (NatsServer s1 = runServerOnPort(22222)) {
            c = cf.createConnection();
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());
        }

        assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

        try (NatsServer s2 = runServerOnPort(22222)) {
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

            c.close();
            assertTrue("isClosed returned false after close() was called.", c.isClosed());
        }
    }

    @Test
    public void testIsReconnectingAndStatus() {

        try (NatsServer ts = runServerOnPort(22222)) {
            final CountDownLatch dcLatch = new CountDownLatch(1);
            final CountDownLatch rcLatch = new CountDownLatch(1);

            ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
            cf.setReconnectAllowed(true);
            cf.setMaxReconnect(10000);
            cf.setReconnectWait(100);

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcLatch.countDown();
                }
            });

            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rcLatch.countDown();
                }
            });

            // Connect, verify initial reconnecting state check, then stop the server
            try (Connection c = cf.createConnection()) {
                assertFalse("isReconnecting returned true when the connection is still open.",
                        c.isReconnecting());

                assertEquals(CONNECTED, c.getState());

                ts.shutdown();

                assertTrue("Disconnect callback wasn't triggered.", await(dcLatch));

                assertTrue("isReconnecting returned false when the client is reconnecting.",
                        c.isReconnecting());

                assertEquals(RECONNECTING, c.getState());

                // Wait until we get the reconnect callback
                try (NatsServer ts2 = runServerOnPort(22222)) {
                    assertTrue("reconnectedCb callback wasn't triggered.",
                            await(rcLatch, 3, TimeUnit.SECONDS));

                    assertFalse("isReconnecting returned true after the client was reconnected.",
                            c.isReconnecting());

                    assertEquals(CONNECTED, c.getState());

                    // Close the connection, reconnecting should still be false
                    c.close();

                    assertFalse("isReconnecting returned true after close() was called.",
                            c.isReconnecting());

                    assertTrue("Status returned " + c.getState()
                            + " after close() was called instead of CLOSED", c.isClosed());
                }
            } catch (IOException | TimeoutException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                fail();
            }
        }
    }

    @Test
    public void testDefaultReconnectFailure() {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setMaxReconnect(4);

            final CountDownLatch ccLatch = new CountDownLatch(1);

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());

                final CountDownLatch dcLatch = new CountDownLatch(1);
                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        dcLatch.countDown();
                    }
                });

                final CountDownLatch rcLatch = new CountDownLatch(1);
                c.setReconnectedCallback(new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rcLatch.countDown();
                    }
                });

                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        ccLatch.countDown();
                    }
                });

                ts.shutdown();

                assertTrue("Should have signaled disconnected",
                        await(dcLatch, 1, TimeUnit.SECONDS));

                assertFalse("Should not have reconnected", await(rcLatch, 1, TimeUnit.SECONDS));

            } // conn
            catch (IOException | TimeoutException e1) {
                e1.printStackTrace();
                fail(e1.getMessage());
            }

            assertTrue("Connection didn't close within timeout",
                    await(ccLatch, 2, TimeUnit.SECONDS));

            // assertEquals(0, c.getStats().getReconnects());
            // assertTrue("Should have been closed but is " + c.getState(), c.isClosed());

        } // ts
    }

    @Test
    public void testReconnectBufSize() {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectBufSize(34); // 34 bytes

            final CountDownLatch dcLatch = new CountDownLatch(1);
            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent ev) {
                    dcLatch.countDown();
                }
            });

            try (ConnectionImpl nc = (ConnectionImpl) cf.createConnection()) {
                try {
                    nc.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Error during flush: " + e.getMessage());
                }

                // Force disconnected state
                ts.shutdown();

                assertTrue("DisconnectedCB should have been triggered", await(dcLatch));

                byte[] msg = "food".getBytes(); // 4 bytes payload, total proto is 17 bytes

                // The buffer check is before the publish operation, here it will be 0
                nc.publish("foo", msg);

                // Here the buffer size will be 17 when we check
                nc.publish("foo", msg);

                // At this point, we are at the buffer limit
                assertEquals(34, nc.getPendingByteCount());

                // This should fail since we have exhausted the backing buffer.
                boolean exThrown = false;
                try {
                    nc.publish("foo", msg);
                } catch (IOException e) {
                    assertEquals(Nats.ERR_RECONNECT_BUF_EXCEEDED, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected to fail to publish message: got no error", exThrown);

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testReconnectVerbose() {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setVerbose(true);

            final CountDownLatch latch = new CountDownLatch(1);
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent ev) {
                    latch.countDown();
                }
            });

            try (Connection nc = cf.createConnection()) {
                try {
                    nc.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Error during flush: " + e.getMessage());
                }

                ts.shutdown();
                sleep(500);
                try (NatsServer ts2 = runServerOnPort(4222)) {
                    assertTrue("Should have reconnected OK", await(latch));
                    try {
                        nc.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Error during flush: " + e.getMessage());
                    }
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    // @Test
    // public void testFullFlushChanDuringReconnect() {
    // final ExecutorService exec = Executors.newSingleThreadExecutor();
    // try (NatsServer ts = utils.createServerOnPort(22222)) {
    // ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
    // cf.setReconnectAllowed(true);
    // cf.setMaxReconnect(10000);
    // cf.setReconnectWait(100);
    // final Channel<Boolean> rch = new Channel<Boolean>();
    // final Channel<Boolean> stop = new Channel<Boolean>();
    //
    // try (Connection nc = cf.createConnection()) {
    // // While connected, publish as fast as we can
    // exec.execute(new Runnable() {
    // public void run() {
    // for (int i=0; ; i++) {
    // try {
    // nc.publish("foo", "hello".getBytes());
    // // Make sure we are sending at least flushChanSize (1024) messages
    // // before potentially pausing.
    // if (i % 2000 == 0) {
    // try {
    // Boolean rv = stop.get(0);
    // if (rv != null) {
    // if (rv == true)
    // return;
    // } else {
    // sleep(100, TimeUnit.MILLISECONDS);
    // }
    // } catch (TimeoutException e) {
    // }
    // }
    // } catch (IOException e) {
    // }
    // }
    // }
    // });
    //
    // // Send a bit
    // sleep(500, TimeUnit.MILLISECONDS);
    //
    // // Shut down the server
    // ts.shutdown();
    //
    // // Continue sending wile we are disconnected
    // sleep(1, TimeUnit.SECONDS);
    //
    // stop.add(true);
    //
    // // Restart the server
    // try (NatsServer ts2 = utils.createServerOnPort(22222, true)) {
    // // Wait for the reconnect CB to be invoked (but not for too long)
    // assertTrue("Reconnect callback wasn't triggered", waitTime(rch, 5, TimeUnit.SECONDS));
    // }
    // exec.shutdownNow();
    // } catch (IOException | TimeoutException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }
    // }
}
