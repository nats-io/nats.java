/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.sleep;
import static io.nats.client.UnitTestUtilities.waitTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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

        props.setProperty(ConnectionFactory.PROP_URL, "nats://localhost:22222");
        props.setProperty(ConnectionFactory.PROP_RECONNECT_ALLOWED, Boolean.toString(true));
        props.setProperty(ConnectionFactory.PROP_MAX_RECONNECT, Integer.toString(10));
        props.setProperty(ConnectionFactory.PROP_RECONNECT_WAIT, Integer.toString(100));

        return props;
    }

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testReconnectDisallowedFlags() {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        cf.setUrl("nats://localhost:22222");
        cf.setReconnectAllowed(false);

        final Channel<Boolean> cch = new Channel<Boolean>();
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                cch.add(true);
            }
        });

        try (NATSServer ts = utils.createServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                sleep(500);
                ts.shutdown();
                try {
                    assertTrue(cch.get(5, TimeUnit.SECONDS));
                } catch (TimeoutException e) {
                    fail(e.getMessage());
                }
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

        final Channel<Boolean> cch = new Channel<Boolean>();
        final Channel<Boolean> dch = new Channel<Boolean>();

        try (NATSServer ts = utils.createServerOnPort(22222)) {
            final AtomicLong ccbTime = new AtomicLong(0L);
            final AtomicLong dcbTime = new AtomicLong(0L);
            try (final ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        ccbTime.set(System.nanoTime());
                        long closeInterval =
                                TimeUnit.NANOSECONDS.toMillis(ccbTime.get() - dcbTime.get());
                        logger.debug("in onClose(), interval was " + closeInterval);
                        // Check to ensure that the disconnect cb fired first
                        assertTrue((dcbTime.get() - ccbTime.get()) < 0);
                        logger.debug("Interval = "
                                + TimeUnit.NANOSECONDS.toMillis((dcbTime.get() - ccbTime.get()))
                                + "ms");
                        assertNotEquals("ClosedCB triggered prematurely.", 0L, dcbTime);
                        cch.add(true);
                        logger.debug("Signaling close...");
                        assertTrue(c.isClosed());
                        logger.debug("Signaled close");
                    }
                });

                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        // Check to ensure that the closed cb didn't fire first
                        dcbTime.set(System.nanoTime());
                        assertTrue((dcbTime.get() - ccbTime.get()) > 0);
                        assertEquals("ClosedCB triggered prematurely.", 0, cch.getCount());
                        dch.add(true);
                        // assertFalse(c.isClosed());
                        logger.debug("Signaled disconnect");
                    }
                });

                assertFalse(c.isClosed());

                ts.shutdown();

                // We want wait to timeout here, and the connection
                // should not trigger the Close CB.
                assertEquals(0, cch.getCount());
                assertFalse(waitTime(cch, 500, TimeUnit.MILLISECONDS));


                // We should wait to get the disconnected callback to ensure
                // that we are in the process of reconnecting.
                try {
                    assertTrue("DisconnectedCB should have been triggered.",
                            dch.get(5, TimeUnit.SECONDS));
                    assertEquals("dch should be empty", 0, dch.getCount());
                } catch (TimeoutException e) {
                    fail("DisconnectedCB triggered incorrectly. " + e.getMessage());
                }

                assertTrue("Expected to be in a reconnecting state", c.isReconnecting());

                // assertEquals(1, cch.getCount());
                // assertTrue(UnitTestUtilities.waitTime(cch, 1000, TimeUnit.MILLISECONDS));

            } // Connection
            catch (IOException | TimeoutException e1) {
                fail("Should have connected OK: " + e1.getMessage());
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        } // NATSServer
    }

    @Test
    public void testBasicReconnectFunctionality() {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setMaxReconnect(10);
        cf.setReconnectWait(100);

        final Channel<Boolean> ch = new Channel<Boolean>();
        final Channel<Boolean> dch = new Channel<Boolean>();

        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dch.add(true);
                logger.debug("dcb triggered");
            }
        });
        final String testString = "bar";

        try (NATSServer ns1 = utils.createServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        String s = new String(msg.getData());
                        if (!s.equals(testString)) {
                            fail("String doesn't match");
                        }
                        ch.add(true);
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
                assertTrue("Did not get the disconnected callback on time",
                        dch.get(5, TimeUnit.SECONDS));

                UnitTestUtilities.sleep(50);
                logger.debug("Publishing test message");
                c.publish("foo", testString.getBytes());

                // restart the server.
                logger.debug("Spinning up ns2");
                try (NATSServer ns2 = utils.createServerOnPort(22222)) {
                    c.setClosedCallback(null);
                    c.setDisconnectedCallback(null);
                    logger.debug("Flushing connection");
                    try {
                        c.flush(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    assertTrue("Did not receive our message", ch.get(5, TimeUnit.SECONDS));
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

        final Channel<Boolean> dch = new Channel<Boolean>();
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dch.add(true);
            }
        });

        final Channel<Boolean> rch = new Channel<Boolean>();
        cf.setReconnectedCallback(new ReconnectedCallback() {
            public void onReconnect(ConnectionEvent event) {
                rch.add(true);
            }
        });

        final AtomicInteger received = new AtomicInteger(0);

        final MessageHandler mh = new MessageHandler() {
            public void onMessage(Message msg) {
                received.incrementAndGet();
            }
        };

        byte[] payload = "bar".getBytes();
        try (NATSServer ns1 = utils.createServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                c.subscribe("foo", mh);
                final Subscription foobarSub = c.subscribe("foobar", mh);

                c.publish("foo", payload);
                c.flush();

                ns1.shutdown();
                // server is stopped here.

                // wait for disconnect
                try {
                    assertTrue("Did not receive a disconnect callback message",
                            dch.get(2, TimeUnit.SECONDS));
                } catch (TimeoutException e1) {
                    fail("Did not receive a disconnect callback message");
                }

                // sub while disconnected.
                Subscription barSub = c.subscribe("bar", mh);

                // Unsub foobar while disconnected
                foobarSub.unsubscribe();

                c.publish("foo", payload);
                c.publish("bar", payload);

                try (NATSServer ns2 = utils.createServerOnPort(22222)) {
                    // server is restarted here...
                    // wait for reconnect
                    assertTrue("Did not receive a reconnect callback message",
                            rch.get(2, TimeUnit.SECONDS));

                    c.publish("foobar", payload);
                    c.publish("foo", payload);

                    final Channel<Boolean> ch = new Channel<Boolean>();
                    c.subscribe("done", new MessageHandler() {
                        public void onMessage(Message msg) {
                            ch.add(true);
                        }
                    });
                    sleep(500, TimeUnit.MILLISECONDS);
                    c.publish("done", null);

                    assertTrue("Did not receive our 'done' message", ch.get(5, TimeUnit.SECONDS));

                    // Sleep a bit to guarantee scheduler runs and processes all subs
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
        } // NATSServer ns
    }

    final Object mu = new Object();
    final Map<Integer, Integer> results = new ConcurrentHashMap<Integer, Integer>();

    @Test
    public void testQueueSubsOnReconnect() throws IllegalStateException, Exception {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        try (NATSServer ts = utils.createServerOnPort(22222)) {

            final Channel<Boolean> rch = new Channel<Boolean>();
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rch.add(true);
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
                try (NATSServer ts2 = utils.createServerOnPort(22222)) {
                    sleep(500);
                    assertTrue("Did not fire ReconnectedCB", rch.get(3, TimeUnit.SECONDS));
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

    public void sendAndCheckMsgs(Connection c, String subj, int numToSend) {
        int numSent = 0;
        for (int i = 0; i < numToSend; i++) {
            try {
                c.publish(subj, Integer.toString(i).getBytes());
            } catch (IllegalStateException | IOException e) {
                fail(e.getMessage());
            }
            numSent++;
        }
        // Wait for processing
        try {
            c.flush();
        } catch (Exception e) {
            fail(e.getMessage());
        }
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
        }

        checkResults(numSent);
    }

    @Test
    public void testIsClosed() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setReconnectAllowed(true);
        cf.setMaxReconnect(60);

        Connection c = null;
        try (NATSServer s1 = utils.createServerOnPort(22222)) {
            c = cf.createConnection();
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());
        }

        assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

        try (NATSServer s2 = utils.createServerOnPort(22222)) {
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

            c.close();
            assertTrue("isClosed returned false after close() was called.", c.isClosed());
        }
    }

    @Test
    public void testIsReconnectingAndStatus() {

        try (NATSServer ts = utils.createServerOnPort(22222)) {
            final Channel<Boolean> dch = new Channel<Boolean>();
            final Channel<Boolean> rch = new Channel<Boolean>();

            ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
            cf.setReconnectAllowed(true);
            cf.setMaxReconnect(10000);
            cf.setReconnectWait(100);

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dch.add(true);
                }
            });

            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rch.add(true);
                }
            });

            // Connect, verify initial reconnecting state check, then stop the server
            try (Connection c = cf.createConnection()) {
                assertFalse("isReconnecting returned true when the connection is still open.",
                        c.isReconnecting());

                assertEquals(ConnState.CONNECTED, c.getState());

                ts.shutdown();

                try {
                    assertTrue("Disconnect callback wasn't triggered.",
                            dch.get(5, TimeUnit.SECONDS));
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
                assertTrue("isReconnecting returned false when the client is reconnecting.",
                        c.isReconnecting());

                assertEquals(ConnState.RECONNECTING, c.getState());

                // Wait until we get the reconnect callback
                try (NATSServer ts2 = utils.createServerOnPort(22222)) {
                    try {
                        assertTrue("reconnectedCB callback wasn't triggered.",
                                rch.get(3, TimeUnit.SECONDS));
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }

                    assertFalse("isReconnecting returned true after the client was reconnected.",
                            c.isReconnecting());

                    assertEquals(ConnState.CONNECTED, c.getState());

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
        try (NATSServer ts = utils.createServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setMaxReconnect(4);

            final Channel<Boolean> cch = new Channel<Boolean>();
            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());

                final Channel<Boolean> dch = new Channel<Boolean>();
                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        dch.add(true);
                    }
                });
                final Channel<Boolean> rch = new Channel<Boolean>();
                c.setReconnectedCallback(new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rch.add(true);
                    }
                });

                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        cch.add(true);
                    }
                });

                ts.shutdown();

                boolean disconnected = false;
                try {
                    disconnected = dch.get(1, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    fail("Should have signaled disconnected");
                }
                assertTrue("Should have signaled disconnected", disconnected);

                boolean reconnected = false;
                try {
                    reconnected = rch.get(1, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // This should happen
                }
                assertFalse("Should not have reconnected", reconnected);

            } // conn
            catch (IOException | TimeoutException e1) {
                e1.printStackTrace();
                fail(e1.getMessage());
            }
            boolean closed = false;
            try {
                closed = cch.get(2, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                fail("Connection didn't close within timeout");
            }
            assertTrue("Conn should have closed within timeout", closed);

            // assertEquals(0, c.getStats().getReconnects());
            // assertTrue("Should have been closed but is " + c.getState(), c.isClosed());

        } // ts
    }

    @Test
    public void testReconnectBufSize() {
        try (NATSServer ts = utils.createServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectBufSize(34); // 34 bytes

            final Channel<Boolean> dch = new Channel<Boolean>();
            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent ev) {
                    dch.add(true);
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

                assertTrue("DisconnectedCB should have been triggered",
                        waitTime(dch, 5, TimeUnit.SECONDS));

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
                    assertEquals(Constants.ERR_RECONNECT_BUF_EXCEEDED, e.getMessage());
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
        try (NATSServer ts = utils.createServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setVerbose(true);

            final Channel<Boolean> rch = new Channel<Boolean>();
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent ev) {
                    rch.add(true);
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
                try (NATSServer ts2 = utils.createServerOnPort(4222)) {
                    assertTrue("Should have reconnected OK", waitTime(rch, 5, TimeUnit.SECONDS));
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
    // try (NATSServer ts = utils.createServerOnPort(22222)) {
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
    // try (NATSServer ts2 = utils.createServerOnPort(22222, true)) {
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
