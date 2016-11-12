/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Nats.ERR_MAX_MESSAGES;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Category(IntegrationTest.class)
public class ITSubscriptionTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final Logger logger = LoggerFactory.getLogger(ITSubscriptionTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private ExecutorService exec;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * Per-test-case setup.
     *
     * @throws Exception if something goes wrong
     */
    @Before
    public void setUp() throws Exception {
        // if (Thread.interrupted()) {
        // Thread.interrupted();
        // }
        exec = Executors.newCachedThreadPool(new NatsThreadFactory("nats-test-thread"));
    }

    /**
     * Per-test-case cleanup.
     *
     * @throws Exception if something goes wrong
     */
    @After
    public void tearDown() throws Exception {
        setLogLevel(Level.INFO);
        if (!exec.isShutdown()) {
            exec.shutdownNow();
        }
    }

    @Test
    public void testServerAutoUnsub() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                assertFalse(c.isClosed());
                final AtomicLong received = new AtomicLong(0L);
                int max = 10;

                try (Subscription s = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        received.incrementAndGet();
                    }
                })) {
                    s.autoUnsubscribe(max);
                    int total = 100;

                    for (int i = 0; i < total; i++) {
                        c.publish("foo", "Hello".getBytes());
                    }
                    try {
                        c.flush();
                    } catch (Exception e1) {
                        fail(e1.getMessage());
                    }

                    sleep(100);

                    assertEquals(max, received.get());
                    assertFalse("Expected subscription to be invalid after hitting max",
                            s.isValid());
                }
            }
        }
    }

    @Test
    public void testClientSyncAutoUnsub() {
        try (NatsServer s = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                assertFalse(c.isClosed());

                long received = 0;
                int max = 10;
                boolean exThrown = false;
                try (SyncSubscription sub = c.subscribeSync("foo")) {
                    sub.autoUnsubscribe(max);

                    int total = 100;
                    for (int i = 0; i < total; i++) {
                        c.publish("foo", "Hello".getBytes());
                    }
                    try {
                        c.flush();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }

                    while (true) {
                        try {
                            sub.nextMessage(100);
                            received++;
                        } catch (IOException e) {
                            assertEquals(ERR_MAX_MESSAGES, e.getMessage());
                            exThrown = true;
                            break;
                        } catch (Exception e) {
                            // catch-all
                            fail("Wrong exception: " + e.getMessage());
                        }
                    }
                    assertTrue("Should have thrown IOException", exThrown);
                    assertEquals(max, received);
                    assertFalse("Expected subscription to be invalid after hitting max",
                            sub.isValid());
                }
            } catch (IOException | TimeoutException e2) {
                fail("Should have connected");
            }
        }
    }

    @Test
    public void testClientAsyncAutoUnsub() throws IOException, TimeoutException {
        final AtomicInteger received = new AtomicInteger(0);
        MessageHandler mh = new MessageHandler() {
            @Override
            public void onMessage(Message msg) {
                received.getAndIncrement();
            }
        };

        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                assertFalse(c.isClosed());

                int max = 10;
                try (Subscription s = c.subscribe("foo", mh)) {
                    s.autoUnsubscribe(max);

                    int total = 100;
                    for (int i = 0; i < total; i++) {
                        c.publish("foo", "Hello".getBytes());
                    }
                    try {
                        c.flush();
                    } catch (Exception e1) {
                        /* NOOP */
                    }

                    sleep(10);
                    assertFalse("Expected subscription to be invalid after hitting max",
                            s.isValid());
                    assertEquals(max, received.get());
                }
            }
        }
    }

    @Test
    public void testAutoUnsubAndReconnect() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            sleep(500);
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger received = new AtomicInteger(0);
            final int max = 10;

            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectWait(50);
            cf.setReconnectAllowed(true);
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    latch.countDown();
                }
            });
            try (Connection nc = cf.createConnection()) {
                AsyncSubscriptionImpl sub =
                        (AsyncSubscriptionImpl) nc.subscribe("foo", new MessageHandler() {
                            public void onMessage(Message msg) {
                                received.incrementAndGet();
                            }
                        });
                sub.autoUnsubscribe(max);

                // Send less than the max
                int total = max / 2;
                String str;
                for (int i = 0; i < total; i++) {
                    str = String.format("Hello %d", i + 1);
                    nc.publish("foo", str.getBytes());
                }
                nc.flush();

                // Restart the server
                srv.shutdown();

                try (NatsServer srv2 = runDefaultServer()) {
                    // and wait to reconnect
                    assertTrue("Failed to get the reconnect cb", latch.await(5, TimeUnit.SECONDS));
                    assertTrue("Subscription should still be valid", sub.isValid());

                    // Ensure we only received 5 messages on our sub up to this point
                    assertEquals(total, nc.getStats().getInMsgs());

                    // Now send more than the total max.
                    int oldTotal = total;
                    total = 3 * max;
                    for (int i = 0; i < total; i++) {
                        str = String.format("Hello %d", i + oldTotal + 1);
                        nc.publish("foo", str.getBytes());
                    }
                    nc.flush();

                    // wait a bit before checking.
                    sleep(50, TimeUnit.MILLISECONDS);

                    assertEquals("Stats don't match reality, ", received.get(),
                            nc.getStats().getInMsgs());

                    // We should have received only up-to-max messages.
                    assertEquals("Received wrong total #msgs,", max, received.get());
                }
            }
        }
    }

    @Test
    public void testAutoUnsubWithParallelNextMsgCalls() throws Exception {
        final CountDownLatch rcbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectWait(50);
            try (final Connection nc = cf.createConnection()) {
                nc.setReconnectedCallback(new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rcbLatch.countDown();
                    }
                });
                final int numRoutines = 3;
                final int max = 100;
                final int total = max * 2;
                final AtomicLong received = new AtomicLong(0);

                final SyncSubscription sub = nc.subscribeSync("foo");
                sub.autoUnsubscribe(max);
                nc.flush();

                final CountDownLatch wg = new CountDownLatch(numRoutines);
                for (int i = 0; i < numRoutines; i++) {
                    exec.execute(new Runnable() {
                        public void run() {
                            long t0 = 0L;
                            long elapsed = 0L;
                            while (true) {
                                // The first to reach the max delivered will cause the
                                // subscription to be removed, which will kick out all
                                // other calls to NextMsg. So don't be afraid of the long
                                // timeout.
                                Message msg;
                                try {
                                    t0 = System.nanoTime();
                                    msg = sub.nextMessage(3, TimeUnit.SECONDS);
                                    assertNotNull(msg);
                                    if (received.incrementAndGet() >= max) {
                                        break;
                                    }
                                } catch (IOException | InterruptedException e) {
                                    elapsed = System.nanoTime() - t0;
                                    logger.debug("Thread {} interrupted: '{}'",
                                            Thread.currentThread().getId(), e.getMessage());
                                    break;
                                }
                            }
                            wg.countDown();
                        }
                    });
                }

                for (int i = 0; i < max / 2; i++) {
                    nc.publish("foo", String.format("Hello %d", i).getBytes());
                }
                nc.flush();

                srv.shutdown();

                try (NatsServer srv2 = runDefaultServer()) {
                    // Make sure we got the reconnected cb
                    assertTrue("Failed to get reconnected cb",
                            await(rcbLatch, 10, TimeUnit.SECONDS));

                    for (int i = 0; i < total; i++) {
                        nc.publish("foo", String.format("Hello %d", i).getBytes());
                    }
                    nc.flush();


                    while (received.get() < max) {
                        sleep(1);
                    }

                    exec.shutdownNow();

                    assertTrue("Subscriber threads should have completed",
                            wg.await(5, TimeUnit.SECONDS));
                    assertEquals("Wrong number of msgs received: ", max, received.get());
                }
            }
        }
    }

    @Test
    public void testAutoUnsubscribeFromCallback() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                try (SyncSubscription s = nc.subscribeSync("foo")) {
                    int max = 10;
                    final long resetUnsubMark = (long) max / 2;
                    final long limit = 100L;
                    final AtomicLong received = new AtomicLong(0);
                    byte[] msg = "Hello".getBytes();

                    // Auto-unsubscribe within the callback with a value lower
                    // than what was already received.

                    Subscription sub = nc.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            long rcvd = received.incrementAndGet();
                            if (rcvd == resetUnsubMark) {
                                try {
                                    msg.getSubscription().autoUnsubscribe((int) rcvd - 1);
                                    nc.flush();
                                } catch (Exception e) {
                                    /* NOOP */
                                }
                            }
                            if (rcvd == limit) {
                                // Something went wrong... fail now
                                fail("Got more messages than expected");
                            }
                            try {
                                nc.publish("foo", msg.getData());
                            } catch (IOException e) {
                                /* NOOP */
                            }
                        }
                    });
                    sub.autoUnsubscribe(max);
                    nc.flush();

                    // Trigger the first message, the other are sent from the callback.
                    nc.publish("foo", msg);
                    sleep(100, TimeUnit.MILLISECONDS);

                    long rcvd = received.get();
                    assertEquals(
                            String.format(
                                    "Wrong number of received messages. Original max was %d reset "
                                            + "to %d, actual received: %d",
                                    max, resetUnsubMark, rcvd),
                            resetUnsubMark, rcvd);

                    // Now check with AutoUnsubscribe with higher value than original
                    received.set(0);
                    final long newMax = (long) 2 * max;

                    sub = nc.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            long rcvd = received.incrementAndGet();
                            if (rcvd == resetUnsubMark) {
                                try {
                                    msg.getSubscription().autoUnsubscribe((int) newMax);
                                    nc.flush();
                                } catch (Exception e) {
                                    /* NOOP */
                                }
                            }
                            if (rcvd == limit) {
                                // Something went wrong... fail now
                                fail("Got more messages than expected");
                            }
                            try {
                                nc.publish("foo", msg.getData());
                            } catch (IOException e) {
                                /* NOOP */
                            }
                        }
                    });
                    sub.autoUnsubscribe(max);
                    nc.flush();

                    // Trigger the first message, the other are sent from the callback.
                    nc.publish("foo", msg);
                    nc.flush();

                    sleep(100, TimeUnit.MILLISECONDS);

                    rcvd = received.get();
                    assertEquals(
                            String.format(
                                    "Wrong number of received messages. Original max was %d reset "
                                            + "to %d, actual received: %d",
                                    max, newMax, rcvd),
                            newMax, rcvd);
                }
            }
        }
    }

    @Test
    public void testCloseSubRelease() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                try (SyncSubscription sub = nc.subscribeSync("foo")) {
                    long start = System.nanoTime();
                    exec.submit(new Runnable() {
                        public void run() {
                            sleep(5);
                            nc.close();
                        }
                    });
                    boolean exThrown = false;
                    try {
                        sleep(100);
                        assertTrue(nc.isClosed());
                        sub.nextMessage(50, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Expected an error from nextMsg", exThrown);
                    }
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                    String msg = String.format("Too much time has elapsed to release NextMsg: %dms",
                            elapsed);
                    assertTrue(msg, elapsed <= 200);
                }
            }
        }
    }

    @Test
    public void testIsValidSubscriber() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {

            try (final Connection nc = newDefaultConnection()) {
                try (SyncSubscription sub = nc.subscribeSync("foo")) {
                    assertTrue("Subscription should be valid", sub.isValid());

                    for (int i = 0; i < 10; i++) {
                        nc.publish("foo", "Hello".getBytes());
                    }
                    nc.flush();

                    try {
                        sub.nextMessage(200);
                    } catch (Exception e) {
                        fail("nextMsg threw an exception: " + e.getMessage());
                    }

                    sub.unsubscribe();


                    boolean exThrown = false;
                    try {
                        sub.autoUnsubscribe(1);
                    } catch (Exception e) {
                        assertTrue(e instanceof IllegalStateException);
                        assertEquals(ERR_BAD_SUBSCRIPTION, e.getMessage());
                        exThrown = true;
                    } finally {
                        assertTrue("nextMsg should have thrown an exception", exThrown);
                    }

                    exThrown = false;
                    try {
                        sub.nextMessage(200);
                        fail("Shouldn't be here");
                    } catch (Exception e) {
                        assertTrue(e instanceof IllegalStateException);
                        assertEquals(ERR_BAD_SUBSCRIPTION, e.getMessage());
                        exThrown = true;
                    } finally {
                        assertTrue("nextMsg should have thrown an exception", exThrown);
                    }
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testSlowSubscriber() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_SLOW_CONSUMER);

        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                try (SyncSubscriptionImpl sub = (SyncSubscriptionImpl) nc.subscribeSync("foo")) {
                    sub.setPendingLimits(100, 1024);

                    for (int i = 0; i < 200; i++) {
                        nc.publish("foo", "Hello".getBytes());
                    }

                    int timeout = 5000;
                    long t0 = System.nanoTime();
                    nc.flush(timeout);
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertFalse(String.format("Flush did not return before timeout: %d >= %d",
                            elapsed, timeout), elapsed >= timeout);
                    sub.nextMessage(200);
                }
            }
        }
    }

    @Test
    public void testSlowAsyncSubscriber() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        final CountDownLatch mcbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = cf.createConnection()) {
                c.setExceptionHandler(null);
                try (final AsyncSubscriptionImpl s =
                             (AsyncSubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                                 public void onMessage(Message msg) {
                                     try {
                                         mcbLatch.await();
                                     } catch (InterruptedException e) {
                                    /* NOOP */
                                     }
                                 }
                             })) {

                    int pml = s.getPendingMsgsLimit();
                    assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_MSGS, pml);
                    int pbl = s.getPendingBytesLimit();
                    assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_BYTES, pbl);

                    // Set new limits
                    pml = 100;
                    pbl = 1024 * 1024;

                    s.setPendingLimits(pml, pbl);

                    assertEquals(pml, s.getPendingMsgsLimit());
                    assertEquals(pbl, s.getPendingBytesLimit());

                    for (int i = 0; i < (pml + 100); i++) {
                        c.publish("foo", "Hello".getBytes());
                    }

                    int flushTimeout = 5000;

                    long t0 = System.nanoTime();
                    long elapsed = 0L;
                    try {
                        c.flush(flushTimeout);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Should not have thrown exception: " + e.getMessage());
                    }

                    elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue("Flush did not return before timeout, elapsed msec=" + elapsed,
                            elapsed < flushTimeout);

                    assertTrue(c.getLastException() instanceof IOException);
                    assertEquals(Nats.ERR_SLOW_CONSUMER, c.getLastException().getMessage());

                    mcbLatch.countDown();
                }
            }
        }
    }

    @Test
    public void testAsyncErrHandler() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                final String subj = "async_test";
                final CountDownLatch blocker = new CountDownLatch(1);

                try (Subscription sub = nc.subscribe(subj, new MessageHandler() {
                    public void onMessage(Message msg) {
                        try {
                            blocker.await();
                        } catch (InterruptedException e) {
                            /* NOOP */
                        }
                    }
                })) {
                    final int limit = 10;
                    final int toSend = 100;

                    // Limit internal subchan length to trip condition easier
                    sub.setPendingLimits(limit, 1024);

                    final CountDownLatch testLatch = new CountDownLatch(1);
                    final AtomicInteger aeCalled = new AtomicInteger(0);

                    nc.setExceptionHandler(new ExceptionHandler() {
                        public void onException(NATSException ex) {
                            aeCalled.incrementAndGet();

                            assertEquals("Did not receive proper subscription", sub,
                                    ex.getSubscription());
                            assertTrue("Expected IOException, but got " + ex,
                                    ex.getCause() instanceof IOException);
                            assertEquals(ERR_SLOW_CONSUMER, ex.getCause().getMessage());

                            // Suppress additional calls
                            if (aeCalled.get() == 1) {
                                // release the test
                                testLatch.countDown();
                            }
                        }
                    });

                    byte[] msg = "Hello World!".getBytes();
                    // First one trips the wait in subscription callback
                    nc.publish(subj, msg);
                    nc.flush();
                    for (int i = 0; i < toSend; i++) {
                        nc.publish(subj, msg);
                    }
                    nc.flush();

                    assertTrue("Failed to call async err handler", await(testLatch));

                    // assertEquals("Wrong #delivered msgs;", limit, sub.getDelivered());
                    // assertEquals("Wrong max pending msgs;", limit, sub.getMaxPendingMsgs());

                    // Make sure dropped stats is correct
                    int dropped = toSend - limit;
                    assertEquals(String.format("Expected dropped to be %d, but was actually %d\n",
                            dropped, sub.getDropped()), dropped, sub.getDropped());
                    int ae = aeCalled.get();
                    assertEquals(
                            String.format("Expected err handler to be called once, got %d\n", ae),
                            1, ae);

                    // release the sub
                    blocker.countDown();

                    sub.unsubscribe();
                    sub.getDropped();
                } // AsyncSubscription
            } // Connection
        } // Server
    }

    @Test
    public void testAsyncSubscriberStarvation()
            throws IOException, TimeoutException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = newDefaultConnection()) {
                // Helper
                try (AsyncSubscription helper = c.subscribe("helper", new MessageHandler() {
                    public void onMessage(Message msg) {
                        // System.err.println("Helper");
                        sleep(100);
                        try {
                            c.publish(msg.getReplyTo(), "Hello".getBytes());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                })) {
                    // System.err.println("helper subscribed");
                    // Kickoff
                    try (AsyncSubscription start = c.subscribe("start", new MessageHandler() {
                        public void onMessage(Message msg) {
                            // System.err.println("Responder");
                            String responseInbox = c.newInbox();
                            c.subscribe(responseInbox, new MessageHandler() {
                                public void onMessage(Message msg) {
                                    // System.err.println("Internal subscriber.");
                                    sleep(100);
                                    latch.countDown();
                                }
                            });
                            // System.err.println("starter subscribed");
                            sleep(100);
                            try {
                                c.publish("helper", responseInbox, "Help me!".getBytes());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } // "start" onMessage
                    })) {
                        UnitTestUtilities.sleep(100);
                        c.publish("start", "Begin".getBytes());
                        // System.err.println("Started");
                        assertTrue("Was stalled inside of callback waiting on another callback",
                                await(latch));
                    } // Start
                } // Helper
            }
        }
    }

    @Test
    public void testAsyncSubscribersOnClose() throws Exception {
        final AtomicInteger callbacks = new AtomicInteger(0);
        int toSend = 10;
        final CountDownLatch mcbLatch = new CountDownLatch(toSend);
        MessageHandler mh = new MessageHandler() {
            public void onMessage(Message msg) {
                callbacks.getAndIncrement();
                try {
                    mcbLatch.await();
                } catch (InterruptedException e) {
                    /* NOOP */
                }
            }
        };

        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                try (AsyncSubscription sub = nc.subscribe("foo", mh)) {
                    for (int i = 0; i < toSend; i++) {
                        nc.publish("foo", "Hello World!".getBytes());
                    }
                    nc.flush();
                    sleep(10);
                    nc.close();

                    // Release callbacks
                    for (int i = 1; i < toSend; i++) {
                        mcbLatch.countDown();
                    }

                    // Wait for some time.
                    sleep(10, TimeUnit.MILLISECONDS);
                    int seen = callbacks.get();
                    assertEquals(String.format(
                            "Expected only one callback, received %d callbacks\n", seen), 1, seen);
                }
            } // Connection
        } // Server
    }

    @Test
    public void testNextMsgCallOnClosedSub()
            throws IOException, TimeoutException, InterruptedException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                try (SyncSubscription sub = nc.subscribeSync("foo")) {
                    sub.unsubscribe();
                    sub.nextMessage(1, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void testAsyncSubscriptionPending() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                // Send some messages to ourselves.
                int total = 100;
                byte[] msg = "0123456789".getBytes();

                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch blockLatch = new CountDownLatch(1);
                try (Subscription sub = nc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        latch.countDown();
                        await(blockLatch, 60, TimeUnit.SECONDS);
                    }
                })) {
                    for (int i = 0; i < total; i++) {
                        nc.publish("foo", msg);
                    }
                    nc.flush();

                    // Wait for a message to be received, so checks are safe
                    assertTrue("No message received", await(latch));

                    // Test old way
                    @SuppressWarnings("deprecation") int queued = sub.getQueuedMessageCount();
                    assertTrue(
                            String.format("Expected %d or %d, got %d\n", total, total - 1, queued),
                            (queued == total) || (queued == total - 1));

                    // new way, we ensure the same and check bytes
                    int pm = sub.getPendingMsgs();
                    assertTrue(String.format("Expected msgs to be %d or %d, got %d\n", total - 1,
                            total, pm), pm == total || pm == total - 1);

                    int pb = sub.getPendingBytes();
                    int mlen = msg.length;
                    long totalSize = total * mlen;

                    assertTrue(String.format("Expected bytes to be %d or %d, got %d\n", total - 1,
                            total, pb), pb == total * mlen || pb == (total - 1) * mlen);

                    // Make sure max has been set. Since we block after the first message is
                    // received, MaxPending should be >= total - 1 and <= total
                    int mpm = sub.getPendingMsgsMax();
                    long mpb = sub.getPendingBytesMax();

                    assertTrue(String.format("Expected max msgs (%d) to be between %d and %d\n",
                            mpm, total - 1, total), mpm == total - 1 || mpm == total);

                    assertTrue(
                            String.format("Expected max bytes (%d) to be between %d and %d\n", mpb,
                                    totalSize - mlen, totalSize),
                            mpb == totalSize || mpb == totalSize - mlen);

                    // Check that clear works.
                    sub.clearMaxPending();

                    mpm = sub.getPendingMsgsMax();
                    mpb = sub.getPendingBytesMax();

                    assertEquals(
                            String.format("Expected max msgs to be 0 vs %d after clearing\n", mpm),
                            0, mpm);
                    assertEquals(
                            String.format("Expected max bytes to be 0 vs %d after clearing\n", mpb),
                            0, mpb);

                    blockLatch.countDown();
                    sub.unsubscribe();

                    // These calls should fail once the subscription is closed.
                    boolean exThrown = false;
                    try {
                        sub.getPendingMsgs();
                    } catch (IllegalStateException e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }

                    exThrown = false;
                    try {
                        sub.getPendingBytes();
                    } catch (IllegalStateException e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }

                    exThrown = false;
                    try {
                        sub.getPendingMsgsMax();
                    } catch (IllegalStateException e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }

                    exThrown = false;
                    try {
                        sub.getPendingBytesMax();
                    } catch (IllegalStateException e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }

                    exThrown = false;
                    try {
                        sub.clearMaxPending();
                    } catch (IllegalStateException e) {
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }

                }
            }
        }
    }

    @Test
    public void testAsyncSubscriptionPendingDrain() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                // Send some messages to ourselves.
                int total = 100;
                byte[] msg = "0123456789".getBytes();

                Subscription sub = nc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                    }
                });
                for (int i = 0; i < total; i++) {
                    nc.publish("foo", msg);
                }
                nc.flush();

                // Wait for all delivered
                while (sub.getDelivered() < total) {
                    sleep(10, TimeUnit.MILLISECONDS);
                }
                assertEquals(
                        String.format("Expected 0 pending msgs, got %d\n", sub.getPendingMsgs()), 0,
                        sub.getPendingMsgs());
                assertEquals(
                        String.format("Expected 0 pending bytes, got %d\n", sub.getPendingBytes()),
                        0, sub.getPendingBytes());

                sub.unsubscribe();
                // Should throw exception
                sub.getDelivered();
            }
        }
    }

    @Test
    public void testSyncSubscriptionPendingDrain() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                // Send some messages to ourselves.
                int total = 100;
                byte[] msg = "0123456789".getBytes();

                try (SyncSubscription sub = nc.subscribeSync("foo")) {
                    for (int i = 0; i < total; i++) {
                        nc.publish("foo", msg);
                    }
                    nc.flush();

                    // Wait for all delivered
                    while (sub.getDelivered() < total) {
                        sub.nextMessage(10, TimeUnit.MILLISECONDS);
                    }
                    assertEquals(String.format("Expected 0 pending msgs, got %d\n",
                            sub.getPendingMsgs()), 0, sub.getPendingMsgs());
                    assertEquals(String.format("Expected 0 pending bytes, got %d\n",
                            sub.getPendingBytes()), 0, sub.getPendingBytes());

                    sub.unsubscribe();

                    // Should throw exception
                    sub.getDelivered();
                }
            }
        }
    }

    @Test
    public void testSyncSubscriptionPending() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                // Send some messages to ourselves.
                int total = 100;
                byte[] msg = "0123456789".getBytes();

                try (SyncSubscription sub = nc.subscribeSync("foo")) {
                    for (int i = 0; i < total; i++) {
                        nc.publish("foo", msg);
                    }
                    nc.flush();

                    // Test old way
                    @SuppressWarnings("deprecation") int queued = sub.getQueuedMessageCount();
                    assertTrue(
                            String.format("Expected %d or %d, got %d\n", total, total - 1, queued),
                            (queued == total) || (queued == total - 1));

                    // new way, we ensure the same and check bytes
                    int msgs = sub.getPendingMsgs();

                    assertEquals(total, msgs);

                    int bytes = sub.getPendingBytes();
                    int mlen = msg.length;
                    assertEquals(total * mlen, bytes);

                    // Now drain some down and make sure pending is correct
                    for (int i = 0; i < total - 1; i++) {
                        sub.nextMessage(10, TimeUnit.MILLISECONDS);
                    }

                    msgs = sub.getPendingMsgs();
                    bytes = sub.getPendingBytes();

                    assertEquals(1, msgs);
                    assertEquals(mlen, bytes);
                }
            }
        }
    }

    private void send(Connection nc, String subject, byte[] payload, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            nc.publish(subject, payload);
        }
        nc.flush();
    }

    private void checkPending(Subscription sub, int limitCount, int limitBytes, int expectedCount,
                              int expectedBytes, int payloadLen) {
        int lc = sub.getPendingMsgsLimit();
        int lb = sub.getPendingBytesLimit();
        String errMsg =
                String.format("Unexpected limits, expected %d msgs %d bytes, got %d msgs %d bytes",
                        limitCount, limitBytes, lc, lb);
        assertTrue(errMsg, lc == limitCount && lb == limitBytes);
        int msgs = sub.getPendingMsgs();
        int bytes = sub.getPendingBytes();

        errMsg = String.format("Unexpected counts, expected %d msgs %d bytes, got %d msgs %d bytes",
                expectedCount, expectedBytes, msgs, bytes);
        assertTrue(errMsg, msgs == expectedCount || msgs == expectedCount - 1);
        assertTrue(errMsg, bytes == expectedBytes || bytes == expectedBytes - payloadLen);
    }

    class MyCb implements MessageHandler {
        final CountDownLatch recvLatch;
        final CountDownLatch blockLatch;

        MyCb(CountDownLatch recvLatch, CountDownLatch blockLatch) {
            this.recvLatch = recvLatch;
            this.blockLatch = blockLatch;
        }

        @Override
        public void onMessage(Message msg) {
            recvLatch.countDown();
            await(blockLatch, 60, TimeUnit.SECONDS);
            try {
                msg.getSubscription().unsubscribe();
            } catch (IOException e) {
                /* NOOP */
            }
        }

    }

    @Test
    public void testSetPendingLimits() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection nc = newDefaultConnection()) {
                final byte[] payload = "hello".getBytes();
                final int payloadLen = payload.length;
                final int toSend = 100;

                CountDownLatch recv = new CountDownLatch(1);
                CountDownLatch block = new CountDownLatch(1);

                int expectedBytes;
                int expectedCount;
                String subj = "foo";
                try (Subscription sub = nc.subscribe(subj, new MyCb(recv, block))) {
                    // Check we apply limit only for size
                    int limitCount = -1;
                    int limitBytes = (toSend / 2) * payload.length;
                    sub.setPendingLimits(limitCount, limitBytes);

                    // send messages
                    send(nc, subj, payload, toSend);
                    // Wait for message to be received
                    assertTrue("Did not get our message", await(recv));
                    recv = new CountDownLatch(1);

                    expectedBytes = limitBytes;
                    expectedCount = limitBytes / payload.length;
                    this.checkPending(sub, limitCount, limitBytes, expectedCount, expectedBytes,
                            payload.length);

                    // Release callback
                    block.countDown();
                    block = new CountDownLatch(1);

                    subj = "bar";
                    try (Subscription sub2 = nc.subscribe(subj, new MyCb(recv, block))) {
                        limitCount = toSend / 4;
                        limitBytes = -1;
                        sub2.setPendingLimits(limitCount, limitBytes);

                        // Send messages
                        send(nc, subj, payload, toSend);
                        // Wait for message to be received
                        assertTrue("Did not get our message", await(recv));
                        recv = new CountDownLatch(1);

                        expectedCount = limitCount;
                        expectedBytes = limitCount * payload.length;
                        checkPending(sub2, limitCount, limitBytes, expectedCount, expectedBytes,
                                payload.length);

                        // Release callback
                        block.countDown();
                        block = new CountDownLatch(1);

                        subj = "baz";
                        try (Subscription sub3 = nc.subscribe(subj, new MyCb(recv, block))) {
                            limitCount = -1;
                            limitBytes = (toSend / 2) * payload.length;
                            sub3.setPendingLimits(limitCount, limitBytes);

                            // Send messages
                            send(nc, subj, payload, toSend);
                            expectedBytes = limitBytes;
                            expectedCount = limitBytes / payload.length;
                            checkPending(sub3, limitCount, limitBytes, expectedCount, expectedBytes,
                                    payload.length);
                            sub3.unsubscribe();
                            nc.flush();

                            subj = "baz";
                            try (Subscription sub4 = nc.subscribe(subj, new MyCb(recv, block))) {
                                limitCount = -1;
                                limitBytes = (toSend / 2) * payload.length;
                                sub4.setPendingLimits(limitCount, limitBytes);

                                // Send messages
                                send(nc, subj, payload, toSend);
                                expectedBytes = limitBytes;
                                expectedCount = limitBytes / payload.length;
                                checkPending(sub4, limitCount, limitBytes, expectedCount,
                                        expectedBytes, payload.length);
                                sub4.unsubscribe();
                                nc.flush();
                            }

                        }

                    }
                }

            }
        }
    }

    @Test
    public void testManyRequests() throws Exception {
        final int numRequests = 1000;
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection conn = newDefaultConnection()) {
                try (final Connection pub = newDefaultConnection()) {
                    try (Subscription sub = conn.subscribe("foo", "bar", new MessageHandler() {
                        public void onMessage(Message message) {
                            try {
                                conn.publish(message.getReplyTo(), "hello".getBytes());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    })) {
                        conn.flush();
                        for (int i = 0; i < numRequests; i++) {
                            assertNotNull(String.format("timed out after %d msgs", i + 1),
                                    pub.request("foo", "blah".getBytes(), 5000));
                        } // for
                    } // Subscription
                } // pub
            } // conn
        } // server
    }
}
