/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.ERR_NO_SERVERS;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;
import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Category(IntegrationTest.class)
public class ITBasicTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ITBasicTest.class);

    private static final LogVerifier verifier = new LogVerifier();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    private final ExecutorService executor = Executors.newCachedThreadPool();
    UnitTestUtilities utils = new UnitTestUtilities();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    /**
     * @throws java.lang.Exception if a problem occurs
     */
    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @Test
    public void testConnectedServer() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl conn = (ConnectionImpl) newDefaultConnection()) {

                String url = conn.getConnectedUrl();
                String badUrl = String.format("Unexpected connected URL of %s\n", url);
                assertNotNull(badUrl, url);
                assertEquals(badUrl, Nats.DEFAULT_URL, url);

                assertNotNull(conn.currentServer().toString());
                assertTrue(conn.currentServer().toString().contains(Nats.DEFAULT_URL));

                String srvId = conn.getConnectedServerId();
                assertNotNull("Expected a connected server id", srvId);

                conn.close();
                url = conn.getConnectedUrl();
                srvId = conn.getConnectedServerId();
                assertNull(url);
                assertNull(srvId);
            }
        }
    }

    @Test
    public void testMultipleClose() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = newDefaultConnection()) {
                List<Callable<String>> callables = new ArrayList<Callable<String>>(10);
                for (int i = 0; i < 10; i++) {
                    final int index = i;
                    callables.add(new Callable<String>() {
                        public String call() throws Exception {
                            c.close();
                            return "Task " + index;
                        }
                    });
                }
                List<Future<String>> futures = executor.invokeAll(callables);
            }
        }
    }

    @Test
    public void testBadOptionTimeoutConnect() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);
        try (NatsServer srv = runDefaultServer()) {
            Options opts = new Options.Builder(defaultOptions()).timeout(-1).build();
            try (Connection conn = Nats.connect(Nats.DEFAULT_URL, opts)) {
                fail("Should not have connected");
            }
        }
    }

    @Test
    public void testSimplePublish() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                c.publish("foo", "Hello World".getBytes());
            }
        }
    }

    @Test
    public void testSimplePublishNoData() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                c.publish("foo", null);
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void testSimplePublishError() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                c.publish(null, "Hello World!".getBytes());
            }
        }
    }

    @Test
    public void testPublishDoesNotFailOnSlowConsumer() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl nc = (ConnectionImpl) newDefaultConnection()) {
                try (SyncSubscriptionImpl sub = (SyncSubscriptionImpl) nc.subscribeSync("foo")) {
                    sub.setPendingLimits(1, 1000);

                    byte[] msg = "Hello".getBytes();
                    IOException pubErr = null;
                    for (int i = 0; i < 10; i++) {
                        try {
                            nc.publish("foo", msg);
                        } catch (IOException e) {
                            pubErr = e;
                            break;
                        }
                        nc.flush();
                    }
                    assertNull("publish() should not fail because of slow consumer.", pubErr);

                    assertNotNull(nc.getLastException());
                    assertTrue(nc.getLastException() instanceof IOException);
                    assertEquals(ERR_SLOW_CONSUMER, nc.getLastException().getMessage());
                }
            }
        }
    }

    private boolean compare(byte[] p1, byte[] p2) {
        return Arrays.equals(p1, p2);
    }

    @Test
    public void testAsyncSubscribe() throws Exception {
        final byte[] omsg = "Hello World".getBytes();

        final CountDownLatch latch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        assertArrayEquals("Message received does not match", omsg, msg.getData());

                        assertNotNull("Callback does not have a valid Subscription",
                                msg.getSubscription());
                        latch.countDown();
                    }

                })) {

                    c.publish("foo", omsg);
                    c.flush();
                    assertTrue("Did not receive message.", await(latch));
                } // AsyncSubscription
            }
        }
    }

    @Test
    public void testSyncSubscribe() throws Exception {
        final byte[] omsg = "Hello World".getBytes();
        int timeoutMsec = 1000;

        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (SyncSubscription s = c.subscribeSync("foo")) {
                    try {
                        Thread.sleep(100);
                        c.publish("foo", omsg);
                        Message msg = s.nextMessage(timeoutMsec);
                        assertArrayEquals("Messages are not equal.", omsg, msg.getData());
                    } catch (InterruptedException e) {
                        /* swallow */
                    }
                }
            }
        }
    }

    @Test
    public void testPubSubWithReply() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (SyncSubscription s = c.subscribeSync("foo")) {
                    final byte[] omsg = "Hello World".getBytes();
                    c.publish("foo", "reply", omsg);
                    c.flush();
                    try {
                        Thread.sleep(100);
                        Message msg = s.nextMessage(10000);
                        assertArrayEquals("Message received does not match: ", omsg, msg.getData());
                    } catch (InterruptedException e) {
                        /* swallow */
                    }
                }
            }
        }
    }

    @Test
    public void testFlush() throws Exception {
        final byte[] omsg = "Hello World".getBytes();
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                c.subscribeSync("foo");
                c.publish("foo", "reply", omsg);
                c.flush();
            }
        }
    }

    @Test
    public void testQueueSubscriber() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                SyncSubscription s1 = c.subscribeSync("foo", "bar");
                SyncSubscription s2 = c.subscribeSync("foo", "bar");
                final byte[] omsg = "Hello World".getBytes();
                c.publish("foo", omsg);
                c.flush();

                int r1 = s1.getQueuedMessageCount();
                int r2 = s2.getQueuedMessageCount();
                assertEquals("Received too many messages for multiple queue subscribers", 1,
                        r1 + r2);

                // Drain the messages.
                s1.nextMessage(250, TimeUnit.MILLISECONDS);
                assertEquals(0, s1.getQueuedMessageCount());

                s2.nextMessage(250, TimeUnit.MILLISECONDS);
                assertEquals(0, s2.getQueuedMessageCount());

                int total = 1000;
                for (int i = 0; i < total; i++) {
                    c.publish("foo", omsg);
                }
                c.flush();

                SyncSubscriptionImpl si1 = (SyncSubscriptionImpl) s1;
                SyncSubscriptionImpl si2 = (SyncSubscriptionImpl) s2;
                assertEquals(total, si1.getChannel().size() + si2.getChannel().size());

                final int variance = (int) (total * 0.15);
                r1 = s1.getQueuedMessageCount();
                r2 = s2.getQueuedMessageCount();
                assertEquals("Incorrect total number of messages: ", total, r1 + r2);

                double expected = total / 2;
                int d1 = (int) Math.abs((expected - r1));
                int d2 = (int) Math.abs((expected - r2));
                if (d1 > variance || d2 > variance) {
                    fail(String.format("Too much variance in totals: %d, %d > %d", d1, d2,
                            variance));
                }
            }
        }
    }

    @Test
    public void testReplyArg() throws Exception {
        final String replyExpected = "bar";

        final CountDownLatch latch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        assertEquals(replyExpected, msg.getReplyTo());
                        latch.countDown();
                    }
                })) {
                    sleep(200);
                    c.publish("foo", "bar", null);
                    assertTrue("Message not received.", await(latch));
                }
            }
        }
    }

    @Test
    public void testSyncReplyArg() throws Exception {
        String replyExpected = "bar";
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (SyncSubscription s = c.subscribeSync("foo")) {
                    sleep(500);
                    c.publish("foo", replyExpected, null);
                    Message m = null;
                    try {
                        m = s.nextMessage(1000);
                    } catch (Exception e) {
                        fail("Received an err on nextMsg(): " + e.getMessage());
                    }
                    assertEquals(replyExpected, m.getReplyTo());
                }
            }
        }
    }

    @Test
    public void testUnsubscribe() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger(0);
        final int max = 20;
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (final AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message m) {
                        count.incrementAndGet();
                        if (count.get() == max) {
                            try {
                                m.getSubscription().unsubscribe();
                                assertFalse(m.getSubscription().isValid());
                            } catch (Exception e) {
                                fail("Unsubscribe failed with err: " + e.getMessage());
                            }
                            latch.countDown();
                        }
                    }
                })) {
                    for (int i = 0; i < max; i++) {
                        c.publish("foo", null, null);
                    }
                    sleep(100);
                    c.flush();

                    if (s.isValid()) {
                        assertTrue("Test complete signal not received", await(latch));
                        assertFalse(s.isValid());
                    }
                    assertEquals(max, count.get());
                }
            }
        }
    }

    @Test
    public void testDoubleUnsubscribe() throws Exception {
        thrown.expect(IllegalStateException.class);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (SyncSubscription s = c.subscribeSync("foo")) {
                    s.unsubscribe();
                    s.unsubscribe();
                }
            }
        }
    }

    @Test
    public void testRequestTimeout() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                assertFalse(c.isClosed());
                assertNull("should time out", c.request("foo", "help".getBytes(), 10));
            }
        }
    }

    @Test
    public void testRequest() throws Exception {
        final byte[] response = "I will help you.".getBytes();
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = newDefaultConnection()) {
                sleep(100);
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        try {
                            c.publish(msg.getReplyTo(), response);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                })) {
                    sleep(100);
                    final byte[] request = "help".getBytes();
                    Message msg = null;
                    try {
                        msg = c.request("foo", request, 5, TimeUnit.SECONDS);
                        assertNotNull(msg);
                        assertArrayEquals("Received invalid response", response, msg.getData());
                    } catch (Exception e) {
                        fail("Request failed: " + e.getMessage());
                    }

                    msg = null;
                    try {
                        msg = c.request("foo", request);
                        assertNotNull("Response message shouldn't be null", msg);
                        assertArrayEquals("Response isn't valid.", response, msg.getData());
                    } catch (Exception e) {
                        fail("Request failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void testRequestNoBody() throws Exception {
        final byte[] response = "I will help you.".getBytes();
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = newDefaultConnection()) {
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        try {
                            c.publish(msg.getReplyTo(), response);
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail(e.getMessage());
                        }
                    }
                })) {
                    UnitTestUtilities.sleep(100);
                    Message msg = c.request("foo", null, 500);
                    assertNotNull("Request shouldn't time out", msg);
                    assertArrayEquals("Response isn't valid.", response, msg.getData());
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testFlushInHandler() throws Exception {
        final CountDownLatch mcbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        try {
                            c.flush();
                            mcbLatch.countDown();
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                })) {
                    c.publish("foo", "Hello".getBytes());
                    assertTrue("Flush did not return properly in callback",
                            mcbLatch.await(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testReleaseFlush() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(Nats.ERR_CONNECTION_CLOSED);

        try (NatsServer srv = runDefaultServer()) {
            Options opts = new Options.Builder().noReconnect().build();
            try (final ConnectionImpl nc = (ConnectionImpl) spy(newDefaultConnection())) {
                for (int i = 0; i < 1000; i++) {
                    nc.publish("foo", "Hello".getBytes());
                }

                final int timeout = 5000;

                // Shunt the flush ping
                OutputStream bwMock = mock(OutputStream.class);
                nc.setOutputStream(bwMock);

                final CountDownLatch latch = new CountDownLatch(1);
                executor.execute(new Runnable() {
                    public void run() {
                        await(latch);
                        sleep(100, TimeUnit.MILLISECONDS);
                        nc.close();
                        verify(nc, times(1)).clearPendingFlushCalls();
                    }
                });
                latch.countDown();
                nc.flush(timeout);
            }
        }
    }

    @Test
    public void testCloseAndDispose() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                c.close();
            }
        }
    }

    @Test
    public void testInbox() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                String inbox = c.newInbox();
                assertFalse("inbox was null or whitespace",
                        inbox == null || inbox.trim().length() == 0);
                assertTrue("Bad INBOX format", inbox.startsWith("_INBOX."));
            }
        }
    }

    @Test
    public void testStats() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                byte[] data = "The quick brown fox jumped over the lazy dog".getBytes();
                int iter = 10;

                for (int i = 0; i < iter; i++) {
                    c.publish("foo", data);
                }

                Statistics stats = c.getStats();
                assertEquals("Not properly tracking OutMsgs: ", iter, stats.getOutMsgs());
                assertEquals("Not properly tracking OutBytes: ", iter * data.length,
                        stats.getOutBytes());

                c.resetStats();

                // Test both sync and async versions of subscribe.
                try (AsyncSubscription s1 = c.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                    }
                })) {
                    try (SyncSubscription s2 = c.subscribeSync("foo")) {
                        for (int i = 0; i < iter; i++) {
                            c.publish("foo", data);
                        }
                        c.flush();

                        stats = c.getStats();
                        stats.toString();
                        assertEquals("Not properly tracking InMsgs: ", 2 * iter, stats.getInMsgs());
                        assertEquals("Not properly tracking InBytes: ", 2 * iter * data.length,
                                stats.getInBytes());
                    }
                }
            }
        }
    }

    @Test
    public void testRaceSafeStats() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            c.publish("foo", null);
                        } catch (IllegalStateException | IOException e) {
                            fail(e.getMessage());
                        }
                    }
                });
                sleep(1000);
                assertEquals(1, c.getStats().getOutMsgs());
            }
        }
    }

    @Test
    public void testLargeMessage() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (final Connection c = newDefaultConnection()) {
                int msgSize = 51200;
                final byte[] omsg = new byte[msgSize];
                for (int i = 0; i < msgSize; i++) {
                    {
                        omsg[i] = (byte) 'A';
                    }
                }

                omsg[msgSize - 1] = (byte) 'Z';

                final CountDownLatch latch = new CountDownLatch(1);
                try (AsyncSubscription sub = c.subscribe("foo", new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        assertTrue("Response isn't valid.", compare(omsg, msg.getData()));
                        latch.countDown();
                    }
                })) {

                    c.publish("foo", omsg);
                    c.flush(1000);
                    assertTrue("Didn't receive callback message",
                            await(latch, 2, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testSendAndRecv() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                final CountDownLatch mhLatch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger();
                final int count = 1000;
                try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        received.incrementAndGet();
                        if (received.get() == count) {
                            mhLatch.countDown();
                        }
                    }
                })) {
                    for (int i = 0; i < count; i++) {
                        c.publish("foo", null);
                    }
                    c.flush();
                    assertTrue(String.format("Received (%s) != count (%s)", received, count),
                            mhLatch.await(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testLargeSubjectAndReply() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
                int size = 1066;
                byte[] subjBytes = new byte[size];
                for (int i = 0; i < size; i++) {
                    subjBytes[i] = 'A';
                }
                final String subject = new String(subjBytes);

                byte[] replyBytes = new byte[size];
                for (int i = 0; i < size; i++) {
                    replyBytes[i] = 'A';
                }
                final String reply = new String(replyBytes);

                final CountDownLatch latch = new CountDownLatch(1);
                try (AsyncSubscription s = c.subscribe(subject, new MessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                        assertEquals(subject.length(), msg.getSubject().length());
                        assertEquals(subject, msg.getSubject());
                        assertEquals(reply.length(), msg.getReplyTo().length());
                        assertEquals(reply, msg.getReplyTo());
                        latch.countDown();
                    }
                })) {

                    c.publish(subject, reply, null);
                    c.flush(5000);
                    assertTrue(await(latch));
                }
            }
        }
    }

    public class CountHandler implements MessageHandler {

        AtomicInteger counter = new AtomicInteger();

        CountHandler(AtomicInteger c) {
            this.counter = c;
        }

        @Override
        public void onMessage(Message msg) {
            counter.incrementAndGet();
        }

    }

    @Test
    public void testManyRequests() throws Exception {
        int numMsgs = 500;
        // setLogLevel(Level.TRACE);

        try (NatsServer srv = runDefaultServer()) {
            Options opts = new Options.Builder().noReconnect().build();
            try (final Connection conn = newDefaultConnection()) {
                try (Subscription s = conn.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message message) {
                        try {
                            conn.publish(message.getReplyTo(), "response".getBytes());
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail(e.getMessage());
                        }
                    }
                })) {
                    for (int i = 0; i < numMsgs; i++) {
                        assertNotNull(String.format("timed out: %d", i),
                                conn.request("foo", "request".getBytes(), 2, TimeUnit.SECONDS));
                    }
                }
            }
        }
    }
}
