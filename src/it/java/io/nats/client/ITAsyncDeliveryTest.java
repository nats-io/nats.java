package io.nats.client;

import io.nats.client.AsyncDeliveryQueueTest.SubscriberKey;
import io.nats.client.IntegrationTest;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/*
 * The MIT License
 *
 * Copyright 2017 Apcera, Inc..
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
/**
 *
 * @author Tim Boudreau
 */
@Category(IntegrationTest.class)
public class ITAsyncDeliveryTest {

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

    @Test(timeout = 480000)
    public void testThreadPerSubscription() throws Throwable {
        boolean batched = false;
        System.out.println("\n\n\n**************** testThreadPerSubscription *********************\n\n\n");
        EH errors = new EH();
        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl conn = (ConnectionImpl) connection(batched, errors)) {
                String msgPrefix = batched ? "batched-mode " : "thread-per-subscriber ";
                testAsyncDelivery(conn, msgPrefix, errors);
                errors.rethrow();
            }
        }
    }

    @Test(timeout = 480000)
    public void testBatchedSubscriptions() throws Throwable {
        boolean batched = true;
        System.out.println("\n\n\n**************** testBatchedSubscriptions *********************\n\n\n");
        EH errors = new EH();
        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl conn = (ConnectionImpl) connection(batched, errors)) {
                String msgPrefix = batched ? "batched-mode " : "thread-per-subscriber ";
                testAsyncDelivery(conn, msgPrefix, errors);
                errors.rethrow();
            }
        }
    }

    static class EH implements ExceptionHandler {

        Throwable thrown = null;

        synchronized void rethrow() throws Throwable {
            if (thrown != null) {
                throw thrown;
            }
        }

        volatile Thread testThread;

        void pickUpTestThread() {
            testThread = Thread.currentThread();
        }

        @Override
        public synchronized void onException(NATSException ex) {
            Throwable t = ex;
            while (t.getCause() != null) {
                t = t.getCause();
                if (t.getCause() == null) {
                    break;
                }
            }
            if (thrown != null) {
                thrown.addSuppressed(t);
            } else {
                thrown = t;
            }
            if (testThread != null) {
                testThread.interrupt();
            }
        }

    }

    static Connection connection(boolean batched, ExceptionHandler errors) throws IOException {
        Options.Builder builder = new Options.Builder().errorCb(errors);
        if (batched) {
            // Intentionally have more threads than channels, so there is the
            // possibility to have two threads get a batch of messages at the
            // same time and delver them out of sequence - this means we actually
            // test the thread affinity code in AsyncDispatchQueue.
            builder.subscriptionDispatchPool(Executors.newFixedThreadPool(16));
        }
        return builder.build().connect();
    }

    final AsyncDeliveryQueueTest.SubscriberKey[] keys = new SubscriberKey[]{
        AsyncDeliveryQueueTest.keyA,
        AsyncDeliveryQueueTest.keyB,
        AsyncDeliveryQueueTest.keyC,
        AsyncDeliveryQueueTest.keyD,
        AsyncDeliveryQueueTest.keyE,
        AsyncDeliveryQueueTest.keyF,
        AsyncDeliveryQueueTest.keyG,};

    void testAsyncDelivery(Connection conn, String msgPrefix, EH errors) throws Throwable {
        System.out.println("TEST DELIVERY " + msgPrefix);
        List<Message> messages = new CopyOnWriteArrayList<>();
        List<MsgHandler> handlers = new ArrayList<>();
        int numMessagesPerKey = 151;
        int batchSize = 31;
        int totalPerKey = numMessagesPerKey * batchSize;
        for (int i = 0; i < numMessagesPerKey; i++) {
            for (SubscriberKey key : keys) {
                messages.addAll(AsyncDeliveryQueueTest.messageBatch(key, batchSize));
            }
        }
        CountDownLatch allProcessed = new CountDownLatch(totalPerKey);
        for (SubscriberKey key : keys) {
            MsgHandler mh = new MsgHandler(msgPrefix, allProcessed, key);
            handlers.add(mh);
            conn.subscribe(key.name, mh);
        }
        Thread.sleep(300); // XXX messagehandler really needs a hook to identify that
        // it is being used as a receiver of messages - i.e. the library has set up
        // threads, etc.
        for (int i = 0; i < messages.size(); i++) {
            conn.publish(messages.get(i));
            // Occasionally get out of the way
            if (i % 31 == 0) {
                Thread.sleep(50);
            }
            if (i % 300 == 0) {
                System.out.println("...published " + i + " so far...");
            }
            if (Thread.interrupted()) {
                errors.rethrow();
            }
        }
        System.out.println("Published " + messages.size() + " messages");
        allProcessed.await(120, TimeUnit.SECONDS);
        errors.rethrow();
        Thread.sleep(1200);
        System.out.println("Exit wait on subscribers");
        for (MsgHandler h : handlers) {
            System.out.println(h.sub + " RECEIVED " + h.deliveredCount());
        }

        int total = 0;
        Collections.reverse(handlers);
        for (MsgHandler h : handlers) {
            int count = h.deliveredCount();
            total += count;
            h.assertArrivedInOrder();
        }
        for (MsgHandler h : handlers) {
            h.assertAllMessagesDelivered(totalPerKey, messages);
        }
        assertEquals(messages.size(), total);
    }

    static final AtomicInteger TOTAL_ORDER = new AtomicInteger();

    static final class MsgHandler implements MessageHandler {

        private final String pfx;

        private final CountDownLatch latch;
        private final Map<Integer, Message> order = new ConcurrentHashMap<>();

        String sub;
        private final SubscriberKey key;

        public MsgHandler(String pfx, CountDownLatch latch, SubscriberKey key) {
            this.pfx = pfx;
            this.latch = latch;
            this.key = key;
        }

        @Override
        public void onMessage(Message msg) {
            if (order.isEmpty()) {
                System.out.println(pfx + "Received first: " + msg.getSubject());
            }
            if (sub != null) {
                // Error handler will catch
                assertEquals(sub, msg.getSubject());
            }
            sub = msg.getSubject();
            order.put(TOTAL_ORDER.getAndIncrement(), msg);
            latch.countDown();
            if (order.size() % 200 == 0) {
                System.out.println(pfx + "Received " + order.size() + " for " + msg.getSubject());
            }
        }

        int deliveredCount() {
            return order.size();
        }

        void assertAllMessagesDelivered(int expectedCount, List<Message> allMessages) {
            // Convert messages to a wrapper class that implements comparable on the byte payload (which is
            // just a long stored in a byte array) and implements proper equals/hashcode.
            List<AsyncDeliveryQueueTest.ComparableMessageWrapper> missing = new ArrayList<>();
            List<AsyncDeliveryQueueTest.ComparableMessageWrapper> wrappers = AsyncDeliveryQueueTest.toWrappers(allMessages);
            Set<AsyncDeliveryQueueTest.ComparableMessageWrapper> myWrappers
                    = new LinkedHashSet<>(AsyncDeliveryQueueTest.toWrappers(new ArrayList<>(order.values())));

            int totalForSubject = 0;
            for (AsyncDeliveryQueueTest.ComparableMessageWrapper m : wrappers) {
                if (key.name.equals(m.message.getSubject())) {
                    totalForSubject++;
                    if (!myWrappers.contains(m)) {
                        missing.add(m);
                    }
                }
            }
            if (!missing.isEmpty()) {
                Collections.sort(missing);
                fail(pfx + sub + " missing messages " + missing);
            }
            assertEquals("Message counts differ", totalForSubject, myWrappers.size());
        }

        void assertArrivedInOrder() {
            List<Message> arrivalOrder = new ArrayList<>();
            collectInOrder(arrivalOrder);
            AsyncDeliveryQueueTest.assertListSorted(pfx + ": Messages arrived out of order", arrivalOrder);
        }

        void collectInOrder(List<? super Message> into) {
            List<Integer> orderedKeys = new ArrayList<>(order.keySet());
            Collections.sort(orderedKeys);
            for (Integer key : orderedKeys) {
                into.add(order.get(key));
            }
        }
    }

}
