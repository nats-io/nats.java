// Copyright 2017-2018 The NATS Authors
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

import io.nats.client.MsgDeliveryPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(UnitTest.class)
public class MsgDeliveryPoolTest extends BaseUnitTest {

    @Test
    public void testMsgDeliveryWorker() throws Exception {
        final MsgDeliveryWorker worker = new MsgDeliveryWorker();
        assertTrue(worker.getName().contains("delivery"));
        worker.start();
        final CountDownLatch latch = new CountDownLatch(1);
        AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(null, "foo", null, new MessageHandler() {
            public void onMessage(Message msg) {
                latch.countDown();
            }
        });
        sub.lock();
        sub.setDeliveryWorker(worker);
        sub.unlock();
        worker.lock();
        worker.postMsg(new Message("hello".getBytes(), "foo", null, sub));
        worker.unlock();
        latch.await();
        worker.shutdown();
    }

    @Test
    public void testMsgDeliveryWorkerShutdownFromCB() throws Exception {
        final MsgDeliveryWorker worker = new MsgDeliveryWorker();
        assertTrue(worker.getName().contains("delivery"));
        worker.start();
        final CountDownLatch latch = new CountDownLatch(1);
        AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(null, "foo", null, new MessageHandler() {
            public void onMessage(Message msg) {
                latch.countDown();
                worker.shutdown();
            }
        });
        sub.lock();
        sub.setDeliveryWorker(worker);
        sub.unlock();
        worker.lock();
        worker.postMsg(new Message("hello".getBytes(), "foo", null, sub));
        worker.unlock();
        latch.await();
        // Double shutdown is fine
        worker.shutdown();
    }

    @Test
    public void testMsgDeliveryWorkerExceptionInCB() throws Exception {
        final MsgDeliveryWorker worker = new MsgDeliveryWorker();        
        worker.start();
        final CountDownLatch latch = new CountDownLatch(2);
        AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(null, "foo", null, new MessageHandler() {
            public void onMessage(Message msg) {
                latch.countDown();
                throw new RuntimeException("On purpose");
            }
        });
        sub.lock();
        sub.setDeliveryWorker(worker);
        sub.unlock();
        // Post 2 messages
        worker.lock();
        worker.postMsg(new Message("hello".getBytes(), "foo", null, sub));
        worker.postMsg(new Message("hello".getBytes(), "foo", null, sub));
        worker.unlock();
        // Callback should be invoked twice, even if throwing exception
        latch.await(2, TimeUnit.SECONDS);
        worker.shutdown();
    }

    @Test
    public void testMsgDeliveryPool() throws Exception {
        MsgDeliveryPool pool = new MsgDeliveryPool(5);
        assertEquals(5, pool.getSize());

        final AtomicInteger received = new AtomicInteger(0);
        Options opts = Nats.defaultOptions();
        ConnectionImpl nc = new ConnectionImpl(opts);
        AsyncSubscriptionImpl sub1 = new AsyncSubscriptionImpl(nc, "foo", null, new MessageHandler() {
            public void onMessage(Message msg) {
                received.incrementAndGet();
            }
        });
        // assign worker to sub
        pool.assignDeliveryWorker(sub1);
        // check it is assigned to sub
        MsgDeliveryWorker worker1 = sub1.getDeliveryWorker();
        assertNotNull(worker1);

        // Make the sub auto-unsubscribe at 2
        sub1.lock();
        sub1.max = 2;
        sub1.unlock();
        // Post 3 messages
        for (int i=0; i<3; i++) {
            final Message msg = new Message("hello".getBytes(), "foo", null, sub1);
            worker1.lock();
            worker1.postMsg(msg);
            worker1.unlock();
        }
        // Wait a bit...
        Thread.sleep(100);
        // Check that only 2 messages were received.
        assertEquals(2, received.get());

        received.set(0);
        AsyncSubscriptionImpl sub2 = new AsyncSubscriptionImpl(nc, "foo", null, new MessageHandler(){
            public void onMessage(Message msg) {
                received.incrementAndGet();
                SubscriptionImpl sub = (SubscriptionImpl) msg.getSubscription();
                sub.lock();
                sub.closed = true;
                sub.unlock();
            }
        });
        // assign worker to sub
        pool.assignDeliveryWorker(sub2);
        // check it is assigned to sub
        MsgDeliveryWorker worker2 = sub2.getDeliveryWorker();
        assertNotNull(worker2);
        // Worker should not be same, but that may be a too restrictive test
        // based on implementation details.
        assertNotEquals(worker1, worker2);
        // Send 2 messages, the subscription should have closed after receiving
        // the first.
         for (int i=0; i<2; i++) {
            final Message msg = new Message("hello".getBytes(), "foo", null, sub2);
            worker2.lock();
            worker2.postMsg(msg);
            worker2.unlock();
        }
        // Wait a bit...
        Thread.sleep(100);
        // Check that only 1 message was received.
        assertEquals(1, received.get());

        // Shutdown
        pool.shutdown();
        assertEquals(0, pool.getSize());

        // Double shutdown is ok
        pool.shutdown();
        assertEquals(0, pool.getSize());

        // Recreate with size 1
        pool = new MsgDeliveryPool(1);

        // Add 2 subs
        pool.assignDeliveryWorker(sub1);
        pool.assignDeliveryWorker(sub2);
        // Verify they share same thread.
        assertEquals(sub1.getDeliveryWorker(), sub2.getDeliveryWorker());
        // Shutdown
        pool.shutdown();
    }
}