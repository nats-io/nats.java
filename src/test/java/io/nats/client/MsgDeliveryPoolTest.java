/*
 *  Copyright (c) 2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import io.nats.client.MsgDeliveryPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(UnitTest.class)
public class MsgDeliveryPoolTest extends BaseUnitTest {

    @Test
    public void testMsgDeliveryPool() throws Exception {
        MsgDeliveryPool pool = new MsgDeliveryPool();
        pool.setSize(5);
        assertEquals(5, pool.getSize());
        // We support only expand, so size should not change
        pool.setSize(2);
        assertEquals(5, pool.getSize());
        // expand
        pool.setSize(7);
        assertEquals(7, pool.getSize());

        final AtomicInteger received = new AtomicInteger(0);
        ConnectionImpl nc = mock(ConnectionImpl.class);
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
    }
}