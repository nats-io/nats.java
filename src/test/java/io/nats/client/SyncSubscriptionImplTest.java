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

package io.nats.client;

import static io.nats.client.Nats.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

@Category(UnitTest.class)
public class SyncSubscriptionImplTest extends BaseUnitTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Mock
    private ConnectionImpl connMock;

    @Mock
    private BlockingQueue<Message> mchMock;

    @Mock
    private Message msgMock;

    private ExecutorService exec;

    /**
     * Per-test-case setup.
     *
     * @throws Exception if something goes wrong
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        exec = Executors.newCachedThreadPool();
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Per-test-case cleanup.
     *
     * @throws Exception if something goes wrong
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        exec.shutdownNow();
    }

    @Test
    public void testSyncSubscriptionImplConnectionImplStringString() {
        String subj = "foo";
        String queue = "bar";

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            assertEquals(nc, s.getConnection());
            assertEquals(subj, s.getSubject());
            assertEquals(queue, s.getQueue());
            assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_MSGS, s.getPendingMsgsLimit());
            assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_BYTES, s.getPendingBytesLimit());
        }
    }

    @Test
    public void testSyncSubscriptionImplConnectionImplStringStringIntInt() {
        String subj = "foo";
        String queue = "bar";
        int msgLimit = 20;
        int byteLimit = -1;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            s.setPendingLimits(msgLimit, byteLimit);
            assertEquals(nc, s.getConnection());
            assertEquals(subj, s.getSubject());
            assertEquals(queue, s.getQueue());
            assertEquals(msgLimit, s.getPendingMsgsLimit());
            assertEquals(byteLimit, s.getPendingBytesLimit());
        }
    }

    @Test
    public void testNextMessage() throws Exception {
        String subj = "foo";
        String queue = "bar";

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            s.setChannel(mchMock);
            when(mchMock.take()).thenReturn(msgMock);
            Message msg = s.nextMessage();
            assertEquals(msgMock, msg);
        }
    }

    @Test
    public void testNextMessageTimeoutSuccess() throws Exception {
        String subj = "foo";
        String queue = "bar";
        long timeout = 1000;

        final ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            when(mchMock.poll(timeout, TimeUnit.MILLISECONDS)).thenReturn(msgMock);
            sub.setChannel(mchMock);
            sub.pCond = mock(Condition.class);

            Message msg = sub.nextMessage(timeout, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            Message foo = verify(mchMock, times(1)).poll(timeout, TimeUnit.MILLISECONDS);
            assertNull(foo);
        }
    }

    @Test
    public void testNextMessageAutoUnsubscribeMax() throws Exception {
        String subj = "foo";
        String queue = "bar";
        long timeout = 1000;

        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            try (SyncSubscriptionImpl sub = (SyncSubscriptionImpl) nc.subscribe(subj, queue)) {
                sub.setMax(1);
                when(mchMock.poll(timeout, TimeUnit.MILLISECONDS)).thenReturn(msgMock);
                sub.setChannel(mchMock);

                Message msg = sub.nextMessage(timeout, TimeUnit.MILLISECONDS);
                assertEquals(msgMock, msg);
                verify(mchMock, times(1)).poll(timeout, TimeUnit.MILLISECONDS);
                verify(nc, times(1)).removeSub(sub);
            }
        }
    }

    @Test
    public void testNextMessageTimesOut() throws Exception {
        String subj = "foo";
        String queue = "bar";
        int timeout = 100;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            assertNull(s.nextMessage(timeout));
        }
    }

    @Test
    public void testNextMessageMaxMessages() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_MAX_MESSAGES);
        String subj = "foo";
        String queue = "bar";
        int timeout = 100;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.setMax(40);
            sub.delivered = 41;
            sub.setChannel(null);
            sub.nextMessage(timeout);
        }
    }

    @Test(timeout = 3000)
    public void testNextMessageInterrupted() throws Exception {
        thrown.expect(InterruptedException.class);
        String subj = "foo";
        int timeout = 30000;

        try (Connection nc = newMockedConnection()) {
            try (final SyncSubscription sub = nc.subscribe(subj)) {
                exec.execute(new Runnable() {
                    public void run() {
                        sleep(500);
                        sub.close();
                    }
                });
                sub.nextMessage(timeout);
            }
        }
    }

    @Test
    public void testNextMessageSubClosed() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);
        String subj = "foo";
        String queue = "bar";
        int timeout = 100;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.setChannel(null);
            sub.closed = true;
            Message msg = sub.nextMessage(timeout);
        }
    }

    @Test
    public void testNextMessageSlowConsumer() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_SLOW_CONSUMER);
        String subj = "foo";
        String queue = "bar";
        int timeout = 100;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.setMax(40);
            sub.delivered = 41;
            sub.setSlowConsumer(true);
            Message msg = sub.nextMessage(timeout);
        }
    }
}
