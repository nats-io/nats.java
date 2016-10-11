/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Category(UnitTest.class)
public class SubscriptionImplTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(SubscriptionImplTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private ConnectionImpl connMock;

    @Mock
    private Channel<Message> mchMock;

    @Mock
    private MessageHandler mcbMock;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @Test
    public void testSubscriptionImplConnectionImplStringString() {
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

        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, subj, queue, mcbMock)) {
            assertEquals(nc, s.getConnection());
            assertEquals(subj, s.getSubject());
            assertEquals(queue, s.getQueue());
            assertEquals(mcbMock, s.msgHandler);
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
    public void testClearMaxPending() {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(nc, "foo", "bar", null)) {
            int maxMsgs = 44;
            int maxBytes = 44 * 1024;
            sub.setPendingMsgsMax(maxMsgs);
            assertEquals(maxMsgs, sub.getPendingMsgsMax());

            sub.setPendingBytesMax(maxBytes);
            assertEquals(maxBytes, sub.getPendingBytesMax());

            sub.clearMaxPending();
            assertEquals(0, sub.getPendingMsgsMax());
            assertEquals(0, sub.getPendingBytesMax());

            assertEquals(nc, sub.getConnection());
        }
    }

    @Test
    public void testCloseChannel() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("testing");

        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(nc, "foo", "bar", null)) {
            sub.setChannel(mchMock);
            assertNotNull(sub.getChannel());
            sub.closeChannel();
            assertNull(sub.getChannel());
            verify(mchMock, times(1)).close();
        }

        try (AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(null, "foo", "bar", null)) {
            sub.setChannel(null);
            assertNull(sub.getChannel());
            sub.closeChannel();
        }

        try (AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(nc, "foo", "bar", null)) {
            doThrow(new NullPointerException("testing")).when(mchMock).close();
            sub.setChannel(mchMock);
            assertNotNull(sub.getChannel());
            sub.closeChannel();
        }
    }

    @Test
    public void testCloseChannelNullConn() {
        // ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        // when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(null, "foo", "bar", null)) {
            assertNotNull(sub.getChannel());
            sub.closeChannel();
            assertNull(sub.getChannel());
        }
    }

    @Test
    public void testSetPendingBytesLimit() {
        String subj = "foo";
        String queue = "bar";
        int maxBytes = 4000000;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.setPendingBytesLimit(maxBytes);
            assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_MSGS, sub.getPendingMsgsLimit());
            assertEquals(maxBytes, sub.getPendingBytesLimit());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            maxBytes = -400;
            sub.setPendingBytesLimit(maxBytes);
            assertEquals(SubscriptionImpl.DEFAULT_MAX_PENDING_MSGS, sub.getPendingMsgsLimit());
            assertEquals(-400, sub.getPendingBytesLimit());
        }

    }

    @Test
    public void testSetPendingLimits() {
        String subj = "foo";
        String queue = "bar";
        int maxMsgsDefaultLimit = 20;
        int maxBytesDefaultLimit = 50;
        int maxMsgs = 4;
        int maxBytes = 4000000;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.setPendingLimits(maxMsgsDefaultLimit, maxBytesDefaultLimit);
            assertEquals(maxMsgsDefaultLimit, sub.getPendingMsgsLimit());
            assertEquals(maxBytesDefaultLimit, sub.getPendingBytesLimit());
            sub.setPendingLimits(maxMsgs, maxBytes);
            assertEquals(maxMsgs, sub.getPendingMsgsLimit());
            assertEquals(maxBytes, sub.getPendingBytesLimit());

            boolean exThrown = false;
            try {
                sub.setPendingLimits(0, 1);
            } catch (IllegalArgumentException e) {
                exThrown = true;
            } finally {
                assertTrue("Setting limit with 0 should fail", exThrown);
            }

            exThrown = false;
            try {
                sub.setPendingLimits(1, 0);
            } catch (IllegalArgumentException e) {
                exThrown = true;
            } finally {
                assertTrue("Setting limit with 0 should fail", exThrown);
            }
        }
    }

    @Test
    public void testAutoUnsubscribe() throws IOException {
        String subj = "foo";
        String queue = "bar";
        int max = 20;

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.autoUnsubscribe(max);
            verify(connMock, times(1)).unsubscribe(eq(sub), eq(max));
        }
    }

    @Test
    public void testAutoUnsubscribeConnNull() throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);
        String subj = "foo";
        String queue = "bar";

        ConnectionImpl nc = null;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.autoUnsubscribe(1);
        }

    }

    @Test
    public void testGetDelivered() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.delivered = count;
            assertEquals(count, sub.getDelivered());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getDelivered();
        }
    }

    @Test
    public void testGetDropped() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.dropped = count;
            assertEquals(count, sub.getDropped());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getDropped();
        }
    }

    @Test
    public void testGetPendingBytesMax() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.pBytesMax = count;
            assertEquals(count, sub.getPendingBytesMax());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getPendingBytesMax();
        }
    }

    @Test
    public void testGetPendingMsgsMax() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.pMsgsMax = count;
            assertEquals(count, sub.getPendingMsgsMax());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getPendingMsgsMax();
        }
    }

    @Test
    public void testGetQueuedMessageCount() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        when(mchMock.getCount()).thenReturn(count);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.pMsgs = count;
            assertEquals(count, sub.getQueuedMessageCount());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getQueuedMessageCount();
        }
    }

    @Test
    public void testGetPendingBytes() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22 * 1024;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.pBytes = count;
            assertEquals(count, sub.getPendingBytes());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getPendingBytes();
        }
    }

    @Test
    public void testGetPendingMsgs() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.pMsgs = count;
            assertEquals(count, sub.getPendingMsgs());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.getPendingMsgs();
        }
    }

    @Test
    public void testIsValid() {
        String subj = "foo";
        String queue = "bar";
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            assertTrue(sub.isValid());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            assertFalse(sub.isValid());
        }
    }

    @Test
    public void testPrintStats() {
        String subj = "foo";
        String queue = "bar";
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.printStats();
        }
    }


    // @Test
    // public void testProcessMsg() {
    // MessageHandler mcb = new MessageHandler() {
    // @Override
    // public void onMessage(Message msg) {}
    // };
    //
    // Message m = new Message("foo", "bar", "Hello".getBytes());
    //
    // // test for when the conn is null
    // try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(null, "foo", "bar", mcb, 20, 0)) {
    // assertFalse("s.processMsg should have returned false", s.processMsg(m));
    // }
    //
    // ConnectionImpl nc = mock(ConnectionImpl.class);
    //
    // // test for when the mcb is null
    // try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
    // assertTrue("s.processMsg should have returned true", s.processMsg(m));
    // }
    //
    // // test for > max
    // try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", mcb, 50, 0)) {
    // // setting this protected var deliberately for testing purposes
    // s.max = 2;
    // assertEquals(2, s.max);
    // assertTrue("s.processMsg should have returned true", s.processMsg(m));
    // assertEquals(1, s.delivered.get());
    // when(nc.isClosed()).thenReturn(true);
    // assertTrue("s.processMsg should have returned true", s.processMsg(m));
    // assertFalse("s.processMsg should have returned false", s.processMsg(m));
    // }
    // when(nc.isClosed()).thenReturn(false);
    // // test for unsubscribe IOException
    // try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", mcb, 50, 0)) {
    // s.setMaxPendingMsgs(1);
    // try {
    // doThrow(new IOException("fake unsubscribe exception")).when(nc).unsubscribe(s, 0);
    // } catch (IOException e) {
    // fail("Mockito doThrow shouldn't have thrown an exception");
    // }
    // assertTrue("s.processMsg should have returned true", s.processMsg(m));
    // } catch (IllegalStateException e) {
    // fail("Shouldn't have thrown an exception");
    // }
    //
    // }

    @Test
    public void testSetPendingBytesMax() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.setPendingBytesMax(count);
            assertEquals(count, sub.getPendingBytesMax());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.setPendingBytesMax(count);
        }
    }

    @Test
    public void testSetPendingMsgsMax() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);

        String subj = "foo";
        String queue = "bar";
        int count = 22;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(connMock, subj, queue)) {
            sub.setPendingMsgsMax(count);
            assertEquals(count, sub.getPendingMsgsMax());
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(null, subj, queue)) {
            sub.setPendingMsgsMax(count);
        }
    }

    @Test
    public void testToString() {
        String expected = "{subject=foo, queue=bar, sid=0, max=0, delivered=0, "
                + "pendingMsgsLimit=65536, pendingBytesLimit=67108864, maxPendingMsgs=0, "
                + "maxPendingBytes=0, valid=true}";
        String expected2 = "{subject=foo, queue=null, sid=0, max=0, delivered=0, "
                + "pendingMsgsLimit=65536, pendingBytesLimit=67108864, maxPendingMsgs=0, "
                + "maxPendingBytes=0, valid=true}";

        try (SubscriptionImpl sub = new AsyncSubscriptionImpl(connMock, "foo", "bar", null)) {
            assertEquals(expected, sub.toString());
        }

        try (SubscriptionImpl sub = new SyncSubscriptionImpl(connMock, "foo", null)) {
            assertEquals(expected2, sub.toString());
        }
    }

    @Test
    public void testUnsubscribeConnectionNull() {
        boolean exThrown = false;
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(null, "foo", "bar", null)) {
            s.unsubscribe();
        } catch (IllegalStateException | IOException e) {
            assertTrue("Exception should have been IllegalStateException",
                    e instanceof IllegalStateException);
            assertEquals(ERR_BAD_SUBSCRIPTION, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Should have thrown IllegalStateException", exThrown);
        }
    }

    @Test
    public void testUnsubscribeConnectionClosed() {
        try (ConnectionImpl nc = mock(ConnectionImpl.class)) {
            // Make sure the connection opts aren't null
            when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

            when(nc.isClosed()).thenReturn(true);

            boolean exThrown = false;
            try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null)) {
                doThrow(IllegalStateException.class).when(nc).unsubscribe(s, 0);
                s.unsubscribe();
                fail("Should have thrown IllegalStateException");
            } catch (Exception e) {
                assertTrue("Exception should have been IllegalStateException, got: "
                        + e.getClass().getSimpleName(), e instanceof IllegalStateException);
                // assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown IllegalStateException", exThrown);
        }
    }

}
