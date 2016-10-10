/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Constants.ERR_SLOW_CONSUMER;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class SyncSubscriptionImplTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(SyncSubscriptionImplTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private Channel<Message> mchMock;

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
        try (SyncSubscriptionImpl s =
                new SyncSubscriptionImpl(nc, subj, queue, msgLimit, byteLimit)) {
            assertEquals(nc, s.getConnection());
            assertEquals(subj, s.getSubject());
            assertEquals(queue, s.getQueue());
            assertEquals(msgLimit, s.getPendingMsgsLimit());
            assertEquals(byteLimit, s.getPendingBytesLimit());
        }
    }

    @Test
    public void testNextMessage() throws IOException {
        String subj = "foo";
        String queue = "bar";

        final Message msgMock = mock(Message.class);
        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            s.setChannel(mchMock);
            when(mchMock.get()).thenReturn(msgMock);
            Message msg = s.nextMessage();
            assertEquals(msgMock, msg);
        }
    }

    @Test
    public void testNextMessageTimeoutSuccess() throws TimeoutException, IOException {
        String subj = "foo";
        String queue = "bar";
        long timeout = 1000;
        final Message msgMock = mock(Message.class);

        final ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            when(mchMock.get(eq(timeout), eq(TimeUnit.MILLISECONDS))).thenReturn(msgMock);
            sub.setChannel(mchMock);

            Message msg = sub.nextMessage(timeout, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            verify(mchMock, times(1)).get(eq(timeout), eq(TimeUnit.MILLISECONDS));
        }
    }

    @Test(expected = TimeoutException.class)
    public void testNextMessageTimesOut() throws TimeoutException, IOException {
        String subj = "foo";
        String queue = "bar";
        int timeout = 100;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue)) {
            s.nextMessage(timeout);
        }
    }

    @Test
    public void testNextMessageMaxMessages() throws TimeoutException, IOException {
        thrown.expect(IOException.class);
        thrown.expectMessage(Constants.ERR_MAX_MESSAGES);
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

    @Test
    public void testNextMessageSubClosed() throws TimeoutException, IOException {
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
    public void testNextMessageSlowConsumer() throws TimeoutException, IOException {
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

    @Test
    public void testSetPendingBytesLimit() {
        String subj = "foo";
        String queue = "bar";
        int max = 20;
        int maxBytes = 4000000;

        ConnectionImpl nc = mock(ConnectionImpl.class);
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue, max, -1)) {
            sub.setPendingBytesLimit(maxBytes);
            assertEquals(maxBytes, sub.pBytesLimit);
        }

        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue, max, 2100)) {
            maxBytes = -400;
            sub.setPendingBytesLimit(maxBytes);
            assertEquals(sub.pBytesLimit, -400);
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
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue,
                maxMsgsDefaultLimit, maxBytesDefaultLimit)) {
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
    public void testAutoUnsubscribeConnNull() throws IOException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_BAD_SUBSCRIPTION);
        String subj = "foo";
        String queue = "bar";
        int max = 20;

        ConnectionImpl nc = null;
        try (SyncSubscriptionImpl sub = new SyncSubscriptionImpl(nc, subj, queue)) {
            sub.autoUnsubscribe(1);
        }

    }

}
