/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class AsyncSubscriptionImplTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testClose() {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            assertEquals(nc, s.getConnection());
        }
    }

    @Test
    public void testProcessMsg() {
        MessageHandler mcb = new MessageHandler() {
            @Override
            public void onMessage(Message msg) {}
        };

        Message m = new Message("foo", "bar", "Hello".getBytes());

        // test for when the conn is null
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(null, "foo", "bar", mcb, 20, 0)) {
            assertFalse("s.processMsg should have returned false", s.processMsg(m));
        }

        ConnectionImpl nc = mock(ConnectionImpl.class);

        // test for when the mcb is null
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            assertTrue("s.processMsg should have returned true", s.processMsg(m));
        }

        // test for > max
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", mcb, 50, 0)) {
            // setting this protected var deliberately for testing purposes
            s.max = 2;
            assertEquals(2, s.max);
            assertTrue("s.processMsg should have returned true", s.processMsg(m));
            assertEquals(1, s.delivered.get());
            when(nc.isClosed()).thenReturn(true);
            assertTrue("s.processMsg should have returned true", s.processMsg(m));
            assertFalse("s.processMsg should have returned false", s.processMsg(m));
        }
        when(nc.isClosed()).thenReturn(false);
        // test for unsubscribe IOException
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", mcb, 50, 0)) {
            s.setMaxPendingMsgs(1);
            try {
                doThrow(new IOException("fake unsubscribe exception")).when(nc).unsubscribe(s, 0);
            } catch (IOException e) {
                fail("Mockito doThrow shouldn't have thrown an exception");
            }
            assertTrue("s.processMsg should have returned true", s.processMsg(m));
        } catch (IllegalStateException e) {
            fail("Shouldn't have thrown an exception");
        }

    }

    @Test
    public void testUnsubscribeConnectionNull() {
        boolean exThrown = false;
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(null, "foo", "bar", null, 20, 0)) {
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
            try (AsyncSubscriptionImpl s =
                    new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
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

    // @Test
    // public void testAsyncSubscriptionImpl() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testEnable() {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            s.enable();
            assertTrue(s.isStarted());
        }
    }

    @Test
    public void testDisable() {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            s.enable();
            assertTrue("s.enable() failed", s.isStarted());
            s.disable();
            assertFalse("s.disable() failed", s.isStarted());
        }
    }

    @Test
    public void testSetMessageHandler() {
        ConnectionImpl nc = mock(ConnectionImpl.class);

        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            assertTrue(s.isValid());
            s.setMessageHandler(new MessageHandler() {
                @Override
                public void onMessage(Message msg) {}
            });
        }
    }

    @Test
    public void testStart() {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        boolean exThrown = false;
        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0)) {
            s.start();
            s.start();
            assertTrue(s.isStarted());
            s.disable();
            assertFalse(s.isStarted());
            try {
                s.setConnection(null);
                s.start();
            } catch (IllegalStateException e) {
                assertEquals(ERR_BAD_SUBSCRIPTION, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have thrown IllegalStateException", exThrown);
            }
        }
    }

    @Test
    public void msgFeederThreadExitOnUnsubscribe() throws IOException {
        ConnectionImpl nc = mock(ConnectionImpl.class);
        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(new ConnectionFactory().options());

        AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(nc, "foo", "bar", null, 20, 0);
        sub.start();
        UnitTestUtilities.sleep(1, TimeUnit.SECONDS);
        sub.unsubscribe();
        assertFalse(sub.isStarted());
        sub.close();
    }

    @Test
    public void msgFeederThreadExitOnConnectionClose() throws IOException, TimeoutException {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        Connection conn = new ConnectionFactory().createConnection(mcf);
        Subscription sub = conn.subscribe("foo", new MessageHandler() {
            @Override
            public void onMessage(Message msg) {
                System.out.println("Got: " + msg);
            }
        });
        assertTrue(((AsyncSubscriptionImpl) sub).isStarted());
        conn.close();
        assertFalse(((AsyncSubscriptionImpl) sub).isStarted());
    }
}
