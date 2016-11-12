/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class NatsExceptionTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

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

    @Test
    public void testNatsEx() {
        NATSException nex = new NATSException();
        assertNotNull(nex);
        String msg = "detail message";
        final Exception causeEx = new Exception("some other problem");
        final Subscription sub = mock(SyncSubscription.class);
        final Connection nc = mock(Connection.class);

        nex = new NATSException(msg);
        assertNotNull(nex);
        assertEquals(msg, nex.getMessage());

        nex = new NATSException(causeEx);
        assertNotNull(nex);
        assertEquals(causeEx, nex.getCause());
        assertEquals("some other problem", nex.getCause().getMessage());

        nex = new NATSException(causeEx, nc, sub);
        assertNotNull(nex);
        assertEquals(causeEx, nex.getCause());
        assertEquals("some other problem", nex.getCause().getMessage());
        assertEquals(nc, nex.getConnection());
        assertEquals(sub, nex.getSubscription());
        Connection nc2 = mock(Connection.class);
        nex.setConnection(nc2);
        assertEquals(nc2, nex.getConnection());
        Subscription sub2 = mock(AsyncSubscription.class);
        nex.setSubscription(sub2);
        assertEquals(sub2, nex.getSubscription());

        nex = new NATSException(msg, causeEx);
        assertNotNull(nex);
        assertEquals(msg, nex.getMessage());
        assertEquals(causeEx, nex.getCause());
        assertEquals("some other problem", nex.getCause().getMessage());
    }

}
