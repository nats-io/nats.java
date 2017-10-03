/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Category(UnitTest.class)
public class AsyncSubscriptionImplTest extends BaseUnitTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    public ConnectionImpl connMock;

    @Mock
    public MessageHandler mcbMock;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testClose() {
        // Make sure the connection opts aren't null
        when(connMock.getOptions()).thenReturn(Nats.defaultOptions());

        AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(connMock, "foo", "bar", mcbMock);
        sub.close();
    }

    @Test
    public void testSetMessageHandler() {
        ConnectionImpl nc = mock(ConnectionImpl.class);

        // Make sure the connection opts aren't null
        when(nc.getOptions()).thenReturn(Nats.defaultOptions());

        MessageHandler mh = new MessageHandler() {
            @Override
            public void onMessage(Message msg) {
            }
        };

        try (AsyncSubscriptionImpl s = new AsyncSubscriptionImpl(nc, "foo", "bar", null)) {
            assertTrue(s.isValid());
            s.setMessageHandler(mh);

            assertEquals(mh, s.getMessageHandler());
        }
    }

}
