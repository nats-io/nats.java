/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Category(UnitTest.class)
public class ConnectionEventTest extends BaseUnitTest {

    @Mock
    private Connection connMock;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public final void testConnectionEvent() {
        new ConnectionEvent(connMock);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testConnectionEventNull() {
        new ConnectionEvent(null);
    }

    @Test
    public final void testGetConnection() {
        ConnectionEvent cev = new ConnectionEvent(connMock);
        assertEquals(connMock, cev.getConnection());
    }
}
