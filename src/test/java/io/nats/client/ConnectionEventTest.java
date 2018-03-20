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
