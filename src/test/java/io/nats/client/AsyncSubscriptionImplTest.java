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
