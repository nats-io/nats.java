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

import static io.nats.client.Nats.ERR_AUTHORIZATION;
import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runServerOnPort;
import static io.nats.client.UnitTestUtilities.runServerWithConfig;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class ITAuthTest extends ITBaseTest {
    private int hitDisconnect;

    @Test
    public void testAuth() {
        String noAuthUrl = "nats://localhost:1222";
        String authUrl = "nats://username:password@localhost:1222";

        try (NatsServer s = runServerWithConfig("auth_1222.conf")) {
            try (Connection c = Nats.connect(noAuthUrl)) {
                fail("Should have received an error while trying to connect");
            } catch (IOException e) {
                assertEquals(ERR_AUTHORIZATION, e.getMessage());
            }

            try (Connection c = Nats.connect(authUrl)) {
                assertFalse(c.isClosed());
            } catch (IOException e) {
                fail("Should have connected successfully, but got: " + e.getMessage());
            }
        }
    }


    @Test
    public void testAuthFailNoDisconnectCb() throws Exception {
        String url = "nats://localhost:1222";
        final CountDownLatch latch = new CountDownLatch(1);

        DisconnectedCallback dcb = new DisconnectedCallback() {
            @Override
            public void onDisconnect(ConnectionEvent event) {
                latch.countDown();
            }
        };

        Options opts = Nats.defaultOptions();
        opts.url = url;
        opts.disconnectedCb = dcb;
        try (NatsServer s = runServerWithConfig("auth_1222.conf")) {
            try (Connection c = opts.connect()) {
                fail("Should have received an error while trying to connect");
            } catch (IOException e) {
                assertEquals(ERR_AUTHORIZATION, e.getMessage());
            }
            assertFalse("Should not have received a disconnect callback on auth failure",
                    latch.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testAuthFailAllowReconnect() throws Exception  {
        String[] servers =
                {"nats://localhost:1221", "nats://localhost:1222", "nats://localhost:1223",};

        final CountDownLatch latch = new CountDownLatch(1);

        ReconnectedCallback rcb = new ReconnectedCallback() {
            public void onReconnect(ConnectionEvent event) {
                latch.countDown();
            }
        };

        Options opts = new Options.Builder(defaultOptions()).dontRandomize().maxReconnect(1)
                .reconnectWait(100).reconnectedCb(rcb).build();
        opts.servers = Nats.processUrlArray(servers);
        // First server: no auth.
        try (final NatsServer ts = runServerOnPort(1221)) {
            // Second server: user/pass auth.
            try (final NatsServer ts2 = runServerWithConfig("auth_1222.conf")) {
                // Third server: no auth.
                try (final NatsServer ts3 = runServerOnPort(1223)) {
                    try (ConnectionImpl c = (ConnectionImpl) opts.connect()) {
                        assertEquals("nats://localhost:1221", c.getConnectedUrl());
                        assertTrue(c.getOptions().isNoRandomize());

                        // Stop the server
                        ts.shutdown();

                        sleep(2, TimeUnit.SECONDS);

                        // The client will try to connect to the second server, and that
                        // should fail. It should then try to connect to the third and succeed.

                        assertTrue("Reconnect callback should have been triggered",
                                await(latch, 5, TimeUnit.SECONDS));
                        assertFalse("Should have reconnected", c.isClosed());
                        assertEquals(servers[2], c.getConnectedUrl());
                    }
                }
            }
        }
    }

    @Test
    public void testAuthFailure() {
        String[] urls =
                {"nats://username@localhost:1222", "nats://username:badpass@localhost:1222",
                        "nats://localhost:1222", "nats://badname:password@localhost:1222"};

        try (NatsServer s = runServerWithConfig("auth_1222.conf")) {
            hitDisconnect = 0;
            DisconnectedCallback dcb = new DisconnectedCallback() {
                @Override
                public void onDisconnect(ConnectionEvent event) {
                    hitDisconnect++;
                }
            };
            Options opts = new Options.Builder().disconnectedCb(dcb).build();

            for (String url : urls) {
                boolean exThrown = false;
                // System.err.println("Trying: " + url);
                Thread.sleep(100);
                opts.url = url;
                try (Connection c = opts.connect()) {
                    fail("Should not have connected");
                } catch (Exception e) {
                    assertTrue("Wrong exception thrown", e instanceof IOException);
                    assertEquals(ERR_AUTHORIZATION, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have received an error while trying to connect", exThrown);
                Thread.sleep(100);
            }
        } catch (Exception e) {
            fail("Unexpected exception thrown: " + e);
        }
        assertTrue("The disconnect event handler was incorrectly invoked.", hitDisconnect == 0);
    }

    @Test
    public void testAuthSuccess() throws Exception  {
        String url = ("nats://username:password@localhost:1222");
        try (NatsServer s = runServerWithConfig("auth_1222.conf")) {
            try (Connection c = Nats.connect(url)) {
                assertTrue(c.isConnected());
            }
        }
    }
}
