/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class ITAuthTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    private int hitDisconnect;

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
