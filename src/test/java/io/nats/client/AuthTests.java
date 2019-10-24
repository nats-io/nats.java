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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.nats.client.Connection.Status;
import io.nats.client.ConnectionListener.Events;

public class AuthTests {
    @Test
    public void testUserPass() throws Exception {
        String[] customArgs = { "--user", "stephen", "--pass", "password" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .userInfo("stephen".toCharArray(), "password".toCharArray()).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUserPassOnReconnect() throws Exception {
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        Connection nc = null;
        Subscription sub = null;
        String[] customArgs = { "--user", "stephen", "--pass", "password" };

        try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(-1)
                    .userInfo("stephen".toCharArray(), "password".toCharArray()).connectionListener(handler).build();
            nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            sub = nc.subscribe("test");
            nc.publish("test", null);
            nc.flush(Duration.ofSeconds(5));
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            handler.prepForStatusChange(Events.DISCONNECTED);
        }

        try {
            nc.flush(Duration.ofSeconds(1));
        } catch (Exception exp) {
        }

        handler.waitForStatusChange(5, TimeUnit.SECONDS);
        assertTrue("Reconnecting status",
                Connection.Status.RECONNECTING == nc.getStatus() || Connection.Status.DISCONNECTED == nc.getStatus());
        handler.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
            handler.waitForStatusChange(5, TimeUnit.SECONDS);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            nc.publish("test", null);
            nc.flush(Duration.ofSeconds(5));
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testUserBCryptPass() throws Exception {
        /*
         * go run mkpasswd.go -p password: password bcrypt hash:
         * $2a$11$1oJy/wZYNTxr9jNwMNwS3eUGhBpHT3On8CL9o7ey89mpgo88VG6ba
         */
        String[] customArgs = { "--user", "ginger", "--pass",
                "$2a$11$1oJy/wZYNTxr9jNwMNwS3eUGhBpHT3On8CL9o7ey89mpgo88VG6ba" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .userInfo("ginger".toCharArray(), "password".toCharArray()).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUserPassInURL() throws Exception {
        String[] customArgs = { "--user", "stephen", "--pass", "password" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://stephen:password@localhost:" + ts.getPort())
                    .maxReconnects(0).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUserPassInURLOnReconnect() throws Exception {
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        Connection nc = null;
        Subscription sub = null;
        String[] customArgs = { "--user", "stephen", "--pass", "password" };

        try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://stephen:password@localhost:" + ts.getPort())
                    .maxReconnects(-1).connectionListener(handler).build();
            nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            sub = nc.subscribe("test");
            nc.publish("test", null);
            nc.flush(Duration.ofSeconds(5));
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            handler.prepForStatusChange(Events.DISCONNECTED);
        }

        try {
            nc.flush(Duration.ofSeconds(1));
        } catch (Exception exp) {
        }

        handler.waitForStatusChange(5, TimeUnit.SECONDS);
        Status status = nc.getStatus();
        assertTrue("Reconnecting status",
                Connection.Status.RECONNECTING == status || Connection.Status.DISCONNECTED == status);
        handler.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
            handler.waitForStatusChange(5, TimeUnit.SECONDS);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            nc.publish("test", null);
            nc.flush(Duration.ofSeconds(5));
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testUserPassInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--user", "stephen", "--pass", "password" };
        String[] customArgs2 = { "--user", "alberto", "--pass", "casadecampo" };
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1, false);
                NatsTestServer ts2 = new NatsTestServer(customArgs2, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://stephen:password@localhost:" + ts1.getPort())
                    .server("nats://alberto:casadecampo@localhost:" + ts2.getPort()).maxReconnects(4).noRandomize()
                    .connectionListener(handler).pingInterval(Duration.ofMillis(100)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://stephen:password@localhost:" + ts1.getPort());

            handler.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            handler.waitForStatusChange(2, TimeUnit.SECONDS);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://alberto:casadecampo@localhost:" + ts2.getPort());
            nc.close();
        }
    }

    @Test
    public void testUserPassInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--user", "stephen", "--pass", "password" };
        String[] customArgs2 = { "--user", "alberto", "--pass", "casadecampo" };
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1, false);
                NatsTestServer ts2 = new NatsTestServer(customArgs2, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://stephen:password@localhost:" + ts1.getPort())
                    .server("nats://localhost:" + ts2.getPort()).noRandomize()
                    .userInfo("alberto".toCharArray(), "casadecampo".toCharArray()).maxReconnects(4).noRandomize()
                    .connectionListener(handler).pingInterval(Duration.ofMillis(100)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://stephen:password@localhost:" + ts1.getPort());

            handler.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            handler.waitForStatusChange(2, TimeUnit.SECONDS);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://localhost:" + ts2.getPort());
            nc.close();
        }
    }

    @Test
    public void testTokenInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1, false);
                NatsTestServer ts2 = new NatsTestServer(customArgs2, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://token_one@localhost:" + ts1.getPort())
                    .server("nats://token_two@localhost:" + ts2.getPort()).maxReconnects(4).noRandomize()
                    .connectionListener(handler).pingInterval(Duration.ofMillis(100)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://token_one@localhost:" + ts1.getPort());

            handler.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            handler.waitForStatusChange(2, TimeUnit.SECONDS);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://token_two@localhost:" + ts2.getPort());
            nc.close();
        }
    }

    @Test
    public void testTokenInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1, false);
                NatsTestServer ts2 = new NatsTestServer(customArgs2, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://token_one@localhost:" + ts1.getPort())
                    .server("nats://localhost:" + ts2.getPort()).token("token_two".toCharArray()).maxReconnects(4)
                    .noRandomize().connectionListener(handler).pingInterval(Duration.ofMillis(100)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://token_one@localhost:" + ts1.getPort());

            handler.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            handler.waitForStatusChange(2, TimeUnit.SECONDS);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getConnectedUrl(), "nats://localhost:" + ts2.getPort());
            nc.close();
        }
    }

    @Test
    public void testToken() throws Exception {
        String[] customArgs = { "--auth", "derek" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0).token("derek".toCharArray())
                    .build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testTokenInURL() throws Exception {
        String[] customArgs = { "--auth", "alberto" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server("nats://alberto@localhost:" + ts.getPort()).maxReconnects(0)
                    .build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testBadUserBadPass() throws Exception {
        Connection nc = null;
        String[] customArgs = { "--user", "stephen", "--pass", "password" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .userInfo("sam".toCharArray(), "notthepassword".toCharArray()).build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testMissingUserPass() throws Exception {
        Connection nc = null;
        String[] customArgs = { "--user", "wally", "--pass", "password" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0).build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testBadToken() throws Exception {
        Connection nc = null;
        String[] customArgs = { "--auth", "colin" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .token("notthetoken".toCharArray()).build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testMissingToken() throws Exception {
        Connection nc = null;
        String[] customArgs = { "--auth", "ivan" };
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0).build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    String createNKeyConfigFile(char[] nkey) throws Exception {
        File tmp = File.createTempFile("nats_java_test", ".conf");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmp));

        writer.write("port: 8222"); // will get rewritten
        writer.newLine();

        writer.write("authorization {");
        writer.newLine();
        writer.write("users = [");
        writer.newLine();
        writer.write(String.format("{\nnkey:%s\n}", new String(nkey)));
        writer.newLine();
        writer.write("]");
        writer.newLine();
        writer.write("}");
        writer.newLine();

        writer.close();

        return tmp.getAbsolutePath();
    }

    @Test
    public void testNKeyAuth() throws Exception {
        NKey theKey = NKey.createUser(null);
        assertNotNull(theKey);

        String configFile = createNKeyConfigFile(theKey.getPublicKey());
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        try (NatsTestServer ts = new NatsTestServer(configFile, false)) {
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .authHandler(new TestAuthHandler(theKey)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testStaticNKeyAuth() throws Exception {
        NKey theKey = NKey.createUser(null);
        assertNotNull(theKey);

        String configFile = createNKeyConfigFile(theKey.getPublicKey());
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        try (NatsTestServer ts = new NatsTestServer(configFile, false)) {
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .authHandler(Nats.staticCredentials(null, theKey.getSeed())).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testJWTAuthWithCredsFile() throws Exception {
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testStaticJWTAuth() throws Exception {
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        // from src/test/resources/jwt_nkey/user.creds
        String jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiI3UE1GTkc0R1c1WkE1NEg3N09TUUZKNkJNQURaSUQ2NTRTVk1XMkRFQVZINVIyUVU0MkhBIiwiaWF0IjoxNTY1ODg5ODk4LCJpc3MiOiJBQUhWU0k1NVlQTkJRWjVQN0Y2NzZDRkFPNFBIQlREWUZRSUVHVEtMUVRJUEVZUEZEVEpOSEhPNCIsIm5hbWUiOiJkZW1vIiwic3ViIjoiVUMzT01MSlhUWVBZN0ZUTVVZNUNaNExHRVdRSTNZUzZKVFZXU0VGRURBMk9MTEpZSVlaVFo3WTMiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.ROSJ7D9ETt9c8ZVHxsM4_FU2dBRLh5cNfb56MxPQth74HAxxtGMl0nn-9VVmWjXgFQn4JiIbwrGfFDBRMzxsAA";
        String nkey = "SUAFYHVVQVOIDOOQ4MTOCTLGNZCJ5PZ4HPV5WAPROGTEIOF672D3R7GBY4";

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .authHandler(Nats.staticCredentials(jwt.toCharArray(), nkey.toCharArray())).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected = IOException.class)
    public void testBadAuthHandler() throws Exception {
        NKey theKey = NKey.createUser(null);
        assertNotNull(theKey);

        String configFile = createNKeyConfigFile(theKey.getPublicKey());
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            throw new IOException();// to pass the test
        }

        try (NatsTestServer ts = new NatsTestServer(configFile, false)) {
            Options options = new Options.Builder().server(ts.getURI()).maxReconnects(0)
                    .authHandler(new TestAuthHandler(null)). // No nkey
                    build();
            Connection nc = Nats.connect(options);
            try {
                assertFalse("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testReconnectWithAuth() throws Exception {
        TestHandler handler = new TestHandler();
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        // Connect should fail on ts1
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/operator.conf", false);
                NatsTestServer ts2 = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().servers(new String[] { ts.getURI(), ts2.getURI() })
                    .noRandomize().maxReconnects(-1).authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts.getURI(), nc.getConnectedUrl());

                handler.prepForStatusChange(Events.RECONNECTED);

                ts.close();

                // Reconnect will fail because ts has the same auth error
                handler.waitForStatusChange(5, TimeUnit.SECONDS);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts2.getURI(), nc.getConnectedUrl());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testCloseOnReconnectWithSameError() throws Exception {
        TestHandler handler = new TestHandler();
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        // Connect should fail on ts1
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/operator_noacct.conf", false);
                NatsTestServer ts2 = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().servers(new String[] { ts.getURI(), ts2.getURI() })
                    .maxReconnects(-1).connectionTimeout(Duration.ofSeconds(2)).noRandomize()
                    .authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts2.getURI(), nc.getConnectedUrl());

                handler.prepForStatusChange(Events.CLOSED);

                ts2.close();

                // Reconnect will fail because ts has the same auth error
                handler.waitForStatusChange(6, TimeUnit.SECONDS);
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testThatAuthErrorIsCleared() throws Exception {
        TestHandler handler = new TestHandler();
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        // Connect should fail on ts1
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/operator_noacct.conf", false);
                NatsTestServer ts2 = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().servers(new String[] { ts.getURI(), ts2.getURI() }).noRandomize()
                    .maxReconnects(-1).connectionTimeout(Duration.ofSeconds(5)).reconnectWait(Duration.ofSeconds(1)) // wait a tad to allow restarts
                    .authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts2.getURI(), nc.getConnectedUrl());

                String tsURI = ts.getURI();
                int port = ts.getPort();
                int port2 = ts2.getPort();

                ts.close();

                // ts3 will be at the same port that ts was
                try (NatsTestServer ts3 = new NatsTestServer("src/test/resources/operator.conf", port, false)) {
                    handler.prepForStatusChange(Events.RECONNECTED);

                    ts2.close();

                    // reconnect should work because we are now running with the good config
                    handler.waitForStatusChange(10, TimeUnit.SECONDS);

                    assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                    assertEquals(ts3.getURI(), nc.getConnectedUrl());
                    assertEquals(tsURI, ts3.getURI());

                    // Close this and go back to the bad config on that port, should be ok 1x
                    handler.prepForStatusChange(Events.RECONNECTED);
                    ts3.close();

                    try (NatsTestServer ts4 = new NatsTestServer("src/test/resources/operator_noacct.conf", port, false);
                            NatsTestServer ts5 = new NatsTestServer("src/test/resources/operator.conf", port2, false)) {
                        handler.waitForStatusChange(10, TimeUnit.SECONDS);
                        assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                        assertEquals(ts5.getURI(), nc.getConnectedUrl());
                    }
                }
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testReconnectAfterExpiration() throws Exception {
        TestHandler handler = new TestHandler();
        String version = NatsTestServer.generateNatsServerVersionString();

        if (!version.contains("v2")) {
            // Server version doesn't support this test
            return;
        }

        CompletableFuture<Boolean> f = new CompletableFuture<Boolean>();

        NatsServerProtocolMock.Customizer timeoutCustomizer = (ts, r,w) -> {
            f.join(); // wait until we are ready
            w.write("-ERR user authentication expired\r\n"); // Drop the line feed
            w.flush();
        };

        int port = NatsTestServer.nextPort();

        // Connect should fail on ts1
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(timeoutCustomizer, port, true);
                NatsTestServer ts2 = new NatsTestServer("src/test/resources/operator.conf", false)) {
            Options options = new Options.Builder().
                        servers(new String[] {ts.getURI(), ts2.getURI()}).
                        maxReconnects(-1).
                        noRandomize().
                        authHandler(Nats.credentials("src/test/resources/jwt_nkey/user.creds")).
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts.getURI(), nc.getConnectedUrl());

                handler.prepForStatusChange(Events.RECONNECTED);
                
                f.complete(true);

                handler.waitForStatusChange(5, TimeUnit.SECONDS);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(ts2.getURI(), nc.getConnectedUrl());

                String err = nc.getLastError();
                assertNotNull(err);
                assertTrue(err.toLowerCase().startsWith("user authentication"));
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
}