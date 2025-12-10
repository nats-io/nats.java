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

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.Connection.Status;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.support.JwtUtils;
import io.nats.client.support.Listener;
import io.nats.client.utils.ResourceUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.parallel.Isolated;

import javax.net.ssl.SSLContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.Listener.LONG_VALIDATE_TIMEOUT;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.NOOP_EL;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ResourceUtils.jwtResource;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

@Isolated
public class AuthTests extends TestBase {

    private String userPassInUrl(String user, String pass, int port) {
        return "nats://" + user + ":" + pass + "@" + NatsRunnerUtils.getDefaultLocalhostHost().host + ":" + port;
    }

    private String tokenInUrl(String userOrToken, int port) {
        return "nats://" + userOrToken + "@" + NatsRunnerUtils.getDefaultLocalhostHost().host + ":" + port;
    }

    public static AuthHandler getUserCredsAuthHander() {
        return Nats.credentials(jwtResource("user.creds"));
    }

    @Test
    public void testUserPass() throws Exception {
        String[] customArgs = { "--user", "uuu", "--pass", "ppp" };
        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            // u/p in url
            Options upInUrlOpts = optionsBuilder(userPassInUrl("uuu", "ppp", ts.getPort())).maxReconnects(0).build();
            assertCanConnect(upInUrlOpts);

            // u/p in options
            Options upInOptionsOpts = optionsBuilder(ts).maxReconnects(0)
                .userInfo("uuu".toCharArray(), "ppp".toCharArray()).build();
            assertCanConnect(upInOptionsOpts);

            Options badUserOpts = optionsBuilder(ts).maxReconnects(0).connectionTimeout(10000)
                .userInfo("zzz".toCharArray(), "ppp".toCharArray()).build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(badUserOpts));

            Options badPassOpts = optionsBuilder(ts).maxReconnects(0).connectionTimeout(10000)
                .userInfo("uuu".toCharArray(), "zzz".toCharArray()).build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(badPassOpts));

            Options missingUserPassOpts = optionsBuilder(ts).maxReconnects(0).connectionTimeout(10000).build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(missingUserPassOpts));
        }
    }

    @Test
    public void testEncodedPassword() throws Exception {
        runInConfiguredServer("encoded_pass.conf", ts -> {
            int port = ts.getPort();
            assertEncoded("space%20space", port);
            assertEncoded("colon%3Acolon", port);
            assertEncoded("colon%3acolon", port); // just making sure lower case hex
            assertEncoded("quote%27quote", port);
            assertEncoded("slash%2Fslash", port);
            assertEncoded("question%3Fquestion", port);
            assertEncoded("pound%23pound", port);
            assertEncoded("sqleft%5Bsqleft", port);
            assertEncoded("sqright%5Dsqright", port);
            assertEncoded("at%40at", port);
            assertEncoded("bang%21bang", port);
            assertEncoded("dollar%24dollar", port);
            assertEncoded("amp%26amp", port);
            assertEncoded("comma%2Ccomma", port);
            assertEncoded("parenleft%28parenleft", port);
            assertEncoded("parentright%29parentright", port);
            assertEncoded("asterix%2Aasterix", port);
            assertEncoded("plus%2Bplus", port);
            assertEncoded("semi%3Bsemi", port);
            assertEncoded("eq%3Deq", port);
            assertEncoded("pct%25pct", port);
            assertEncoded("%2b%3a%c2%a1%c2%a2%c2%a3%c2%a4%c2%a5%c2%a6%c2%a7%c2%a8%c2%a9%c2%aa%c2%ab%c2%ac%20%f0%9f%98%80", port);

            // a plus sign in a user or pass is a plus sign, not a space
            assertThrows(AuthenticationException.class, () -> assertEncoded("space+space", port));
        });
    }

    private void assertEncoded(String encoded, int port) throws IOException, InterruptedException {
        String url = userPassInUrl("u" + encoded, "p" + encoded, port);
        Options options = optionsBuilder(url).build();
        Connection c = Nats.connect(options);
        c.getServerInfo();
        c.close();
    }

    @Test
    @EnabledOnOs({ WINDOWS })
    public void testNeedsJsonEncoding() throws Exception {
        assertNeedsJsonEncoding("\n");
        assertNeedsJsonEncoding("\b");
        assertNeedsJsonEncoding("\f");
        assertNeedsJsonEncoding("\r");
        assertNeedsJsonEncoding("\t");
        assertNeedsJsonEncoding("/");
        assertNeedsJsonEncoding("" + (char)9);
        assertNeedsJsonEncoding("\\");
    }

    private static void assertNeedsJsonEncoding(String test) throws Exception {
        String user = "u" + test + "u";
        String pass = "p" + test + "p";
        String[] customArgs = {"--user", "\"" + user + "\"", "--pass", "\"" + pass + "\""};
        try (NatsTestServer ts = new NatsTestServer(
            NatsServerRunner.builder().customArgs(customArgs))) {
            // See config file for user/pass
            Options options = optionsBuilder(ts)
                .userInfo(user, pass)
                .maxReconnects(0).build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testUserPassOnReconnect() throws Exception {
        Listener listener = new Listener();
        Connection nc;
        Subscription sub;
        String[] customArgs = { "--user", "uuu", "--pass", "ppp" };
        int port;

        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            port = ts.getPort();
            // See config file for user/pass
            Options options = optionsBuilder(ts).maxReconnects(-1)
                    .userInfo("uuu".toCharArray(), "ppp".toCharArray()).connectionListener(listener).build();
            nc = managedConnect(options);

            sub = nc.subscribe("test");
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);

            listener.queueConnectionEvent(Events.DISCONNECTED);
        }

        TestBase.flushConnection(nc);
        listener.validate();

        listener.queueConnectionEvent(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port)) {
            confirmConnected(nc); // wait for reconnect
            listener.validate();

            nc.publish("test", null);
//            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);

            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);

            closeAndConfirm(nc);
        }
    }

    @Test
    public void testUserBCryptPass() throws Exception {
        /*
         * use a bcrypt hash generator on the cleartext pass
         * ppp -> $2a$12$UjzncyjtsE6rJG4LSGk.JOweXV3P2umZ38gHuj4OMY0X/nQudiDgG
         */
        String[] customArgs = { "--user", "uuu", "--pass",
                "$2a$12$UjzncyjtsE6rJG4LSGk.JOweXV3P2umZ38gHuj4OMY0X/nQudiDgG" };
        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            // See config file for user/pass
            Options options = optionsBuilder(ts).maxReconnects(0)
                    .userInfo("uuu".toCharArray(), "ppp".toCharArray()).build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testUserPassInURLOnReconnect() throws Exception {
        Listener listener = new Listener();
        int port;
        Connection nc;
        Subscription sub;
        String[] customArgs = { "--user", "uuu", "--pass", "ppp" };

        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            port = ts.getPort();
            // See config file for user/pass
            Options options = new Options.Builder().server(userPassInUrl("uuu", "ppp", ts.getPort()))
                .maxReconnects(-1).connectionListener(listener).errorListener(NOOP_EL).build();
            nc = managedConnect(options);

            sub = nc.subscribe("test");
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            listener.queueConnectionEvent(Events.DISCONNECTED);
        }

        TestBase.flushConnection(nc);

        listener.validate();

        Status status = nc.getStatus();
        assertTrue(
                Connection.Status.RECONNECTING == status || Connection.Status.DISCONNECTED == status, "Reconnecting status");
        listener.queueConnectionEvent(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port)) {
            confirmConnected(nc); // wait for reconnect
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            closeAndConfirm(nc);
        }
    }

    @Test
    public void testUserPassInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--user", "uuu", "--pass", "ppp" };
        String[] customArgs2 = { "--user", "uuu2", "--pass", "ppp2" };
        Listener listener = new Listener();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1);
             NatsTestServer ts2 = new NatsTestServer(customArgs2)) {
            String url1 = userPassInUrl("uuu", "ppp", ts1.getPort());
            String url2 = userPassInUrl("uuu2", "ppp2", ts2.getPort());
            Options options = optionsBuilder(url1, url2)
                .maxReconnects(4)
                .noRandomize()
                .connectionListener(listener)
                .pingInterval(Duration.ofMillis(100))
                .build();

            try (Connection nc = managedConnect(options)) {
                assertEquals(nc.getConnectedUrl(), url1);
                listener.queueConnectionEvent(Events.RESUBSCRIBED);
                ts1.close();
                confirmConnected(nc); // wait for reconnect
                assertEquals(nc.getConnectedUrl(), url2);
            }
        }
    }

    @Test
    public void testUserPassInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--user", "uuu", "--pass", "ppp" };
        String[] customArgs2 = { "--user", "uuu2", "--pass", "ppp2" };
        Listener listener = new Listener();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1);
             NatsTestServer ts2 = new NatsTestServer(customArgs2)) {
            String url1 = userPassInUrl("uuu", "ppp", ts1.getPort());
            Options options = optionsBuilder(url1, ts2.getNatsLocalhostUri())
                .userInfo("uuu2".toCharArray(), "ppp2".toCharArray())
                .maxReconnects(4)
                .noRandomize()
                .connectionListener(listener)
                .pingInterval(Duration.ofMillis(100)).build();
            try (Connection nc = managedConnect(options)) {
                assertEquals(nc.getConnectedUrl(), url1);

                listener.queueConnectionEvent(Events.RESUBSCRIBED, LONG_VALIDATE_TIMEOUT);
                ts1.close();
                listener.validate();
                assertConnected(nc);
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
            }
        }
    }

    @Test
    public void testTokenInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        Listener listener = new Listener();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1);
             NatsTestServer ts2 = new NatsTestServer(customArgs2)) {
            String url1 = tokenInUrl("token_one", ts1.getPort());
            String url2 = tokenInUrl("token_two", ts2.getPort());
            Options options = optionsBuilder(url1, url2)
                .maxReconnects(4)
                .noRandomize()
                .connectionListener(listener)
                .pingInterval(Duration.ofMillis(100))
                .build();
            try (Connection nc = managedConnect(options)) {
                assertEquals(nc.getConnectedUrl(), url1);

                listener.queueConnectionEvent(Events.RESUBSCRIBED);
                ts1.close();
                listener.validate();

                confirmConnected(nc); // wait for reconnect
                assertEquals(nc.getConnectedUrl(), url2);
            }
        }
    }

    @Test
    public void testTokenInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        Listener listener = new Listener();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1);
             NatsTestServer ts2 = new NatsTestServer(customArgs2)) {
            String url1 = tokenInUrl("token_one", ts1.getPort());
            Options options = optionsBuilder(url1, ts2.getNatsLocalhostUri())
                .token("token_two".toCharArray())
                .maxReconnects(4)
                .noRandomize()
                .connectionListener(listener)
                .pingInterval(Duration.ofMillis(100))
                .build();

            try (Connection nc = managedConnect(options)) {
                assertEquals(nc.getConnectedUrl(), url1);

                listener.queueConnectionEvent(Events.RESUBSCRIBED);
                ts1.close();
                listener.validate();

                confirmConnected(nc); // wait for reconnect
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
            }
        }
    }

    @Test
    public void testToken() throws Exception {
        String[] customArgs = { "--auth", "token" };
        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            // token in options
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .token("token".toCharArray())
                .build();
            assertCanConnect(options);

            // token in url
            options = optionsBuilder(tokenInUrl("token", ts.getPort()))
                .maxReconnects(0).build();
            assertCanConnect(options);

            // incorrect token
            Options incorrectToken = optionsBuilder(ts)
                .maxReconnects(0)
                .token("incorrectToken".toCharArray())
                .build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(incorrectToken));

            // incorrect token
            Options missingToken = optionsBuilder(ts)
                .maxReconnects(0)
                .build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(missingToken));
        }
    }

    String createNKeyConfigFile(char[] nkey) throws Exception {
        File tmp = Files.createTempFile("nats_java_test", ".conf").toFile();
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
        String configFilePath = createNKeyConfigFile(theKey.getPublicKey());

        NatsServerRunner.Builder b = NatsServerRunner.builder().configFilePath(configFilePath);
        try (NatsTestServer ts = new NatsTestServer(b)) {

            Options authHandlerOptions = optionsBuilder(ts).maxReconnects(0)
                .authHandler(new AuthHandlerForTesting(theKey)).build();
            assertCanConnect(authHandlerOptions);

            Options staticOptions = optionsBuilder(ts).maxReconnects(0)
                .authHandler(Nats.staticCredentials(null, theKey.getSeed())).build();
            assertCanConnect(staticOptions);

            // direct through Nats.connect
            Connection nc = Nats.connect(ts.getServerUri(), Nats.staticCredentials(null, theKey.getSeed()));
            confirmConnectedThenClosed(nc);

            // fails with no nkey
            Options noNkey = optionsBuilder(ts).maxReconnects(0)
                .authHandler(new AuthHandlerForTesting(null)).build();
            assertThrows(IOException.class, () -> Nats.connect(noNkey));
        }
    }

    @Test
    public void testJWTAuthWithCredsFile() throws Exception {
        // manual auth handler or credential path
        runInSharedConfiguredServer("operator.conf", ts -> {
            Options options = optionsBuilder(ts).maxReconnects(0)
                .authHandler(getUserCredsAuthHander())
                .build();
            assertCanConnect(options);

            options = optionsBuilder(ts).maxReconnects(0)
                .credentialPath(jwtResource("user.creds"))
                .build();
            assertCanConnect(options);

            //test Nats.connect method
            Connection nc = Nats.connect(ts.getServerUri(), getUserCredsAuthHander());
            confirmConnectedThenClosed(nc);
        });
    }

    @Test
    public void testJWTAuthWithCredsFileAlso() throws Exception {
        //test Nats.connect method
        runInConfiguredServer("operatorJnatsTest.conf", ts -> {
            Connection nc = Nats.connect(ts.getServerUri(), Nats.credentials(jwtResource("userJnatsTest.creds")));
            confirmConnectedThenClosed(nc);
        });
    }

    @Test
    public void testWsJWTAuthWithCredsFile() throws Exception {
        runInConfiguredServer("ws_operator.conf", ts -> {
            String uri = ts.getLocalhostUri(WS);
            // in options
            Options options = optionsBuilder(uri).maxReconnects(0)
                .authHandler(getUserCredsAuthHander()).build();
            assertCanConnect(options);

            // directly Nats.connect
            Connection nc = Nats.connect(uri, getUserCredsAuthHander());
            confirmConnectedThenClosed(nc);
        });
    }

    @Test
    public void testWssJWTAuthWithCredsFile() throws Exception {
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        runInConfiguredServer("wss_operator.conf", ts -> {
            String uri = ts.getLocalhostUri("wss");
            Options options = optionsBuilder(uri).maxReconnects(0).sslContext(ctx)
                .authHandler(getUserCredsAuthHander()).build();
            assertCanConnect(options);
        });
    }

    @Test
    public void testStaticJWTAuth() throws Exception {
        // from src/test/resources/jwt_nkey/user.creds
        String jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiI3UE1GTkc0R1c1WkE1NEg3N09TUUZKNkJNQURaSUQ2NTRTVk1XMkRFQVZINVIyUVU0MkhBIiwiaWF0IjoxNTY1ODg5ODk4LCJpc3MiOiJBQUhWU0k1NVlQTkJRWjVQN0Y2NzZDRkFPNFBIQlREWUZRSUVHVEtMUVRJUEVZUEZEVEpOSEhPNCIsIm5hbWUiOiJkZW1vIiwic3ViIjoiVUMzT01MSlhUWVBZN0ZUTVVZNUNaNExHRVdRSTNZUzZKVFZXU0VGRURBMk9MTEpZSVlaVFo3WTMiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.ROSJ7D9ETt9c8ZVHxsM4_FU2dBRLh5cNfb56MxPQth74HAxxtGMl0nn-9VVmWjXgFQn4JiIbwrGfFDBRMzxsAA";
        String nkey = "SUAFYHVVQVOIDOOQ4MTOCTLGNZCJ5PZ4HPV5WAPROGTEIOF672D3R7GBY4";

        runInSharedConfiguredServer("operator.conf", ts -> {
            Options options = optionsBuilder(ts).maxReconnects(0)
                .authHandler(Nats.staticCredentials(jwt.toCharArray(), nkey.toCharArray())).build();
            assertCanConnect(options);
        });
    }

    @Test
    public void testReconnectWithAuth() throws Exception {
        Listener listener = new Listener();
        // Connect should fail on ts2
        runInConfiguredServer("operator.conf", ts1 -> { // closed as part of test, so cannot be shared
            runInSharedConfiguredServer("operator.conf", ts2 -> {
                Options options = optionsBuilder(ts1.getServerUri(), ts2.getServerUri())
                    .noRandomize()
                    .maxReconnects(-1)
                    .authHandler(getUserCredsAuthHander())
                    .connectionListener(listener)
                    .errorListener(listener)
                    .build();
                Connection nc = managedConnect(options);
                assertEquals(ts1.getServerUri(), nc.getConnectedUrl());

                listener.queueConnectionEvent(Events.RECONNECTED);

                ts1.close();

                // Reconnect will fail because ts has the same auth error
                listener.validate();
                assertConnected(nc);
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
                closeAndConfirm(nc);
            });
        });
    }

    @Test
    public void testCloseOnReconnectWithSameError() throws Exception {
        // Connect should fail on ts1
        runInConfiguredServer("operator.conf", ts2 -> { // closed in test, so cannot be shared
            runInSharedConfiguredServer("operator_noacct.conf", ts1 -> {
                Listener listener = new Listener();
                Options options = optionsBuilder(ts1.getServerUri(), ts2.getServerUri())
                    .maxReconnects(-1)
                    .connectionTimeout(Duration.ofSeconds(2))
                    .connectionListener(listener)
                    .noRandomize()
                    .authHandler(getUserCredsAuthHander())
                    .build();

                try (Connection nc = managedConnect(options)) {
                    assertEquals(ts2.getServerUri(), nc.getConnectedUrl());

                    listener.queueConnectionEvent(Events.CLOSED, LONG_VALIDATE_TIMEOUT);

                    ts2.close();

                    // Reconnect will fail because ts has the same auth error
                    listener.validate();
                }
            });
        });
    }

    @Test
    public void testThatAuthErrorIsCleared() throws Exception {
        AtomicReference<Connection> ncRef = new AtomicReference<>();
        AtomicReference<String> server2Ref = new AtomicReference<>();
        AtomicInteger port2Ref = new AtomicInteger();

        runInConfiguredServer("operator_noacct.conf", ts1 -> {
            runInConfiguredServer("operator.conf", ts2 -> {
                String server1 = ts1.getServerUri();
                String server2 = ts2.getServerUri();
                server2Ref.set(server2);
                port2Ref.set(ts2.getPort());
                Options options = optionsBuilder(server1, server2)
                    .noRandomize()
                    .maxReconnects(-1)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .reconnectWait(Duration.ofSeconds(1)) // wait a tad to allow restarts
                    .authHandler(getUserCredsAuthHander())
                    .build();
                Connection nc = managedConnect(options);
                ncRef.set(nc);
                assertEquals(server2, nc.getConnectedUrl());
            });

            runInConfiguredServer("operator.conf", port2Ref.get(), ts2 -> {
                confirmConnected(ncRef.get());
                assertEquals(server2Ref.get(), ncRef.get().getConnectedUrl());
            });
        });
    }

    @Test
    public void testReconnectAfterExpiration() throws Exception {
        _testReconnectAfter("user authentication expired");
    }

    @Test
    public void testReconnectAfterRevoked() throws Exception {
        _testReconnectAfter("user authentication revoked");
    }

    @Test
    public void testReconnectAfterAuthorizationViolation() throws Exception {
        _testReconnectAfter("authorization violation");
    }

    @Test
    public void testReconnectAfterAccountAuthenticationExpired() throws Exception {
        _testReconnectAfter("account authentication expired");
    }

    private static void _testReconnectAfter(String errText) throws Exception {
        Listener listener = new Listener();
        CompletableFuture<Boolean> fMock = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer timeoutCustomizer = (ts, r, w) -> {
            fMock.join(); // wait until we are ready
            w.write("-ERR " + errText + "\r\n"); // Drop the line feed
            w.flush();
        };

        int port = NatsTestServer.nextPort();

        // Connect should fail on ts1
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(timeoutCustomizer, port, true)) {
            runInSharedConfiguredServer("operator.conf", ts2 -> {
                Options options = optionsBuilder(mockTs, ts2)
                    .maxReconnects(-1)
                    .noRandomize()
                    .authHandler(getUserCredsAuthHander())
                    .errorListener(listener)
                    .connectionListener(listener)
                    .build();

                listener.queueConnectionEvent(Events.RECONNECTED);

                try (Connection nc = standardConnect(options)) {
                    assertEquals(mockTs.getServerUri(), nc.getConnectedUrl());
                    fMock.complete(true);
                    listener.validate();
                    assertEquals(ts2.getServerUri(), nc.getConnectedUrl());

                    String err = nc.getLastError();
                    assertNotNull(err);
                    assertTrue(err.toLowerCase().contains(errText));
                }
            });
        }
    }

    @Test
    public void testRealUserAuthenticationExpired() throws Exception {
        Listener listener = new Listener();

        String accountSeed = "SAAPXJRFMUYDUH3NOZKE7BS2ZDO2P4ND7G6W743MTNA3KCSFPX3HNN6AX4";
        String accountId = "ACPWDUYSZRRF7XAEZKUAGPUH6RPICWEHSTFELYKTOWUVZ4R2XMP4QJJX";
        String userSeed = "SUAJ44FQWKEWGRSIPRFCIGDTVYSMUMRRHB4CPFXXRG5GODO5XY7S2L45ZA";

        NKey nKeyAccount = NKey.fromSeed(accountSeed.toCharArray());

        NKey nKeyUser = NKey.fromSeed(userSeed.toCharArray());
        String publicUserKey = new String(nKeyUser.getPublicKey());

        long expires = 1000;
        Duration expiration = Duration.ofMillis(expires);
        String jwt = JwtUtils.issueUserJWT(nKeyAccount, accountId, publicUserKey, "jnatsTestUser", expiration);

        String creds = String.format(JwtUtils.NATS_USER_JWT_FORMAT, jwt, new String(nKeyUser.getSeed()));
        String credsFile = ResourceUtils.createTempFile("nats_java_test", ".creds", creds.split("\\Q\\n\\E"));

        runInConfiguredServer("operatorJnatsTest.conf", ts -> {
            Options options = Options.builder()
                .server(ts.getServerUri())
                .credentialPath(credsFile)
                .connectionListener(listener)
                .errorListener(listener)
                .maxReconnects(5)
                .build();

            listener.queueError("User Authentication Expired");

            // so these shenanigans to test this...
            // 1. sometimes it connects and the expiration comes while connected
            // 2. sometimes the connect exceptions right away
            // 3. sometimes the connect happens but still exceptions
            // this is all simply the speed and timing of the machine/server/connection
            try (Connection ignored = Nats.connect(options)) {
                sleep(2500); // 1. connected, so the validate() at the end verifies this
            }
            catch (AuthenticationException ignore) {
                return; // 2. sometimes the connect exceptions right away
            }
            catch (RuntimeException e) {
                // 3. sometimes the connect happens but still exceptions
                Throwable t = e;
                while (t != null) {
                    if (t instanceof AuthenticationException) {
                        return;
                    }
                    t = t.getCause();
                }
                fail(e);
            }
            listener.validate(); // 1. finish
        });
    }
}
