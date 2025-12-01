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
import io.nats.client.impl.Listener;
import io.nats.client.impl.ListenerForTesting;
import io.nats.client.impl.ListenerFuture;
import io.nats.client.support.JwtUtils;
import io.nats.client.utils.ResourceUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import javax.net.ssl.SSLContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.nats.client.NatsTestServer.configFileServer;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.*;
import static io.nats.client.utils.ResourceUtils.jwtResource;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

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
            Options inUrl = optionsBuilder(userPassInUrl("uuu", "ppp", ts.getPort())).maxReconnects(0).build();
            assertCanConnect(inUrl);

            // u/p in options
            Options inOptions = optionsBuilder(ts).maxReconnects(0)
                .userInfo("uuu".toCharArray(), "ppp".toCharArray()).build();
            assertCanConnect(inOptions);

            Options badUser = optionsBuilder(ts).maxReconnects(0)
                .userInfo("zzz".toCharArray(), "ppp".toCharArray()).build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(badUser));

            Options badPass = optionsBuilder(ts).maxReconnects(0)
                .userInfo("uuu".toCharArray(), "zzz".toCharArray()).build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(badPass));

            Options missingUserPass = optionsNoReconnect(ts);
            assertThrows(AuthenticationException.class, () -> Nats.connect(missingUserPass));
        }
    }

    @Test
    public void testEncodedPassword() throws Exception {
        try (NatsTestServer ts = configFileServer("encoded_pass.conf")) {
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
        }
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
        ListenerForTesting listener = new ListenerForTesting();
        Connection nc;
        Subscription sub;
        String[] customArgs = { "--user", "uuu", "--pass", "ppp" };
        int port;

        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            port = ts.getPort();
            // See config file for user/pass
            Options options = optionsBuilder(ts).maxReconnects(-1)
                    .userInfo("uuu".toCharArray(), "ppp".toCharArray()).connectionListener(listener).build();
            nc = standardConnectionWait(options);

            sub = nc.subscribe("test");
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        TestBase.flushConnection(nc);

        listener.waitForStatusChange(5, TimeUnit.SECONDS);
        assertTrue(
                Connection.Status.RECONNECTING == nc.getStatus() || Connection.Status.DISCONNECTED == nc.getStatus(), "Reconnecting status");
        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port)) {
            listenerConnectionWait(nc, listener);

            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);

            standardCloseConnection(nc);
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
        ListenerForTesting listener = new ListenerForTesting();
        int port;
        Connection nc;
        Subscription sub;
        String[] customArgs = { "--user", "uuu", "--pass", "ppp" };

        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            port = ts.getPort();
            // See config file for user/pass
            Options options = new Options.Builder().server(userPassInUrl("uuu", "ppp", ts.getPort()))
                .maxReconnects(-1).connectionListener(listener).errorListener(NOOP_EL).build();
            nc = standardConnectionWait(options);

            sub = nc.subscribe("test");
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        TestBase.flushConnection(nc);

        listener.waitForStatusChange(5, TimeUnit.SECONDS);

        Status status = nc.getStatus();
        assertTrue(
                Connection.Status.RECONNECTING == status || Connection.Status.DISCONNECTED == status, "Reconnecting status");
        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer(customArgs, port)) {
            listenerConnectionWait(nc, listener);
            nc.publish("test", null);
            flushConnection(nc, MEDIUM_FLUSH_TIMEOUT_MS);
            Message msg = sub.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testUserPassInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--user", "uuu", "--pass", "ppp" };
        String[] customArgs2 = { "--user", "uuu2", "--pass", "ppp2" };
        ListenerForTesting listener = new ListenerForTesting();
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
            Connection nc = standardConnectionWait(options);
            assertEquals(nc.getConnectedUrl(), url1);

            listener.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            listenerConnectionWait(nc, listener);
            assertEquals(nc.getConnectedUrl(), url2);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testUserPassInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--user", "uuu", "--pass", "ppp" };
        String[] customArgs2 = { "--user", "uuu2", "--pass", "ppp2" };
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts1 = new NatsTestServer(customArgs1);
             NatsTestServer ts2 = new NatsTestServer(customArgs2)) {
            String url1 = userPassInUrl("uuu", "ppp", ts1.getPort());
            Options options = optionsBuilder(url1, ts2.getNatsLocalhostUri())
                .userInfo("uuu2".toCharArray(), "ppp2".toCharArray())
                .maxReconnects(4)
                .noRandomize()
                .connectionListener(listener)
                .pingInterval(Duration.ofMillis(100)).build();
            Connection nc = standardConnectionWait(options);
            assertEquals(nc.getConnectedUrl(), url1);

            listener.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            listener.waitForStatusChange(10, TimeUnit.SECONDS);
            assertConnected(nc);
            assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testTokenInURLClusteredWithDifferentUser() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        ListenerForTesting listener = new ListenerForTesting();
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
            Connection nc = standardConnectionWait(options);
            assertEquals(nc.getConnectedUrl(), url1);

            listener.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();
            listener.waitForStatusChange(2, TimeUnit.SECONDS);

            standardConnectionWait(nc);
            assertEquals(nc.getConnectedUrl(), url2);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testTokenInURLWithFallback() throws Exception {
        String[] customArgs1 = { "--auth", "token_one" };
        String[] customArgs2 = { "--auth", "token_two" };
        ListenerForTesting listener = new ListenerForTesting();
        Connection nc;
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
            nc = standardConnectionWait(options);
            assertEquals(nc.getConnectedUrl(), url1);

            listener.prepForStatusChange(Events.RESUBSCRIBED);
            ts1.close();

            listener.waitForStatusChange(STANDARD_CONNECTION_WAIT_MS, TimeUnit.MILLISECONDS);
            listenerConnectionWait(nc, listener);
            assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
            standardCloseConnection(nc);
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
            standardConnectionWait(nc);
            standardCloseConnection(nc);

            // fails with no nkey
            Options noNkey = optionsBuilder(ts).maxReconnects(0)
                .authHandler(new AuthHandlerForTesting(null)).build();
            assertThrows(IOException.class, () -> Nats.connect(noNkey));
        }
    }

    @Test
    public void testJWTAuthWithCredsFile() throws Exception {
        // manual auth handler or credential path
        NatsTestServer ts = sharedConfigServer("operator.conf");
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
        standardConnectionWait(nc);
        standardCloseConnection(nc);
    }

    @Test
    public void testJWTAuthWithCredsFileAlso() throws Exception {
        //test Nats.connect method
        try (NatsTestServer ts = NatsTestServer.configFileServer("operatorJnatsTest.conf")) {
            Connection nc = Nats.connect(ts.getServerUri(), Nats.credentials(jwtResource("userJnatsTest.creds")));
            standardConnectionWait(nc);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testWsJWTAuthWithCredsFile() throws Exception {
        try (NatsTestServer ts = NatsTestServer.configFileServer("ws_operator.conf")) {
            String uri = ts.getLocalhostUri(WS);
            // in options
            Options options = optionsBuilder(uri).maxReconnects(0)
                .authHandler(getUserCredsAuthHander()).build();
            assertCanConnect(options);

            // directly Nats.connect
            Connection nc = Nats.connect(uri, getUserCredsAuthHander());
            standardConnectionWait(nc);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testWssJWTAuthWithCredsFile() throws Exception {
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        try (NatsTestServer ts = NatsTestServer.configFileServer("wss_operator.conf"))
        {
            String uri = ts.getLocalhostUri("wss");
            Options options = optionsBuilder(uri).maxReconnects(0).sslContext(ctx)
                .authHandler(getUserCredsAuthHander()).build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testStaticJWTAuth() throws Exception {
        // from src/test/resources/jwt_nkey/user.creds
        String jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiI3UE1GTkc0R1c1WkE1NEg3N09TUUZKNkJNQURaSUQ2NTRTVk1XMkRFQVZINVIyUVU0MkhBIiwiaWF0IjoxNTY1ODg5ODk4LCJpc3MiOiJBQUhWU0k1NVlQTkJRWjVQN0Y2NzZDRkFPNFBIQlREWUZRSUVHVEtMUVRJUEVZUEZEVEpOSEhPNCIsIm5hbWUiOiJkZW1vIiwic3ViIjoiVUMzT01MSlhUWVBZN0ZUTVVZNUNaNExHRVdRSTNZUzZKVFZXU0VGRURBMk9MTEpZSVlaVFo3WTMiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.ROSJ7D9ETt9c8ZVHxsM4_FU2dBRLh5cNfb56MxPQth74HAxxtGMl0nn-9VVmWjXgFQn4JiIbwrGfFDBRMzxsAA";
        String nkey = "SUAFYHVVQVOIDOOQ4MTOCTLGNZCJ5PZ4HPV5WAPROGTEIOF672D3R7GBY4";

        NatsTestServer ts = sharedConfigServer("operator.conf");
        Options options = optionsBuilder(ts).maxReconnects(0)
            .authHandler(Nats.staticCredentials(jwt.toCharArray(), nkey.toCharArray())).build();
        assertCanConnect(options);
    }

    @Test
    public void testReconnectWithAuth() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        // Connect should fail on ts2
        try (NatsTestServer ts = NatsTestServer.configFileServer("operator.conf")) { // closed in test, so cannot be shared
            NatsTestServer ts2 = sharedConfigServer("operator.conf"); // do not auto close this!!
            Options options = optionsBuilder(ts.getServerUri(), ts2.getServerUri())
                    .noRandomize().maxReconnects(-1).authHandler(getUserCredsAuthHander()).build();
            Connection nc = newConnection(options);
            assertEquals(ts.getServerUri(), nc.getConnectedUrl());

            listener.prepForStatusChange(Events.RECONNECTED);

            ts.close();

            // Reconnect will fail because ts has the same auth error
            listener.waitForStatusChange(5, TimeUnit.SECONDS);
            assertConnected(nc);
            assertEquals(ts2.getServerUri(), nc.getConnectedUrl());
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testCloseOnReconnectWithSameError() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();

        // Connect should fail on ts1
        try (NatsTestServer ts2 = NatsTestServer.configFileServer("operator.conf")) { // closed in test, so cannot be shared
            NatsTestServer ts1 = sharedConfigServer("operator_noacct.conf"); // do not auto close this!!!
            Options options = optionsBuilder(ts1.getServerUri(), ts2.getServerUri())
                    .maxReconnects(-1).connectionTimeout(Duration.ofSeconds(2)).noRandomize()
                    .authHandler(getUserCredsAuthHander()).build();
            Connection nc = newConnection(options);
            assertEquals(ts2.getServerUri(), nc.getConnectedUrl());

            listener.prepForStatusChange(Events.CLOSED);

            ts2.close();

            // Reconnect will fail because ts has the same auth error
            listener.waitForStatusChange(6, TimeUnit.SECONDS);
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testThatAuthErrorIsCleared() throws Exception {
        // this test can't use any of the shared servers since it closes servers
        // Connect should fail on ts1
        try (NatsTestServer ts1 = NatsTestServer.configFileServer("operator_noacct.conf");
             NatsTestServer ts2 = NatsTestServer.configFileServer("operator.conf")) {

            Options options = optionsBuilder(ts1.getServerUri(), ts2.getServerUri())
                .noRandomize()
                .maxReconnects(-1)
                .connectionTimeout(Duration.ofSeconds(5))
                .reconnectWait(Duration.ofSeconds(1)) // wait a tad to allow restarts
                .authHandler(getUserCredsAuthHander())
                .errorListener(new ListenerForTesting())
                .build();
            Connection nc = standardConnectionWait(options);
            assertEquals(ts2.getServerUri(), nc.getConnectedUrl());

            String tsURI = ts1.getServerUri();
            int port1 = ts1.getPort();
            int port2 = ts2.getPort();

            ts1.close();
            // ts3 will be at the same port that ts was
            try (NatsTestServer ts3 = configFileServer("operator.conf", port1)) {
                ListenerForTesting listener = new ListenerForTesting();
                listener.prepForStatusChange(Events.RECONNECTED);

                ts2.close();

                // reconnect should work because we are now running with the good config
                listenerConnectionWait(nc, listener, VERY_LONG_CONNECTION_WAIT_MS);

                assertEquals(ts3.getServerUri(), nc.getConnectedUrl());
                assertEquals(tsURI, ts3.getServerUri());

                // Close this and go back to the bad config on that port, should be ok 1x
                listener.prepForStatusChange(Events.RECONNECTED);
                ts3.close();

                try (NatsTestServer ignored = configFileServer("operator_noacct.conf", port1);
                     NatsTestServer ts5 = configFileServer("operator.conf", port2)) {
                    listenerConnectionWait(nc, listener, VERY_LONG_CONNECTION_WAIT_MS);
                    assertEquals(ts5.getServerUri(), nc.getConnectedUrl());
                }
            }
            standardCloseConnection(nc);
        }
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
            NatsTestServer ts2 = sharedConfigServer("operator.conf");

            Options options = optionsBuilder(mockTs, ts2)
                .maxReconnects(-1)
                .noRandomize()
                .authHandler(getUserCredsAuthHander())
                .errorListener(listener)
                .connectionListener(listener)
                .build();

            ListenerFuture f = listener.prepForConnectionEvent(Events.RECONNECTED);

            try (Connection nc = newConnection(options)) {
                assertEquals(mockTs.getServerUri(), nc.getConnectedUrl());
                fMock.complete(true);
                listener.validateReceived(f);
                assertEquals(ts2.getServerUri(), nc.getConnectedUrl());

                String err = nc.getLastError();
                assertNotNull(err);
                assertTrue(err.toLowerCase().contains(errText));
            }
        }
    }

    @Test
    public void testRealUserAuthenticationExpired() throws Exception {
        Listener listener = new Listener(false);
        ListenerFuture fExpired = listener.prepForError("User Authentication Expired");

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

        try (NatsTestServer ts = NatsTestServer.configFileServer("operatorJnatsTest.conf")) {
            Options options = Options.builder()
                .server(ts.getServerUri())
                .credentialPath(credsFile)
                .connectionListener(listener)
                .errorListener(listener)
                .maxReconnects(5)
                .build();
            try (Connection ignored = newConnection(options)) {
                listener.validateReceived(fExpired);
            }
            catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Authorization Violation"));
            }
        }
    }
}
