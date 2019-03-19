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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.junit.Test;

import io.nats.client.ConnectionListener.Events;
import io.nats.client.impl.DataPort;
import io.nats.client.utils.CloseOnUpgradeAttempt;

public class OptionsTests {
    @Test
    public void testDefaultOptions() {
        Options o = new Options.Builder().build();

        assertEquals("default one server", 1, o.getServers().size());
        assertEquals("default url", Options.DEFAULT_URL, o.getServers().toArray()[0].toString());

        assertEquals("default data port type", Options.DEFAULT_DATA_PORT_TYPE, o.getDataPortType());

        assertEquals("default verbose", false, o.isVerbose());
        assertEquals("default pedantic", false, o.isPedantic());
        assertEquals("default norandomize", false, o.isNoRandomize());
        assertEquals("default oldstyle", false, o.isOldRequestStyle());
        assertEquals("default noEcho", false, o.isNoEcho());
        assertEquals("default UTF8 Support", false, o.supportUTF8Subjects());

        assertNull("default username", o.getUsername());
        assertNull("default password", o.getPassword());
        assertNull("default token", o.getToken());
        assertNull("default connection name", o.getConnectionName());

        assertNull("default ssl context", o.getSslContext());

        assertEquals("default max reconnect", Options.DEFAULT_MAX_RECONNECT, o.getMaxReconnect());
        assertEquals("default ping max", Options.DEFAULT_MAX_PINGS_OUT, o.getMaxPingsOut());
        assertEquals("default reconnect buffer size", Options.DEFAULT_RECONNECT_BUF_SIZE, o.getReconnectBufferSize());

        assertEquals("default reconnect wait", Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait());
        assertEquals("default connection timeout", Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout());
        assertEquals("default ping interval", Options.DEFAULT_PING_INTERVAL, o.getPingInterval());
        assertEquals("default cleanup interval", Options.DEFAULT_REQUEST_CLEANUP_INTERVAL,
                o.getRequestCleanupInterval());

        assertNull("error handler", o.getErrorListener());
        assertNull("disconnect handler", o.getConnectionListener());
    }

    @Test
    public void testChainedBooleanOptions() throws NoSuchAlgorithmException {
        Options o = new Options.Builder().verbose().pedantic().noRandomize().supportUTF8Subjects().noEcho().oldRequestStyle().build();
        assertNull("default username", o.getUsername());
        assertEquals("chained verbose", true, o.isVerbose());
        assertEquals("chained pedantic", true, o.isPedantic());
        assertEquals("chained norandomize", true, o.isNoRandomize());
        assertEquals("chained oldstyle", true, o.isOldRequestStyle());
        assertEquals("chained noecho", true, o.isNoEcho());
        assertEquals("chained utf8", true, o.supportUTF8Subjects());
    }

    @Test
    public void testChainedStringOptions() throws NoSuchAlgorithmException {
        Options o = new Options.Builder().userInfo("hello", "world").connectionName("name").build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained username", "hello", o.getUsername());
        assertEquals("chained password", "world", o.getPassword());
        assertEquals("chained connection name", "name", o.getConnectionName());
    }

    @Test
    public void testChainedSecure() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        SSLContext.setDefault(ctx);
        Options o = new Options.Builder().secure().build();
        assertEquals("chained context", ctx, o.getSslContext());
    }

    @Test
    public void testChainedSSLOptions() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        Options o = new Options.Builder().sslContext(ctx).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained context", ctx, o.getSslContext());
    }

    @Test
    public void testChainedIntOptions() {
        Options o = new Options.Builder().maxReconnects(100).maxPingsOut(200).reconnectBufferSize(300).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained max reconnect", 100, o.getMaxReconnect());
        assertEquals("chained ping max", 200, o.getMaxPingsOut());
        assertEquals("chained reconnect buffer size", 300, o.getReconnectBufferSize());
    }

    @Test
    public void testChainedDurationOptions() {
        Options o = new Options.Builder().reconnectWait(Duration.ofMillis(101))
                .connectionTimeout(Duration.ofMillis(202)).pingInterval(Duration.ofMillis(303))
                .requestCleanupInterval(Duration.ofMillis(404)).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained reconnect wait", Duration.ofMillis(101), o.getReconnectWait());
        assertEquals("chained connection timeout", Duration.ofMillis(202), o.getConnectionTimeout());
        assertEquals("chained ping interval", Duration.ofMillis(303), o.getPingInterval());
        assertEquals("chained cleanup interval", Duration.ofMillis(404), o.getRequestCleanupInterval());
    }

    @Test
    public void testChainedErrorHandler() {
        TestHandler handler = new TestHandler();
        Options o = new Options.Builder().errorListener(handler).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained error handler", handler, o.getErrorListener());
    }

    @Test
    public void testChainedConnectionListener() {
        ConnectionListener cHandler = (c, e) -> System.out.println("connection event" + e);
        Options o = new Options.Builder().connectionListener(cHandler).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNull("error handler", o.getErrorListener());
        assertTrue("chained connection handler", cHandler == o.getConnectionListener());
    }

    @Test
    public void testPropertiesBooleanBuilder() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_VERBOSE, "true");
        props.setProperty(Options.PROP_PEDANTIC, "true");
        props.setProperty(Options.PROP_NORANDOMIZE, "true");
        props.setProperty(Options.PROP_USE_OLD_REQUEST_STYLE, "true");
        props.setProperty(Options.PROP_OPENTLS, "true");
        props.setProperty(Options.PROP_NO_ECHO, "true");
        props.setProperty(Options.PROP_UTF8_SUBJECTS, "true");

        Options o = new Options.Builder(props).build();
        assertNull("default username", o.getUsername());
        assertEquals("property verbose", true, o.isVerbose());
        assertEquals("property pedantic", true, o.isPedantic());
        assertEquals("property norandomize", true, o.isNoRandomize());
        assertEquals("property oldstyle", true, o.isOldRequestStyle());
        assertEquals("property noecho", true, o.isNoEcho());
        assertEquals("property utf8", true, o.supportUTF8Subjects());
        assertNotNull("property opentls", o.getSslContext());
    }

    @Test
    public void testPropertiesStringOptions() throws NoSuchAlgorithmException {
        Properties props = new Properties();
        props.setProperty(Options.PROP_USERNAME, "hello");
        props.setProperty(Options.PROP_PASSWORD, "world");
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property username", "hello", o.getUsername());
        assertEquals("property password", "world", o.getPassword());
        assertEquals("property connection name", "name", o.getConnectionName());
    }

    @Test
    public void testPropertiesSSLOptions() throws Exception {
        // don't use default for tests, issues with forcing algorithm exception in other tests break it
        SSLContext.setDefault(TestSSLUtils.createTestSSLContext());
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "true");

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property context", o.getSslContext());
    }

    @Test
    public void testPropertyIntOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_MAX_RECONNECT, "100");
        props.setProperty(Options.PROP_MAX_PINGS, "200");
        props.setProperty(Options.PROP_RECONNECT_BUF_SIZE, "300");
        props.setProperty(Options.PROP_MAX_CONTROL_LINE, "400");

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property max reconnect", 100, o.getMaxReconnect());
        assertEquals("property ping max", 200, o.getMaxPingsOut());
        assertEquals("property reconnect buffer size", 300, o.getReconnectBufferSize());
        assertEquals("property max control line", 400, o.getMaxControlLine());
    }

    @Test
    public void testDefaultPropertyIntOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "-1");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "-1");
        props.setProperty(Options.PROP_PING_INTERVAL, "-1");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "-1");
        props.setProperty(Options.PROP_MAX_CONTROL_LINE, "-1");

        Options o = new Options.Builder(props).build();
        assertEquals("default max control line", Options.DEFAULT_MAX_CONTROL_LINE, o.getMaxControlLine());
        assertEquals("default reconnect wait", Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait());
        assertEquals("default connection timeout", Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout());
        assertEquals("default ping interval", Options.DEFAULT_PING_INTERVAL, o.getPingInterval());
        assertEquals("default cleanup interval", Options.DEFAULT_REQUEST_CLEANUP_INTERVAL,
                o.getRequestCleanupInterval());
    }

    @Test
    public void testPropertyDurationOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "101");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "202");
        props.setProperty(Options.PROP_PING_INTERVAL, "303");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "404");

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property reconnect wait", Duration.ofMillis(101), o.getReconnectWait());
        assertEquals("property connection timeout", Duration.ofMillis(202), o.getConnectionTimeout());
        assertEquals("property ping interval", Duration.ofMillis(303), o.getPingInterval());
        assertEquals("property cleanup interval", Duration.ofMillis(404), o.getRequestCleanupInterval());
    }

    @Test
    public void testPropertyErrorHandler() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_ERROR_LISTENER, TestHandler.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property error handler", o.getErrorListener());

        o.getErrorListener().errorOccurred(null, "bad subject");
        assertEquals("property error handler class", ((TestHandler) o.getErrorListener()).getCount(), 1);
    }

    @Test
    public void testPropertyConnectionListeners() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_CONNECTION_CB, TestHandler.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property connection handler", o.getConnectionListener());

        o.getConnectionListener().connectionEvent(null, Events.DISCONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.RECONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.CLOSED);

        assertEquals("property connect handler class", ((TestHandler) o.getConnectionListener()).getCount(), 3);
    }

    @Test
    public void testChainOverridesProperties() throws NoSuchAlgorithmException {
        Properties props = new Properties();
        props.setProperty(Options.PROP_TOKEN, "token");
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        Options o = new Options.Builder(props).connectionName("newname").build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property token", "token", o.getToken());
        assertEquals("property connection name", "newname", o.getConnectionName());
    }

    @Test
    public void testDefaultConnectOptions() {
        Options o = new Options.Builder().build();
        String expected = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true}";
        assertEquals("default connect options", expected, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null));
    }

    @Test
    public void testConnectOptionsWithNameAndContext() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        Options o = new Options.Builder().sslContext(ctx).connectionName("c1").build();
        String expected = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\",\"name\":\"c1\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"echo\":true}";
        assertEquals("default connect options", expected, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null));
    }

    @Test
    public void testAuthConnectOptions() {
        Options o = new Options.Builder().userInfo("hello", "world").build();
        String expectedNoAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true}";
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true"
                + ",\"user\":\"hello\",\"pass\":\"world\"}";
        assertEquals("no auth connect options", expectedNoAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null));
        assertEquals("auth connect options", expectedWithAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", true, null));
    }

    @Test
    public void testNKeyConnectOptions() throws Exception {
        TestAuthHandler th = new TestAuthHandler();
        byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String sig = Base64.getUrlEncoder().withoutPadding().encodeToString(th.sign(nonce));

        Options o = new Options.Builder().authHandler(th).build();
        String expectedNoAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true}";
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true"
                + ",\"nkey\":\""+new String(th.getID())+"\",\"sig\":\""+sig+"\",\"jwt\":\"\"}";
        assertEquals("no auth connect options", expectedNoAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, nonce));
        assertEquals("auth connect options", expectedWithAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", true, nonce));
    }

    @Test
    public void testDefaultDataPort() {
        Options o = new Options.Builder().build();
        DataPort dataPort = o.buildDataPort();

        assertNotNull(dataPort);
        assertEquals("default dataPort", Options.DEFAULT_DATA_PORT_TYPE, dataPort.getClass().getCanonicalName());
    }

    @Test
    public void testPropertyDataPortType() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_DATA_PORT_TYPE, CloseOnUpgradeAttempt.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type

        assertEquals("property data port class", CloseOnUpgradeAttempt.class.getCanonicalName(),
                o.buildDataPort().getClass().getCanonicalName());
    }

    @Test
    public void testUserPassInURL() {
        String serverURI = "nats://derek:password@localhost:2222";
        Options o = new Options.Builder().server(serverURI).build();

        String connectString = o.buildProtocolConnectOptionsString(serverURI, true, null);
        assertTrue(connectString.contains("\"user\":\"derek\""));
        assertTrue(connectString.contains("\"pass\":\"password\""));
        assertFalse(connectString.contains("\"token\":"));
    }

    @Test
    public void testTokenInURL() {
        String serverURI = "nats://alberto@localhost:2222";
        Options o = new Options.Builder().server(serverURI).build();

        String connectString = o.buildProtocolConnectOptionsString(serverURI, true, null);
        assertTrue(connectString.contains("\"auth_token\":\"alberto\""));
        assertFalse(connectString.contains("\"user\":"));
        assertFalse(connectString.contains("\"pass\":"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNoProps() throws NoSuchAlgorithmException {
        new Options.Builder(null);
        assertFalse(true);
    }

    @Test
    public void testServerInProperties() {
        Properties props = new Properties();
        String url = "nats://localhost:8080";
        props.setProperty(Options.PROP_URL, url);

        Options o = new Options.Builder(props).build();
        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(1, serverArray.length);
        assertEquals("property server", url, serverArray[0].toString());
    }

    @Test
    public void testServersInProperties() {
        Properties props = new Properties();
        String url1 = "nats://localhost:8080";
        String url2 = "nats://localhost:8081";
        String urls = url1 + ", " + url2;
        props.setProperty(Options.PROP_SERVERS, urls);

        Options o = new Options.Builder(props).build();
        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(2, serverArray.length);
        assertEquals("property server", url1, serverArray[0].toString());
        assertEquals("property server", url2, serverArray[1].toString());
    }

    @Test
    public void testServers() {
        String url1 = "nats://localhost:8080";
        String url2 = "nats://localhost:8081";
        String[] serverUrls = {url1, url2};
        Options o = new Options.Builder().servers(serverUrls).build();

        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(2, serverArray.length);
        assertEquals("property server", url1, serverArray[0].toString());
        assertEquals("property server", url2, serverArray[1].toString());
    }

    @Test
    public void testServersWithCommas() {
        String url1 = "nats://localhost:8080";
        String url2 = "nats://localhost:8081";
        String serverURLs = url1 + "," + url2;
        Options o = new Options.Builder().server(serverURLs).build();

        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(2, serverArray.length);
        assertEquals("property server", url1, serverArray[0].toString());
        assertEquals("property server", url2, serverArray[1].toString());
    }

    @Test
    public void testEmptyStringInServers() {
        String url1 = "nats://localhost:8080";
        String url2 = "";
        String[] serverUrls = {url1, url2};
        Options o = new Options.Builder().servers(serverUrls).build();

        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(1, serverArray.length);
        assertEquals("property server", url1, serverArray[0].toString());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testBadClassInPropertyConnectionListeners() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_CONNECTION_CB, "foo");
        new Options.Builder(props);
        assertFalse(true);
    }

    @Test(expected=IllegalStateException.class)
    public void testTokenAndUserThrows() {
        new Options.Builder().token("foo").userInfo("foo", "bar").build();
        assertFalse(true);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnBadServerURI() {
        new Options.Builder().server("foo:/bar\\:blammer").build();
        assertFalse(true);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptyServersProp() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_SERVERS, "");

        new Options.Builder(props).build();
        assertFalse(true);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnBadServersURI() {
        String url1 = "nats://localhost:8080";
        String url2 = "foo:/bar\\:blammer";
        String[] serverUrls = {url1, url2};
        new Options.Builder().servers(serverUrls).build();
        assertFalse(true);
    }
    
    @Test
    public void testSetExectuor() {
        ExecutorService exec = Executors.newCachedThreadPool();
        Options options = new Options.Builder().executor(exec).build();
        assertEquals(exec, options.getExecutor());
    }
    
    @Test
    public void testDefaultExecutor() throws Exception {
        Options options = new Options.Builder().connectionName("test").build();
        Future<String> future = options.getExecutor().submit(new Callable<String>(){
            public String call() {
                return Thread.currentThread().getName();
            }
        });
        String name = future.get(5, TimeUnit.SECONDS);
        assertTrue(name.startsWith("test"));

        options = new Options.Builder().build();
        future = options.getExecutor().submit(new Callable<String>(){
            public String call() {
                return Thread.currentThread().getName();
            }
        });
        name = future.get(5, TimeUnit.SECONDS);
        assertTrue(name.startsWith(Options.DEFAULT_THREAD_NAME_PREFIX));
    }

    @Test
    public void testParseURIForServer() throws URISyntaxException {
        String[][] test = {
            {"nats://localhost:4222","nats://localhost:4222"},
            {"tls://localhost:4222","tls://localhost:4222"},
            {"opentls://localhost:4222","opentls://localhost:4222"},
            {"localhost:4222","nats://localhost:4222"},

            {"nats://localhost","nats://localhost:4222"},
            {"tls://localhost","tls://localhost:4222"},
            {"opentls://localhost","opentls://localhost:4222"},
            {"localhost","nats://localhost:4222"},

            {"nats://connect.nats.io:4222","nats://connect.nats.io:4222"},
            {"tls://connect.nats.io:4222","tls://connect.nats.io:4222"},
            {"opentls://connect.nats.io:4222","opentls://connect.nats.io:4222"},
            {"connect.nats.io:4222","nats://connect.nats.io:4222"},

            {"nats://connect.nats.io","nats://connect.nats.io:4222"},
            {"tls://connect.nats.io","tls://connect.nats.io:4222"},
            {"opentls://connect.nats.io","opentls://connect.nats.io:4222"},
            {"connect.nats.io","nats://connect.nats.io:4222"},
            
            {"nats://192.168.0.1:4222","nats://192.168.0.1:4222"},
            {"tls://192.168.0.1:4222","tls://192.168.0.1:4222"},
            {"opentls://192.168.0.1:4222","opentls://192.168.0.1:4222"},
            {"192.168.0.1:4222","nats://192.168.0.1:4222"},

            {"nats://192.168.0.1","nats://192.168.0.1:4222"},
            {"tls://192.168.0.1","tls://192.168.0.1:4222"},
            {"opentls://192.168.0.1","opentls://192.168.0.1:4222"},
            {"192.168.0.1","nats://192.168.0.1:4222"},
        };

        for (int i=0 ;i<test.length;i++) {
            URI actual = Options.parseURIForServer(test[i][0]);
            URI expected = new URI(test[i][1]);
            assertEquals(expected.toASCIIString(), actual.toASCIIString());
        }
    }

/* These next three require that no default is set anywhere, if another test
    requires SSLContext.setDefault() and runs before these, they will fail. Commenting
    out for now, this can be run manually.

    @Test(expected=NoSuchAlgorithmException.class)
    public void testThrowOnBadContextForSecure() throws Exception {
        try {
            System.setProperty("javax.net.ssl.keyStore", "foo");
            System.setProperty("javax.net.ssl.trustStore", "bar");
            new Options.Builder().secure().build();
            assertFalse(true);
        }
        finally {
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.trustStore");
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnBadContextForTLSUrl() throws Exception {
        try {
            System.setProperty("javax.net.ssl.keyStore", "foo");
            System.setProperty("javax.net.ssl.trustStore", "bar");
            new Options.Builder().server("tls://localhost:4242").build();
            assertFalse(true);
        }
        finally {
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.trustStore");
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnBadContextSecureProp() {
        try {
            System.setProperty("javax.net.ssl.keyStore", "foo");
            System.setProperty("javax.net.ssl.trustStore", "bar");
            
            Properties props = new Properties();
            props.setProperty(Options.PROP_SECURE, "true");
            new Options.Builder(props).build();
            assertFalse(true);
        }
        finally {
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.trustStore");
        }
    }
    */
}