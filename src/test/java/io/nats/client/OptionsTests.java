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

import io.nats.client.ConnectionListener.Events;
import io.nats.client.impl.DataPort;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class OptionsTests {
    @Test
    public void testDefaultOptions() {
        Options o = new Options.Builder().build();

        assertEquals(1, o.getServers().size(), "default one server");
        assertEquals(Options.DEFAULT_URL, o.getServers().toArray()[0].toString(), "default url");

        assertEquals(Collections.emptyList(), o.getHttpRequestInterceptors(), "default http request interceptors");
        assertEquals(Options.DEFAULT_DATA_PORT_TYPE, o.getDataPortType(), "default data port type");

        assertFalse(o.isVerbose(), "default verbose");
        assertFalse(o.isPedantic(), "default pedantic");
        assertFalse(o.isNoRandomize(), "default norandomize");
        assertFalse(o.isOldRequestStyle(), "default oldstyle");
        assertFalse(o.isNoEcho(), "default noEcho");
        assertFalse(o.supportUTF8Subjects(), "default UTF8 Support");
        assertFalse(o.isNoHeaders(), "default header support");
        assertFalse(o.isNoNoResponders(), "default no responders support");
        assertEquals(Options.DEFAULT_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL, o.isDiscardMessagesWhenOutgoingQueueFull(),
                "default discard messages when outgoing queue full");

        assertNull(o.getUsernameChars(), "default username");
        assertNull(o.getPasswordChars(), "default password");
        assertNull(o.getTokenChars(), "default token");
        assertNull(o.getConnectionName(), "default connection name");

        assertNull(o.getSslContext(), "default ssl context");

        assertEquals(Options.DEFAULT_MAX_RECONNECT, o.getMaxReconnect(), "default max reconnect");
        assertEquals(Options.DEFAULT_MAX_PINGS_OUT, o.getMaxPingsOut(), "default ping max");
        assertEquals(Options.DEFAULT_RECONNECT_BUF_SIZE, o.getReconnectBufferSize(), "default reconnect buffer size");
        assertEquals(Options.DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, o.getMaxMessagesInOutgoingQueue(),
                "default max messages in outgoing queue");

        assertEquals(Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait(), "default reconnect wait");
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout(), "default connection timeout");
        assertEquals(Options.DEFAULT_PING_INTERVAL, o.getPingInterval(), "default ping interval");
        assertEquals(Options.DEFAULT_REQUEST_CLEANUP_INTERVAL, o.getRequestCleanupInterval(),
                "default cleanup interval");

        assertNull(o.getErrorListener(), "error handler");
        assertNull(o.getConnectionListener(), "disconnect handler");

        // COVERAGE
        o.setOldRequestStyle(true);
        assertTrue(o.isOldRequestStyle(), "default oldstyle");
    }

    @Test
    public void testChainedBooleanOptions() {
        Options o = new Options.Builder().verbose().pedantic().noRandomize().supportUTF8Subjects()
                .noEcho().oldRequestStyle().noHeaders().noNoResponders()
                .discardMessagesWhenOutgoingQueueFull()
                .build();
        assertNull(o.getUsernameChars(), "default username");
        assertTrue(o.isVerbose(), "chained verbose");
        assertTrue(o.isPedantic(), "chained pedantic");
        assertTrue(o.isNoRandomize(), "chained norandomize");
        assertTrue(o.isOldRequestStyle(), "chained oldstyle");
        assertTrue(o.isNoEcho(), "chained noecho");
        assertTrue(o.supportUTF8Subjects(), "chained utf8");
        assertTrue(o.isNoHeaders(), "chained no headers");
        assertTrue(o.isNoNoResponders(), "chained no noResponders");
        assertTrue(o.isDiscardMessagesWhenOutgoingQueueFull(), "chained discard messages when outgoing queue full");
    }

    @Test
    public void testChainedStringOptions() {
        Options o = new Options.Builder().userInfo("hello".toCharArray(), "world".toCharArray()).connectionName("name").build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertArrayEquals("hello".toCharArray(), o.getUsernameChars(), "chained username");
        assertArrayEquals("world".toCharArray(), o.getPasswordChars(), "chained password");
        assertEquals("name", o.getConnectionName(), "chained connection name");
    }

    @Test
    public void testChainedSecure() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        SSLContext.setDefault(ctx);
        Options o = new Options.Builder().secure().build();
        assertEquals(ctx, o.getSslContext(), "chained context");
    }

    @Test
    public void testChainedSSLOptions() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        Options o = new Options.Builder().sslContext(ctx).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(ctx, o.getSslContext(), "chained context");
    }

    @Test
    public void testChainedIntOptions() {
        Options o = new Options.Builder().maxReconnects(100).maxPingsOut(200).reconnectBufferSize(300)
                .maxControlLine(400)
                .maxMessagesInOutgoingQueue(500)
                .build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(100, o.getMaxReconnect(), "chained max reconnect");
        assertEquals(200, o.getMaxPingsOut(), "chained ping max");
        assertEquals(300, o.getReconnectBufferSize(), "chained reconnect buffer size");
        assertEquals(400, o.getMaxControlLine(), "chained max control line");
        assertEquals(500, o.getMaxMessagesInOutgoingQueue(), "chained max messages in outgoing queue");
    }

    @Test
    public void testChainedDurationOptions() {
        Options o = new Options.Builder().reconnectWait(Duration.ofMillis(101))
                .connectionTimeout(Duration.ofMillis(202)).pingInterval(Duration.ofMillis(303))
                .requestCleanupInterval(Duration.ofMillis(404))
                .reconnectJitter(Duration.ofMillis(505))
                .reconnectJitterTls(Duration.ofMillis(606))
                .build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(Duration.ofMillis(101), o.getReconnectWait(), "chained reconnect wait");
        assertEquals(Duration.ofMillis(202), o.getConnectionTimeout(), "chained connection timeout");
        assertEquals(Duration.ofMillis(303), o.getPingInterval(), "chained ping interval");
        assertEquals(Duration.ofMillis(404), o.getRequestCleanupInterval(), "chained cleanup interval");
        assertEquals(Duration.ofMillis(505), o.getReconnectJitter(), "chained reconnect jitter");
        assertEquals(Duration.ofMillis(606), o.getReconnectJitterTls(), "chained cleanup jitter tls");
    }

    @Test
    public void testHttpRequestInterceptors() {
        java.util.function.Consumer<HttpRequest> interceptor1 = req -> {
            req.getHeaders().add("Test1", "Header");
        };
        java.util.function.Consumer<HttpRequest> interceptor2 = req -> {
            req.getHeaders().add("Test2", "Header");
        };
        Options o = new Options.Builder()
            .httpRequestInterceptor(interceptor1)
            .httpRequestInterceptor(interceptor2)
            .build();
        assertEquals(o.getHttpRequestInterceptors(), Arrays.asList(interceptor1, interceptor2));

        o = new Options.Builder()
            .httpRequestInterceptors(Arrays.asList(interceptor2, interceptor1))
            .build();
        assertEquals(o.getHttpRequestInterceptors(), Arrays.asList(interceptor2, interceptor1));
    }

    @Test
    public void testChainedErrorHandler() {
        TestHandler handler = new TestHandler();
        Options o = new Options.Builder().errorListener(handler).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(handler, o.getErrorListener(), "chained error handler");
    }

    @Test
    public void testChainedConnectionListener() {
        ConnectionListener cHandler = (c, e) -> System.out.println("connection event" + e);
        Options o = new Options.Builder().connectionListener(cHandler).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNull(o.getErrorListener(), "error handler");
        assertSame(cHandler, o.getConnectionListener(), "chained connection handler");
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
        props.setProperty(Options.PROP_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL, "true");

        Options o = new Options.Builder(props).build();
        assertNull(o.getUsernameChars(), "default username chars");
        assertTrue(o.isVerbose(), "property verbose");
        assertTrue(o.isPedantic(), "property pedantic");
        assertTrue(o.isNoRandomize(), "property norandomize");
        assertTrue(o.isOldRequestStyle(), "property oldstyle");
        assertTrue(o.isNoEcho(), "property noecho");
        assertTrue(o.supportUTF8Subjects(), "property utf8");
        assertTrue(o.isDiscardMessagesWhenOutgoingQueueFull(), "property discard messages when outgoing queue full");
        assertNotNull(o.getSslContext(), "property opentls");
    }

    @Test
    public void testPropertiesStringOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_USERNAME, "hello");
        props.setProperty(Options.PROP_PASSWORD, "world");
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertArrayEquals("hello".toCharArray(), o.getUsernameChars(), "property username");
        assertArrayEquals("world".toCharArray(), o.getPasswordChars(), "property password");
        assertEquals("name", o.getConnectionName(), "property connection name");

        // COVERAGE
        props.setProperty(Options.PROP_CONNECTION_NAME, "");
        new Options.Builder(props).build();

        props.remove(Options.PROP_CONNECTION_NAME);
        new Options.Builder(props).build();
    }

    @Test
    public void testPropertiesSSLOptions() throws Exception {
        // don't use default for tests, issues with forcing algorithm exception in other tests break it
        SSLContext.setDefault(TestSSLUtils.createTestSSLContext());
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "true");

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getSslContext(), "property context");
    }

    @Test
    public void testPropertiesCoverageOptions() throws Exception {
        // don't use default for tests, issues with forcing algorithm exception in other tests break it
        SSLContext.setDefault(TestSSLUtils.createTestSSLContext());
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "false");
        props.setProperty(Options.PROP_OPENTLS, "false");
        props.setProperty(Options.PROP_NO_HEADERS, "true");
        props.setProperty(Options.PROP_NO_NORESPONDERS, "true");
        props.setProperty(Options.PROP_RECONNECT_JITTER, "1000");
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "2000");

        Options o = new Options.Builder(props).build();
        assertNull(o.getSslContext(), "property context");
        assertTrue(o.isNoHeaders());
        assertTrue(o.isNoNoResponders());
    }

    @Test
    public void testPropertyIntOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_MAX_RECONNECT, "100");
        props.setProperty(Options.PROP_MAX_PINGS, "200");
        props.setProperty(Options.PROP_RECONNECT_BUF_SIZE, "300");
        props.setProperty(Options.PROP_MAX_CONTROL_LINE, "400");
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "500");

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(100, o.getMaxReconnect(), "property max reconnect");
        assertEquals(200, o.getMaxPingsOut(), "property ping max");
        assertEquals(300, o.getReconnectBufferSize(), "property reconnect buffer size");
        assertEquals(400, o.getMaxControlLine(), "property max control line");
        assertEquals(500, o.getMaxMessagesInOutgoingQueue(), "property max messages in outgoing queue");
    }

    @Test
    public void testDefaultPropertyIntOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "-1");
        props.setProperty(Options.PROP_RECONNECT_JITTER, "-1");
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "-1");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "-1");
        props.setProperty(Options.PROP_PING_INTERVAL, "-1");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "-1");
        props.setProperty(Options.PROP_MAX_CONTROL_LINE, "-1");
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "-1");

        Options o = new Options.Builder(props).build();
        assertEquals(Options.DEFAULT_MAX_CONTROL_LINE, o.getMaxControlLine(), "default max control line");
        assertEquals(Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait(), "default reconnect wait");
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout(), "default connection timeout");
        assertEquals(Options.DEFAULT_PING_INTERVAL, o.getPingInterval(), "default ping interval");
        assertEquals(Options.DEFAULT_REQUEST_CLEANUP_INTERVAL, o.getRequestCleanupInterval(),
                "default cleanup interval");
        assertEquals(Options.DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, o.getMaxMessagesInOutgoingQueue(),
                "default max messages in outgoing queue");
    }

    @Test
    public void testPropertyDurationOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "101");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "202");
        props.setProperty(Options.PROP_PING_INTERVAL, "303");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "404");
        props.setProperty(Options.PROP_RECONNECT_JITTER, "505");
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "606");

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(Duration.ofMillis(101), o.getReconnectWait(), "property reconnect wait");
        assertEquals(Duration.ofMillis(202), o.getConnectionTimeout(), "property connection timeout");
        assertEquals(Duration.ofMillis(303), o.getPingInterval(), "property ping interval");
        assertEquals(Duration.ofMillis(404), o.getRequestCleanupInterval(), "property cleanup interval");
        assertEquals(Duration.ofMillis(505), o.getReconnectJitter(), "property reconnect jitter");
        assertEquals(Duration.ofMillis(606), o.getReconnectJitterTls(), "property reconnect jitter tls");
    }

    @Test
    public void testPropertyErrorHandler() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_ERROR_LISTENER, TestHandler.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getErrorListener(), "property error handler");

        o.getErrorListener().errorOccurred(null, "bad subject");
        assertEquals(((TestHandler) o.getErrorListener()).getCount(), 1, "property error handler class");
    }

    @Test
    public void testPropertyConnectionListeners() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_CONNECTION_CB, TestHandler.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getConnectionListener(), "property connection handler");

        o.getConnectionListener().connectionEvent(null, Events.DISCONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.RECONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.CLOSED);

        assertEquals(((TestHandler) o.getConnectionListener()).getCount(), 3, "property connect handler class");
    }

    @Test
    public void testChainOverridesProperties() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_TOKEN, "token");
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        Options o = new Options.Builder(props).connectionName("newname").build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertArrayEquals("token".toCharArray(), o.getTokenChars(), "property token");
        assertEquals("newname", o.getConnectionName(), "property connection name");
    }

    @Test
    public void testDefaultConnectOptions() {
        Options o = new Options.Builder().build();
        String expected = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true,\"no_responders\":true}";
        assertEquals(expected, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null).toString(), "default connect options");
    }

    @Test
    public void testNonDefaultConnectOptions() {
        Options o = new Options.Builder().noNoResponders().noHeaders().noEcho().pedantic().verbose().build();
        String expected = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":true,\"pedantic\":true,\"tls_required\":false,\"echo\":false,\"headers\":false,\"no_responders\":false}";
        assertEquals(expected, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null).toString(), "non default connect options");
    }

    @Test
    public void testConnectOptionsWithNameAndContext() throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        Options o = new Options.Builder().sslContext(ctx).connectionName("c1").build();
        String expected = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\",\"name\":\"c1\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"echo\":true,\"headers\":true,\"no_responders\":true}";
        assertEquals(expected, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null).toString(), "default connect options");
    }

    @Test
    public void testAuthConnectOptions() {
        Options o = new Options.Builder().userInfo("hello".toCharArray(), "world".toCharArray()).build();
        String expectedNoAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true,\"no_responders\":true}";
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true,\"no_responders\":true"
                + ",\"user\":\"hello\",\"pass\":\"world\"}";
        assertEquals(expectedNoAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, null).toString(), "no auth connect options");
        assertEquals(expectedWithAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", true, null).toString(), "auth connect options");
    }
    /*
    expected: <{"lang":"java","version":"2.8.0","protocol":1,"verbose":false,"pedantic":false,"tls_required":false,"echo":true}>
    but was: <{"lang":"java","version":"2.8.0","protocol":1,"verbose":false,"pedantic":false,"tls_required":false,"echo":true,"headers":true}>
     */

    @Test
    public void testNKeyConnectOptions() throws Exception {
        TestAuthHandler th = new TestAuthHandler();
        byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String sig = Base64.getUrlEncoder().withoutPadding().encodeToString(th.sign(nonce));

        Options o = new Options.Builder().authHandler(th).build();
        String expectedNoAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true,\"no_responders\":true}";
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true"
                + ",\"no_responders\":true,\"nkey\":\""+new String(th.getID())+"\",\"sig\":\""+sig+"\",\"jwt\":\"\"}";
        assertEquals(expectedNoAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, nonce).toString(), "no auth connect options");
        assertEquals(expectedWithAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", true, nonce).toString(), "auth connect options");
    }

    @Test
    public void testDefaultDataPort() {
        Options o = new Options.Builder().build();
        DataPort dataPort = o.buildDataPort();

        assertNotNull(dataPort);
        assertEquals(Options.DEFAULT_DATA_PORT_TYPE, dataPort.getClass().getCanonicalName(), "default dataPort");
    }

    @Test
    public void testPropertyDataPortType() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_DATA_PORT_TYPE, CloseOnUpgradeAttempt.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type

        assertEquals(CloseOnUpgradeAttempt.class.getCanonicalName(), o.buildDataPort().getClass().getCanonicalName(),
                "property data port class");
    }

    @Test
    public void testJetStreamProperties() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_INBOX_PREFIX, "custom-inbox-no-dot");
        Options o = new Options.Builder(props).build();
        assertEquals("custom-inbox-no-dot.", o.getInboxPrefix());

        props.setProperty(Options.PROP_INBOX_PREFIX, "custom-inbox-ends-dot.");
        o = new Options.Builder(props).build();
        assertEquals("custom-inbox-ends-dot.", o.getInboxPrefix());
    }

    @Test
    public void testUserPassInURL() {
        String serverURI = "nats://derek:password@localhost:2222";
        Options o = new Options.Builder().server(serverURI).build();

        String connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"user\":\"derek\""));
        assertTrue(connectString.contains("\"pass\":\"password\""));
        assertFalse(connectString.contains("\"token\":"));
    }

    @Test
    public void testTokenInURL() {
        String serverURI = "nats://alberto@localhost:2222";
        Options o = new Options.Builder().server(serverURI).build();

        String connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"auth_token\":\"alberto\""));
        assertFalse(connectString.contains("\"user\":"));
        assertFalse(connectString.contains("\"pass\":"));
    }

    @Test
    public void testThrowOnNoProps() {
        assertThrows(IllegalArgumentException.class, () -> new Options.Builder(null));
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
        assertEquals(url, serverArray[0].toString(), "property server");
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
        assertEquals(url1, serverArray[0].toString(), "property server");
        assertEquals(url2, serverArray[1].toString(), "property server");
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
        assertEquals(url1, serverArray[0].toString(), "property server");
        assertEquals(url2, serverArray[1].toString(), "property server");
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
        assertEquals(url1, serverArray[0].toString(), "property server");
        assertEquals(url2, serverArray[1].toString(), "property server");
    }

    @Test
    public void testEmptyAndNullStringsInServers() {
        String url = "nats://localhost:8080";
        String[] serverUrls = {"", null, url};
        Options o = new Options.Builder().servers(serverUrls).build();

        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        assertEquals(1, serverArray.length);
        assertEquals(url, serverArray[0].toString(), "property server");
    }

    @Test
    public void testBadClassInPropertyConnectionListeners() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties props = new Properties();
            props.setProperty(Options.PROP_CONNECTION_CB, "foo");
            new Options.Builder(props);
        });
    }

    @Test
    public void testTokenAndUserThrows() {
        assertThrows(IllegalStateException.class,
                () -> new Options.Builder().token("foo".toCharArray()).userInfo("foo".toCharArray(), "bar".toCharArray()).build());
    }

    @Test
    public void testThrowOnBadServerURI() {
        assertThrows(IllegalArgumentException.class,
                () -> new Options.Builder().server("foo:/bar\\:blammer").build());
    }

    @Test
    public void testThrowOnEmptyServersProp() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, "");
            new Options.Builder(props).build();
        });
    }

    @Test
    public void testThrowOnBadServersURI() {
        assertThrows(IllegalArgumentException.class, () -> {
            String url1 = "nats://localhost:8080";
            String url2 = "foo:/bar\\:blammer";
            String[] serverUrls = {url1, url2};
            new Options.Builder().servers(serverUrls).build();
        });
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
        Future<String> future = options.getExecutor().submit(() -> Thread.currentThread().getName());
        String name = future.get(5, TimeUnit.SECONDS);
        assertTrue(name.startsWith("test"));

        options = new Options.Builder().build();
        future = options.getExecutor().submit(() -> Thread.currentThread().getName());
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

        for (String[] strings : test) {
            URI actual = Options.parseURIForServer(strings[0], false);
            URI expected = new URI(strings[1]);
            assertEquals(expected.toASCIIString(), actual.toASCIIString());
        }
        URI actual = Options.parseURIForServer("connect.nats.io", true);
        URI expected = new URI("wss://connect.nats.io:4222");
        assertEquals(expected.toASCIIString(), actual.toASCIIString());
}

    @Test
    public void testParseBadURIForServer() {

        String[] strings = new String[] {
                "unk://123.1.1.1", // unknown protocol
                "//123.1.1.1",     // ends up no host
                ":4222"            // just wrong
        };

        for (String string : strings) {
            assertThrows(URISyntaxException.class, () -> Options.parseURIForServer(string, false));
        }
    }

    @Test
    public void testReconnectDelayHandler() {
        ReconnectDelayHandler rdh = l -> Duration.ofSeconds(l * 2);

        Options o = new Options.Builder().reconnectDelayHandler(rdh).build();
        ReconnectDelayHandler rdhO = o.getReconnectDelayHandler();

        assertNotNull(rdhO);
        assertEquals(10, rdhO.getWaitTime(5).getSeconds());
    }

    @Test
    public void testInboxPrefixCoverage() {
        Options o = new Options.Builder().inboxPrefix("foo").build();
        assertEquals("foo.", o.getInboxPrefix());
        o = new Options.Builder().inboxPrefix("foo.").build();
        assertEquals("foo.", o.getInboxPrefix());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void coverageForDeprecated() {
        Options o = new Options.Builder()
                .token("deprecated")
                .build();
        assertEquals("deprecated", o.getToken());
        assertNull(o.getUsername());
        assertNull(o.getPassword());

        o = new Options.Builder()
                .userInfo("user", "pass")
                .build();
        assertEquals("user", o.getUsername());
        assertEquals("pass", o.getPassword());
        assertNull(o.getToken());
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