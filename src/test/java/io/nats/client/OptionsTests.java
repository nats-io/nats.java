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
import io.nats.client.impl.*;
import io.nats.client.support.HttpRequest;
import io.nats.client.support.NatsUri;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import io.nats.client.utils.CoverageServerPool;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.nats.client.Options.*;
import static io.nats.client.support.Encoding.base64UrlEncodeToString;
import static io.nats.client.support.NatsConstants.DEFAULT_PORT;
import static org.junit.jupiter.api.Assertions.*;

public class OptionsTests {

    public static final String URL_PROTO_HOST_PORT_8080 = "nats://localhost:8080";
    public static final String URL_PROTO_HOST_PORT_8081 = "nats://localhost:8081";
    public static final String URL_HOST_PORT_8081 = "localhost:8081";

    @Test
    public void testClientVersion() {
        assertTrue(Nats.CLIENT_VERSION.endsWith(".dev"));
    }

    @Test
    public void testDefaultOptions() {
        Options o = new Options.Builder().build();
        _testDefaultOptions(o);
        _testDefaultOptions(new Options.Builder(o).build());
    }

    private static void _testDefaultOptions(Options o) {
        assertEquals(1, o.getServers().size(), "default one server");
        assertEquals(1, o.getUnprocessedServers().size(), "default one server");
        assertEquals(Options.DEFAULT_URL, o.getServers().toArray()[0].toString(), "default url");

        assertEquals(Collections.emptyList(), o.getHttpRequestInterceptors(), "default http request interceptors");
        assertEquals(Options.DEFAULT_DATA_PORT_TYPE, o.getDataPortType(), "default data port type");

        assertFalse(o.isVerbose(), "default verbose");
        assertFalse(o.isPedantic(), "default pedantic");
        assertFalse(o.isNoRandomize(), "default norandomize");
        assertFalse(o.isOldRequestStyle(), "default oldstyle");
        assertFalse(o.isNoEcho(), "default noEcho");
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
        assertEquals(DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, o.getMaxMessagesInOutgoingQueue(),
            "default max messages in outgoing queue");

        assertEquals(Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait(), "default reconnect wait");
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout(), "default connection timeout");
        assertEquals(DEFAULT_PING_INTERVAL, o.getPingInterval(), "default ping interval");
        assertEquals(Options.DEFAULT_REQUEST_CLEANUP_INTERVAL, o.getRequestCleanupInterval(),
            "default cleanup interval");

        assertInstanceOf(ErrorListenerLoggerImpl.class, o.getErrorListener(), "error listener");
        assertNull(o.getConnectionListener(), "disconnect listener");
        assertNull(o.getStatisticsCollector(), "statistics collector");
        assertFalse(o.isOldRequestStyle(), "default oldstyle");
        assertFalse(o.isEnableFastFallback(), "fast fallback");
    }

    @Test
    public void testOldStyle() {
        Options o = new Options.Builder().build();
        assertFalse(o.isOldRequestStyle(), "default oldstyle");
        //noinspection deprecation
        o.setOldRequestStyle(true);
        assertTrue(o.isOldRequestStyle(), "true oldstyle");
        //noinspection deprecation
        o.setOldRequestStyle(false);
        assertFalse(o.isOldRequestStyle(), "false oldstyle");
    }

    @Test
    public void testChainedBooleanOptions() {
        Options o = new Options.Builder().verbose().pedantic().noRandomize()
            .noEcho().oldRequestStyle().noHeaders().noNoResponders()
            .discardMessagesWhenOutgoingQueueFull()
            .build();
        _testChainedBooleanOptions(o);
        _testChainedBooleanOptions(new Options.Builder(o).build());
    }

    private static void _testChainedBooleanOptions(Options o) {
        assertNull(o.getUsernameChars(), "default username");
        assertTrue(o.isVerbose(), "chained verbose");
        assertTrue(o.isPedantic(), "chained pedantic");
        assertTrue(o.isNoRandomize(), "chained norandomize");
        assertTrue(o.isOldRequestStyle(), "chained oldstyle");
        assertTrue(o.isNoEcho(), "chained noecho");
        assertTrue(o.isNoHeaders(), "chained no headers");
        assertTrue(o.isNoNoResponders(), "chained no noResponders");
        assertTrue(o.isDiscardMessagesWhenOutgoingQueueFull(), "chained discard messages when outgoing queue full");
    }

    @Test
    public void testChainedStringOptions() {
        Options o = new Options.Builder().userInfo("hello".toCharArray(), "world".toCharArray()).connectionName("name").build();
        _testChainedStringOptions(o);
        _testChainedStringOptions(new Options.Builder(o).build());
    }

    private static void _testChainedStringOptions(Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertArrayEquals("hello".toCharArray(), o.getUsernameChars(), "chained username");
        assertArrayEquals("world".toCharArray(), o.getPasswordChars(), "chained password");
        assertEquals("name", o.getConnectionName(), "chained connection name");
    }

    @Test
    public void testChainedSecure() throws Exception {
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        SSLContext.setDefault(ctx);
        Options o = new Options.Builder().secure().build();
        _testChainedSecure(ctx, o);
        _testChainedSecure(ctx, new Options.Builder(o).build());
    }

    private static void _testChainedSecure(SSLContext ctx, Options o) {
        assertEquals(ctx, o.getSslContext(), "chained context");
    }

    @Test
    public void testChainedSSLOptions() throws Exception {
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options o = new Options.Builder().sslContext(ctx).build();
        _testChainedSSLOptions(ctx, o);
        _testChainedSSLOptions(ctx, new Options.Builder(o).build());
    }

    private static void _testChainedSSLOptions(SSLContext ctx, Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(ctx, o.getSslContext(), "chained context");
    }

    @Test
    public void testChainedIntOptions() {
        Options o = new Options.Builder().maxReconnects(100).maxPingsOut(200).reconnectBufferSize(300)
            .maxControlLine(400)
            .maxMessagesInOutgoingQueue(500)
            .build();
        _testChainedIntOptions(o);
        _testChainedIntOptions(new Options.Builder(o).build());
    }

    private static void _testChainedIntOptions(Options o) {
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
        _testChainedDurationOptions(o);
        _testChainedDurationOptions(new Options.Builder(o).build());
    }

    private static void _testChainedDurationOptions(Options o) {
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
        ListenerForTesting listener = new ListenerForTesting();
        Options o = new Options.Builder().errorListener(listener).build();
        _testChainedErrorListener(listener, o);
        _testChainedErrorListener(listener, new Options.Builder(o).build());
    }

    private static void _testChainedErrorListener(ListenerForTesting listener, Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(listener, o.getErrorListener(), "chained error listener");
    }

    @Test
    public void testChainedConnectionListener() {
        ConnectionListener cHandler = (c, e) -> System.out.println("connection event" + e);
        Options o = new Options.Builder().connectionListener(cHandler).build();
        _testChainedConnectionListener(cHandler, o);
        _testChainedConnectionListener(cHandler, new Options.Builder(o).build());
    }

    private static void _testChainedConnectionListener(ConnectionListener cHandler, Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertInstanceOf(ErrorListenerLoggerImpl.class, o.getErrorListener(), "error listener");
        assertSame(cHandler, o.getConnectionListener(), "chained connection listener");
    }

    @Test
    public void testChainedStatisticsCollector() {
        StatisticsCollector cHandler = new CoverageStatisticsCollector();
        Options o = new Options.Builder().statisticsCollector(cHandler).build();
        _testChainedStatisticsCollector(cHandler, o);
        _testChainedStatisticsCollector(cHandler, new Options.Builder(o).build());
    }

    private static void _testChainedStatisticsCollector(StatisticsCollector cHandler, Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertInstanceOf(CoverageStatisticsCollector.class, o.getStatisticsCollector(), "statistics collector");
        assertSame(cHandler, o.getStatisticsCollector(), "chained statistics collector");
    }

    @Test
    public void testDurationProperties() {
        // test millis
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "" + (15 * MINUTE));
        props.setProperty(Options.PROP_RECONNECT_JITTER, "" + (2 * DAY + 3 * HOUR + 4 * MINUTE));
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "" + DAY);
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "42000");
        props.setProperty(Options.PROP_SOCKET_WRITE_TIMEOUT, "42123");
        props.setProperty(Options.PROP_PING_INTERVAL, "20345");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "" + (10 * HOUR));
        _testDurationProperties(new Options.Builder(props).build());

        // test duration strings
        props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "PT15M");
        props.setProperty(Options.PROP_RECONNECT_JITTER, "P2DT3H4M");
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "P1D");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "PT42S");
        props.setProperty(Options.PROP_SOCKET_WRITE_TIMEOUT, "PT42.123S");
        props.setProperty(Options.PROP_PING_INTERVAL, "PT20.345S");
        props.setProperty(Options.PROP_CLEANUP_INTERVAL, "PT10H");
        _testDurationProperties(new Options.Builder(props).build());

        // test negative value gives default
        props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "-1");
        Options o = new Options.Builder(props).build();
        assertEquals(2000, o.getReconnectWait().toMillis());

        // test parse error
        Properties px1 = new Properties();
        px1.setProperty(Options.PROP_RECONNECT_WAIT, "A");
        assertThrows(NumberFormatException.class, () -> new Options.Builder(px1).build());
    }

    private static final long MINUTE = 1000 * 60;
    private static final long HOUR = MINUTE * 60;
    private static final long DAY = HOUR * 24;

    private static void _testDurationProperties(Options o) {
        assertEquals(15 * MINUTE, o.getReconnectWait().toMillis());
        assertEquals(2 * DAY + 3 * HOUR + 4 * MINUTE, o.getReconnectJitter().toMillis());
        assertEquals(DAY, o.getReconnectJitterTls().toMillis());
        assertEquals(42000, o.getConnectionTimeout().toMillis());
        assertEquals(42123, o.getSocketWriteTimeout().toMillis());
        assertEquals(20345, o.getPingInterval().toMillis());
        assertEquals(10 * HOUR, o.getRequestCleanupInterval().toMillis());
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
        props.setProperty(Options.PROP_DISCARD_MESSAGES_WHEN_OUTGOING_QUEUE_FULL, "true");

        Options o = new Options.Builder(props).build();
        _testPropertiesBooleanBuilder(o);
        _testPropertiesBooleanBuilder(new Options.Builder(o).build());
    }

    private static void _testPropertiesBooleanBuilder(Options o) {
        assertNull(o.getUsernameChars(), "default username chars");
        assertTrue(o.isVerbose(), "property verbose");
        assertTrue(o.isPedantic(), "property pedantic");
        assertTrue(o.isNoRandomize(), "property norandomize");
        assertTrue(o.isOldRequestStyle(), "property oldstyle");
        assertTrue(o.isNoEcho(), "property noecho");
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
        _testPropertiesStringOptions(o);
        _testPropertiesStringOptions(new Options.Builder(o).build());

        // COVERAGE
        props.setProperty(Options.PROP_CONNECTION_NAME, "");
        new Options.Builder(props).build();

        props.remove(Options.PROP_CONNECTION_NAME);
        new Options.Builder(props).build();
    }

    private static void _testPropertiesStringOptions(Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertArrayEquals("hello".toCharArray(), o.getUsernameChars(), "property username");
        assertArrayEquals("world".toCharArray(), o.getPasswordChars(), "property password");
        assertEquals("name", o.getConnectionName(), "property connection name");
    }

    @Test
    public void testPropertiesSSLOptions() throws Exception {
        // don't use default for tests, issues with forcing algorithm exception in other tests break it
        SSLContext.setDefault(SslTestingHelper.createTestSSLContext());
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "true");

        Options o = new Options.Builder(props).build();
        _testPropertiesSSLOptions(o);
        _testPropertiesSSLOptions(new Options.Builder(o).build());
    }

    private static void _testPropertiesSSLOptions(Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getSslContext(), "property context");
    }

    @Test
    public void testSupportUTF8Subjects() {
        Options o = new Options.Builder().build();
        assertFalse(o.supportUTF8Subjects());

        o = new Options.Builder().supportUTF8Subjects().build();
        assertTrue(o.supportUTF8Subjects());

        Properties props = new Properties();
        props.setProperty(Options.PROP_UTF8_SUBJECTS, "true");
        o = new Options.Builder(props).build();
        assertTrue(o.supportUTF8Subjects());
    }

    @Test
    public void testBuilderCoverageOptions() {
        Options o = new Options.Builder().build();
        assertTrue(o.clientSideLimitChecks());
        assertNull(o.getServerPool()); // there is a default provider

        o = new Options.Builder().clientSideLimitChecks(true).build();
        assertTrue(o.clientSideLimitChecks());
        o = new Options.Builder()
            .clientSideLimitChecks(false)
            .serverPool(new NatsServerPool())
            .build();
        assertFalse(o.clientSideLimitChecks());
        assertNotNull(o.getServerPool());
    }

    @Test
    public void testProperties() throws Exception {
        Properties props = new Properties();

        // stringProperty
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        // stringProperty builds an auth handler
        props.setProperty(Options.PROP_CREDENTIAL_PATH, "src/test/resources/jwt_nkey/test.creds");

        // charArrayProperty
        props.setProperty(Options.PROP_USERNAME, "user");

        // intProperty
        props.setProperty(Options.PROP_MAX_RECONNECT, "10");

        // intGtEqZeroProperty
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "11");

        // longProperty
        props.setProperty(Options.PROP_RECONNECT_BUF_SIZE, "2999999999");

        // durationProperty
        props.setProperty(Options.PROP_PING_INTERVAL, "1000");

        // classnameProperty
        props.setProperty(Options.PROP_SERVERS_POOL_IMPLEMENTATION_CLASS, "io.nats.client.utils.CoverageServerPool");

        Options o = new Options.Builder(props).build();
        _testProperties(o);

        props = new Properties();
        props.load(ResourceUtils.resourceAsInputStream("options_coverage_with_prefix.properties"));
        o = new Options.Builder(props).build();
        _testProperties(o);

        props = new Properties();
        props.load(ResourceUtils.resourceAsInputStream("options_coverage_with_prefix_underscore.properties"));
        o = new Options.Builder(props).build();
        _testProperties(o);

        props = new Properties();
        props.load(ResourceUtils.resourceAsInputStream("options_coverage_without_prefix.properties"));
        o = new Options.Builder(props).build();
        _testProperties(o);

        props = new Properties();
        props.load(ResourceUtils.resourceAsInputStream("options_coverage_without_prefix_underscore.properties"));
        o = new Options.Builder(props).build();
        _testProperties(o);

        String propertiesFilePath = createTempPropertiesFile(props);
        System.out.println(propertiesFilePath);
        o = new Options.Builder(propertiesFilePath).build();
        _testProperties(o);

        // intGtEqZeroProperty not gt zero gives default
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "-1");
        o = new Options.Builder(props).build();
        assertEquals(DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, o.getMaxMessagesInOutgoingQueue());

        // last one wins
        props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "500");
        o = new Options.Builder(props)
            .maxMessagesInOutgoingQueue(1000)
            .build();
        assertEquals(1000, o.getMaxMessagesInOutgoingQueue());

        o = new Options.Builder()
            .maxMessagesInOutgoingQueue(1000)
            .properties(props)
            .build();
        assertEquals(500, o.getMaxMessagesInOutgoingQueue());
    }

    public static String createTempPropertiesFile(Properties props) throws IOException {
        File f = File.createTempFile("jnats", ".properties");
        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        for (String key : props.stringPropertyNames()) {
            writer.write(key + "=" + props.getProperty(key) + System.lineSeparator());
        }
        writer.flush();
        writer.close();
        return f.getAbsolutePath();
    }

    private static void _testProperties(Options o) {
        assertEquals("name", o.getConnectionName());
        assertNotNull(o.getUsernameChars());
        assertEquals("user", new String(o.getUsernameChars()));
        assertEquals(10, o.getMaxReconnect());
        assertEquals(11, o.getMaxMessagesInOutgoingQueue());
        assertEquals(2999999999L, o.getReconnectBufferSize());
        assertEquals(1000, o.getPingInterval().toMillis());
        assertNotNull(o.getAuthHandler());
        assertNotNull(o.getServerPool());
        assertInstanceOf(CoverageServerPool.class, o.getServerPool());
    }

    @Test
    public void testPropertiesCoverageOptions() throws Exception {
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "false");
        props.setProperty(Options.PROP_OPENTLS, "false");
        props.setProperty(Options.PROP_NO_HEADERS, "true");
        props.setProperty(Options.PROP_NO_NORESPONDERS, "true");
        props.setProperty(Options.PROP_RECONNECT_JITTER, "1000");
        props.setProperty(Options.PROP_RECONNECT_JITTER_TLS, "2000");
        props.setProperty(Options.PROP_CLIENT_SIDE_LIMIT_CHECKS, "true"); // deprecated
        props.setProperty(Options.PROP_IGNORE_DISCOVERED_SERVERS, "true");
        props.setProperty(Options.PROP_NO_RESOLVE_HOSTNAMES, "true");
        props.setProperty(PROP_FORCE_FLUSH_ON_REQUEST, "false");

        Options o = new Options.Builder(props).build();
        _testPropertiesCoverageOptions(o);
        _testPropertiesCoverageOptions(new Options.Builder(o).build());
    }

    private static void _testPropertiesCoverageOptions(Options o) {
        assertNull(o.getSslContext());
        assertTrue(o.isNoHeaders());
        assertTrue(o.isNoNoResponders());
        assertTrue(o.clientSideLimitChecks());
        assertTrue(o.isIgnoreDiscoveredServers());
        assertTrue(o.isNoResolveHostnames());
        assertFalse(o.forceFlushOnRequest());
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
        _testPropertyIntOptions(o);
        _testPropertyIntOptions(new Options.Builder(o).build());
    }

    private static void _testPropertyIntOptions(Options o) {
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
        _testDefaultPropertyIntOptions(o);
        _testDefaultPropertyIntOptions(new Options.Builder(o).build());
    }

    private static void _testDefaultPropertyIntOptions(Options o) {
        assertEquals(Options.DEFAULT_MAX_CONTROL_LINE, o.getMaxControlLine(), "default max control line");
        assertEquals(Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait(), "default reconnect wait");
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, o.getConnectionTimeout(), "default connection timeout");
        assertEquals(DEFAULT_PING_INTERVAL, o.getPingInterval(), "default ping interval");
        assertEquals(Options.DEFAULT_REQUEST_CLEANUP_INTERVAL, o.getRequestCleanupInterval(),
            "default cleanup interval");
        assertEquals(DEFAULT_MAX_MESSAGES_IN_OUTGOING_QUEUE, o.getMaxMessagesInOutgoingQueue(),
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
        _testPropertyDurationOptions(o);
        _testPropertyDurationOptions(new Options.Builder(o).build());
    }

    private static void _testPropertyDurationOptions(Options o) {
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertEquals(Duration.ofMillis(101), o.getReconnectWait(), "property reconnect wait");
        assertEquals(Duration.ofMillis(202), o.getConnectionTimeout(), "property connection timeout");
        assertEquals(Duration.ofMillis(303), o.getPingInterval(), "property ping interval");
        assertEquals(Duration.ofMillis(404), o.getRequestCleanupInterval(), "property cleanup interval");
        assertEquals(Duration.ofMillis(505), o.getReconnectJitter(), "property reconnect jitter");
        assertEquals(Duration.ofMillis(606), o.getReconnectJitterTls(), "property reconnect jitter tls");
    }

    @Test
    public void testPropertyErrorListener() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_ERROR_LISTENER, ListenerForTesting.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getErrorListener(), "property error listener");

        o.getErrorListener().errorOccurred(null, "bad subject");
        assertEquals(((ListenerForTesting) o.getErrorListener()).getCount(), 1, "property error listener class");
    }

    @Test
    public void testPropertyConnectionListeners() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_CONNECTION_CB, ListenerForTesting.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getConnectionListener(), "property connection listener");

        o.getConnectionListener().connectionEvent(null, Events.DISCONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.RECONNECTED);
        o.getConnectionListener().connectionEvent(null, Events.CLOSED);

        assertEquals(((ListenerForTesting) o.getConnectionListener()).getCount(), 3, "property connect listener class");
    }

    @Test
    public void testPropertyStatisticsCollector() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_STATISTICS_COLLECTOR, CoverageStatisticsCollector.class.getCanonicalName());

        Options o = new Options.Builder(props).build();
        assertFalse(o.isVerbose(), "default verbose"); // One from a different type
        assertNotNull(o.getStatisticsCollector(), "property statistics collector");

        o.getStatisticsCollector().incrementOutMsgs();
        assertEquals(o.getStatisticsCollector().getOutMsgs(), 1, "property statistics collector class");
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
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
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
        AuthHandlerForTesting th = new AuthHandlerForTesting();
        byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String sig = base64UrlEncodeToString(th.sign(nonce));

        Options o = new Options.Builder().authHandler(th).build();
        String expectedNoAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
            + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true,\"no_responders\":true}";
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
            + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,\"headers\":true"
            + ",\"no_responders\":true,\"nkey\":\""+new String(th.getID())+"\",\"sig\":\""+sig+"\",\"jwt\":\"\"}";
        assertEquals(expectedNoAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", false, nonce).toString(), "no auth connect options");
        assertEquals(expectedWithAuth, o.buildProtocolConnectOptionsString("nats://localhost:4222", true, nonce).toString(), "auth connect options");
    }

    // Test for auth handler from nkey, option JWT and user info
    @Test
    public void testNKeyJWTAndUserInfoOptions() throws Exception {
        // "jwt" is encoded from:
        // Header:    {"alg":"HS256"}
        // Payload:   {"jti":"","iat":2000000000,"iss":"","name":"user_jwt","sub":"","nats":{"pub":{"deny":[">"]},
        //            "sub":{"deny":[">"]},"subs":-1,"data":-1,"payload":-1,"type":"user","version":2}}
        String jwt = "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIiLCJpYXQiOjIwMDAwMDAwMDAsImlzcyI6IiIsIm5hbWUiOiJ1c2VyX2p3"
                + "dCIsInN1YiI6IiIsIm5hdHMiOnsicHViIjp7ImRlbnkiOlsiPiJdfSwic3ViIjp7ImRlbnkiOlsiPiJdfSwic3VicyI6LTEsImRh"
                + "dGEiOi0xLCJwYXlsb2FkIjotMSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfX0";
        NKey nkey = NKey.createUser(null);
        String username = "username";
        String password = "password";
        AuthHandlerForTesting th = new AuthHandlerForTesting(nkey, jwt.toCharArray());
        byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String sig = Base64.getUrlEncoder().withoutPadding().encodeToString(th.sign(nonce));

        // Assert that no auth and user info is given
        Options options = new Options.Builder().authHandler(th)
                .userInfo(username.toCharArray(), password.toCharArray()).build();
        String expectedWithoutAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,"
                + "\"headers\":true,\"no_responders\":true}";
        String actualWithoutAuth = options
                .buildProtocolConnectOptionsString("nats://localhost:4222", false, nonce).toString();
        assertEquals(expectedWithoutAuth, actualWithoutAuth);

        // Assert that auth and user info is given via options
        String expectedWithAuth = "{\"lang\":\"java\",\"version\":\"" + Nats.CLIENT_VERSION + "\""
                + ",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"echo\":true,"
                + "\"headers\":true,\"no_responders\":true,\"nkey\":\"" + new String(th.getID()) + "\",\"sig\":\""
                + sig + "\",\"jwt\":\"" + jwt + "\",\"user\":\"" + username + "\",\"pass\":\"" + password + "\"}";
        String actualWithAuthInOptions = options
                .buildProtocolConnectOptionsString("nats://localhost:4222", true, nonce).toString();
        assertEquals(expectedWithAuth, actualWithAuthInOptions);

        // Assert that auth is given via options and user info is given via server URI
        Options optionsWithoutUserInfo = new Options.Builder().authHandler(th).build();
        String serverUriWithAuth = "nats://" + username + ":" + password + "@localhost:4222";
        String actualWithAuthInServerUri = optionsWithoutUserInfo
                .buildProtocolConnectOptionsString(serverUriWithAuth, true, nonce).toString();
        assertEquals(expectedWithAuth, actualWithAuthInServerUri);
    }


    @Test
    public void testDefaultDataPort() {
        Options o = new Options.Builder().socketWriteTimeout(null).build();
        DataPort dataPort = o.buildDataPort();
        assertNotNull(dataPort);
        assertEquals(Options.DEFAULT_DATA_PORT_TYPE, dataPort.getClass().getCanonicalName(), "old default dataPort");

        o = new Options.Builder().build();
        dataPort = o.buildDataPort();
        assertNotNull(dataPort);
        assertEquals(SocketDataPortWithWriteTimeout.class.getCanonicalName(), dataPort.getClass().getCanonicalName(), "new default dataPort");
    }

    @Test
    public void testTimeoutValidations() {
        assertThrows(IllegalStateException.class, () -> Options.builder()
            .socketReadTimeoutMillis((int)DEFAULT_PING_INTERVAL.toMillis())
            .build());

        assertThrows(IllegalStateException.class, () -> Options.builder()
            .socketWriteTimeout(DEFAULT_CONNECTION_TIMEOUT)
            .build());
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
    public void testTokenSupplier() {
        String serverURI = "nats://localhost:2222";
        Options o = new Options.Builder().build();
        String connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertFalse(connectString.contains("\"auth_token\""));

        //noinspection deprecation
        o = new Options.Builder().token((String)null).build();
        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertFalse(connectString.contains("\"auth_token\""));

        //noinspection deprecation
        o = new Options.Builder().token("   ").build();
        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertFalse(connectString.contains("\"auth_token\""));

        o = new Options.Builder().token((char[])null).build();
        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertFalse(connectString.contains("\"auth_token\""));

        o = new Options.Builder().token(new char[0]).build();
        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertFalse(connectString.contains("\"auth_token\""));

        AtomicInteger counter = new AtomicInteger(0);
        Supplier<char[]> tokenSupplier = () -> ("short-lived-token-" + counter.incrementAndGet()).toCharArray();
        o = new Options.Builder().tokenSupplier(tokenSupplier).build();

        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"auth_token\":\"short-lived-token-1\""));

        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"auth_token\":\"short-lived-token-2\""));

        Properties properties = new Properties();
        properties.setProperty(PROP_TOKEN_SUPPLIER, TestingDynamicTokenSupplier.class.getCanonicalName());
        o = new Options.Builder().properties(properties).build();

        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"auth_token\":\"dynamic-token-1\""));

        connectString = o.buildProtocolConnectOptionsString(serverURI, true, null).toString();
        assertTrue(connectString.contains("\"auth_token\":\"dynamic-token-2\""));
    }

    @Test
    public void testThrowOnNoOpts() {
        assertThrows(IllegalArgumentException.class, () -> new Options.Builder((Options) null));
    }

    @Test
    public void testThrowOnNoProps() {
        assertThrows(IllegalArgumentException.class, () -> new Options.Builder((Properties) null));
    }

    @Test
    public void testServerInProperties() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_URL, URL_PROTO_HOST_PORT_8080);
        assertServersAndUnprocessed(false, new Options.Builder(props).build());
    }

    @Test
    public void testServersInProperties() {
        Properties props = new Properties();
        String urls = URL_PROTO_HOST_PORT_8080 + ", " + URL_HOST_PORT_8081;
        props.setProperty(Options.PROP_SERVERS, urls);
        assertServersAndUnprocessed(true, new Options.Builder(props).build());
    }

    @Test
    public void testServers() {
        String[] serverUrls = {URL_PROTO_HOST_PORT_8080, URL_HOST_PORT_8081};
        Options options = new Options.Builder().servers(serverUrls).build();
        assertServersAndUnprocessed(true, options);
    }

    @Test
    public void testServersWithCommas() {
        String serverURLs = URL_PROTO_HOST_PORT_8080 + "," + URL_HOST_PORT_8081;
        assertServersAndUnprocessed(true, new Options.Builder().server(serverURLs).build());
    }

    @Test
    public void testEmptyAndNullStringsInServers() {
        String[] serverUrls = {"", null, URL_PROTO_HOST_PORT_8080, URL_HOST_PORT_8081};
        assertServersAndUnprocessed(true, new Options.Builder().servers(serverUrls).build());
    }

    private void assertServersAndUnprocessed(boolean two, Options o) {
        Collection<URI> servers = o.getServers();
        URI[] serverArray = servers.toArray(new URI[0]);
        List<String> un = o.getUnprocessedServers();

        int size = two ? 2 : 1;
        assertEquals(size, serverArray.length);
        assertEquals(size, un.size());

        assertEquals(URL_PROTO_HOST_PORT_8080, serverArray[0].toString(), "property server");
        assertEquals(URL_PROTO_HOST_PORT_8080, un.get(0), "unprocessed server");

        if (two) {
            assertEquals(URL_PROTO_HOST_PORT_8081, serverArray[1].toString(), "property server");
            assertEquals(URL_HOST_PORT_8081, un.get(1), "unprocessed server");
            assertEquals(URL_PROTO_HOST_PORT_8080 + "," + URL_PROTO_HOST_PORT_8081, NatsUri.join(",", o.getNatsServerUris())); // coverage
        }
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
    public void testBadClassInPropertyStatisticsCollector() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties props = new Properties();
            props.setProperty(Options.PROP_STATISTICS_COLLECTOR, "foo");
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
    public void testThrowOnBadServersURI() {
        assertThrows(IllegalArgumentException.class, () -> {
            String[] serverUrls = {URL_PROTO_HOST_PORT_8080, "foo:/bar\\:blammer"};
            new Options.Builder().servers(serverUrls).build();
        });
    }

    @Test
    public void testSetExecutor() {
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
    public void testCallbackExecutor() throws ExecutionException, InterruptedException, TimeoutException {
        ThreadFactory threadFactory = r -> new Thread(r, "test");
        Options options = new Options.Builder()
                .callbackThreadFactory(threadFactory)
                .build();
        Future<?> callbackFuture = options.getCallbackExecutor().submit(() -> {
            assertEquals("test", Thread.currentThread().getName());
        });
        callbackFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testConnectExecutor() throws ExecutionException, InterruptedException, TimeoutException {
        ThreadFactory threadFactory = r -> new Thread(r, "test");
        Options options = new Options.Builder()
                .connectThreadFactory(threadFactory)
                .build();
        Future<?> connectFuture = options.getConnectExecutor().submit(() -> {
            assertEquals("test", Thread.currentThread().getName());
        });
        connectFuture.get(5, TimeUnit.SECONDS);
    }

    String[] schemes = new String[]   { "NATS", "unk",  "tls",  "opentls",  "ws",   "wss", "nats"};
    boolean[] secures = new boolean[] { false,  false,  true,   true,       false,  true,  false};
    boolean[] wses = new boolean[]    { false,  false,  false,  false,      true,   true,  false};
    String[] hosts = new String[]     { "host", "1.2.3.4", "[1:2:3:4::5]", null, "nats"};
    boolean[] ips = new boolean[]     { false,  true,      true,           false, false};
    Integer[] ports = new Integer[]   {1122, null};
    String[] userInfos = new String[] {null, "u:p"};

    @Test
    public void testNatsUri() throws URISyntaxException {
        for (int e = 0; e < schemes.length; e++) {
            _testNatsUri(e, null);
            if (e > 1) {
                _testNatsUri(-e, schemes[e]);
            }
        }

        //noinspection DataFlowIssue // NatsUri constructor parameters are annotated as @NonNull
        assertThrows(NullPointerException.class, () -> new NatsUri((String)null));

        // coverage
        //noinspection SimplifiableAssertion,ConstantValue
        assertFalse(new NatsUri(Options.DEFAULT_URL).equals(null));
        //noinspection SimplifiableAssertion
        assertFalse(new NatsUri(Options.DEFAULT_URL).equals(new Object()));
    }

    private void _testNatsUri(int e, String nullScheme) throws URISyntaxException {
        String scheme = e < 0 ? null : schemes[e];
        e = Math.abs(e);
        for (int h = 0; h < hosts.length; h++) {
            String host = hosts[h];
            for (Integer port : ports) {
                for (String userInfo : userInfos) {
                    StringBuilder sb = new StringBuilder();
                    String expectedScheme;
                    if (scheme == null) {
                        expectedScheme = nullScheme;
                    }
                    else {
                        expectedScheme = scheme;
                        sb.append(scheme).append("://");
                    }
                    if (userInfo != null) {
                        sb.append(userInfo).append("@");
                    }
                    if (host != null) {
                        sb.append(host);
                    }
                    int expectedPort;
                    if (port == null) {
                        expectedPort = DEFAULT_PORT;
                    }
                    else {
                        expectedPort = port;
                        sb.append(":").append(port);
                    }
                    if (host == null || "unk".equals(scheme)) {
                        assertThrows(URISyntaxException.class, () -> new NatsUri(sb.toString()));
                    }
                    else {
                        NatsUri uri1 = scheme == null ? new NatsUri(sb.toString(), nullScheme) : new NatsUri(sb.toString());
                        NatsUri uri2 = new NatsUri(uri1.getUri());
                        assertEquals(uri1, uri2);
                        checkCreate(uri1, secures[e], wses[e], ips[h], expectedScheme, host, expectedPort, userInfo);
                        checkCreate(uri2, secures[e], wses[e], ips[h], expectedScheme, host, expectedPort, userInfo);
                    }
                }
            }
        }
    }

    private static void checkCreate(NatsUri uri, boolean secure, boolean ws, boolean ip, String scheme, String host, int port, String userInfo) throws URISyntaxException {
        scheme = scheme.toLowerCase();
        assertEquals(secure, uri.isSecure());
        assertEquals(ws, uri.isWebsocket());
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
        assertEquals(userInfo, uri.getUserInfo());
        String expectedUri = userInfo == null
            ? scheme + "://" + host + ":" + port
            : scheme + "://" + userInfo + "@" + host + ":" + port;
        assertEquals(expectedUri, uri.toString());
        expectedUri = userInfo == null
            ? scheme + "://rehost:" + port
            : scheme + "://" + userInfo + "@rehost:" + port;
        assertEquals(expectedUri, uri.reHost("rehost").toString());
        assertEquals(ip, uri.hostIsIpAddress());
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

    @Test
    public void testSslContextIsProvided() {
        Options o = new Options.Builder().server("nats://localhost").build();
        assertNull(o.getSslContext());
        o = new Options.Builder().server("ws://localhost").build();
        assertNull(o.getSslContext());
        o = new Options.Builder().server("localhost").build();
        assertNull(o.getSslContext());
        o = new Options.Builder().server("tls://localhost").build();
        assertNotNull(o.getSslContext());
        o = new Options.Builder().server("wss://localhost").build();
        assertNotNull(o.getSslContext());
        o = new Options.Builder().server("opentls://localhost").build();
        assertNotNull(o.getSslContext());
        o = new Options.Builder().server("nats://localhost,tls://localhost").build();
        assertNotNull(o.getSslContext());
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

    @Test
    public void testFastFallback() {
        Options options = new Options.Builder().enableFastFallback().build();
        assertTrue(options.isEnableFastFallback());
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
