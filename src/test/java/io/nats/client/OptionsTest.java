/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.PROP_CLOSED_CB;
import static io.nats.client.Nats.PROP_CONNECTION_NAME;
import static io.nats.client.Nats.PROP_CONNECTION_TIMEOUT;
import static io.nats.client.Nats.PROP_DISCONNECTED_CB;
import static io.nats.client.Nats.PROP_EXCEPTION_HANDLER;
import static io.nats.client.Nats.PROP_MAX_PINGS;
import static io.nats.client.Nats.PROP_MAX_RECONNECT;
import static io.nats.client.Nats.PROP_NORANDOMIZE;
import static io.nats.client.Nats.PROP_PASSWORD;
import static io.nats.client.Nats.PROP_PEDANTIC;
import static io.nats.client.Nats.PROP_PING_INTERVAL;
import static io.nats.client.Nats.PROP_RECONNECTED_CB;
import static io.nats.client.Nats.PROP_RECONNECT_ALLOWED;
import static io.nats.client.Nats.PROP_RECONNECT_BUF_SIZE;
import static io.nats.client.Nats.PROP_RECONNECT_WAIT;
import static io.nats.client.Nats.PROP_SECURE;
import static io.nats.client.Nats.PROP_SERVERS;
import static io.nats.client.Nats.PROP_TLS_DEBUG;
import static io.nats.client.Nats.PROP_URL;
import static io.nats.client.Nats.PROP_USERNAME;
import static io.nats.client.Nats.PROP_VERBOSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

@Category(UnitTest.class)
public class OptionsTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

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

    private static final String url = "nats://foobar:4122";
    static final String hostname = "foobar2";
    static final int port = 2323;
    private static final String username = "larry";
    private static final String password = "password";
    private static final String servers = "nats://cluster-host-1:5151 , nats://cluster-host-2:5252";
    private static final String[] serverArray = servers.split(",\\s+");
    static List<URI> sList = null;
    private static final Boolean noRandomize = true;
    private static final String name = "my_connection";
    private static final Boolean verbose = true;
    private static final Boolean pedantic = true;
    private static final Boolean secure = true;
    private static final Boolean reconnectAllowed = false;
    private static final int maxReconnect = 14;
    private static final int reconnectWait = 100;
    private static final int reconnectBufSize = 12 * 1024 * 1024;
    private static final int timeout = 2000;
    private static final int pingInterval = 5000;
    private static final int maxPings = 4;
    static final Boolean tlsDebug = true;

    @Test
    public void testBuilderProperties() throws Exception {

        final ClosedCallback ccb = mock(ClosedCallback.class);
        final DisconnectedCallback dcb = mock(DisconnectedCallback.class);
        final ReconnectedCallback rcb = mock(ReconnectedCallback.class);
        final ExceptionHandler eh = mock(ExceptionHandler.class);

        Properties props = new Properties();
//        props.setProperty(PROP_HOST, hostname);
//        props.setProperty(PROP_PORT, Integer.toString(port));
        props.setProperty(PROP_URL, url);
        props.setProperty(PROP_USERNAME, username);
        props.setProperty(PROP_PASSWORD, password);
        props.setProperty(PROP_SERVERS, servers);
        props.setProperty(PROP_NORANDOMIZE, Boolean.toString(noRandomize));
        props.setProperty(PROP_CONNECTION_NAME, name);
        props.setProperty(PROP_VERBOSE, Boolean.toString(verbose));
        props.setProperty(PROP_PEDANTIC, Boolean.toString(pedantic));
        props.setProperty(PROP_SECURE, Boolean.toString(secure));
        props.setProperty(PROP_TLS_DEBUG, Boolean.toString(secure));
        props.setProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(reconnectAllowed));
        props.setProperty(PROP_MAX_RECONNECT, Integer.toString(maxReconnect));
        props.setProperty(PROP_RECONNECT_WAIT, Integer.toString(reconnectWait));
        props.setProperty(PROP_RECONNECT_BUF_SIZE, Integer.toString(reconnectBufSize));
        props.setProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(timeout));
        props.setProperty(PROP_PING_INTERVAL, Integer.toString(pingInterval));
        props.setProperty(PROP_MAX_PINGS, Integer.toString(maxPings));
        props.setProperty(PROP_EXCEPTION_HANDLER, eh.getClass().getName());
        props.setProperty(PROP_CLOSED_CB, ccb.getClass().getName());
        props.setProperty(PROP_DISCONNECTED_CB, dcb.getClass().getName());
        props.setProperty(PROP_RECONNECTED_CB, rcb.getClass().getName());

        Options opts = new Options.Builder(props).build();

//        assertEquals(hostname, opts.host);
//        assertEquals(port, opts.port);

        assertEquals(username, opts.getUsername());
        assertEquals(password, opts.getPassword());
        List<URI> s1 = ConnectionFactoryTest.serverArrayToList(serverArray);
        List<URI> s2 = opts.getServers();
        assertEquals(s1, s2);
        assertEquals(noRandomize, opts.isNoRandomize());
        assertEquals(name, opts.getConnectionName());
        assertEquals(verbose, opts.isVerbose());
        assertEquals(pedantic, opts.isPedantic());
        assertEquals(secure, opts.isSecure());
        assertEquals(reconnectAllowed, opts.isReconnectAllowed());
        assertEquals(maxReconnect, opts.getMaxReconnect());
        assertEquals(reconnectWait, opts.getReconnectWait());
        assertEquals(reconnectBufSize, opts.getReconnectBufSize());
        assertEquals(timeout, opts.getConnectionTimeout());
        assertEquals(pingInterval, opts.getPingInterval());
        assertEquals(maxPings, opts.getMaxPingsOut());
        assertEquals(eh.getClass().getName(), opts.getExceptionHandler().getClass().getName());
        assertEquals(ccb.getClass().getName(), opts.getClosedCallback().getClass().getName());
        assertEquals(dcb.getClass().getName(), opts.getDisconnectedCallback().getClass().getName());
        assertEquals(rcb.getClass().getName(), opts.getReconnectedCallback().getClass().getName());
    }

    @Test
    public void testBuilderOptions() throws Exception {
        String url = "nats://bumblebee.company.com:4222";
        String username = "derek";
        String password = "mypassword";
        String token = "alkjsdf09234ipoiasfasdf";
        List<URI> servers = Arrays.asList(URI.create("nats://foobar:1222"), URI.create
                ("nats://localhost:2222"));
        boolean noRandomize = true;
        String connectionName = "myConnection";
        boolean verbose = true;
        boolean pedantic = true;
        boolean secure = true;
        boolean allowReconnect = false;
        int maxReconnect = 20;
        int reconnectBufSize = 12345678;
        long reconnectWait = 742;
        int connectionTimeout = 1212;
        long pingInterval = 200000;
        int maxPingsOut = 7;
        SSLContext sslContext = SSLContext.getInstance(Nats.DEFAULT_SSL_PROTOCOL);
        boolean tlsDebug = true;
        TcpConnectionFactory factory = UnitTestUtilities.newMockedTcpConnectionFactory();
        DisconnectedCallback disconnectedCb = mock(DisconnectedCallback.class);
        ClosedCallback closedCb = mock(ClosedCallback.class);
        ReconnectedCallback reconnectedCb = mock(ReconnectedCallback.class);
        ExceptionHandler asyncErrorCb = mock(ExceptionHandler.class);

        String host = "myhost";
        int port = 2122;

        Options expected = new Options.Builder()
                .userInfo(username, password)
                .token(token)
                .dontRandomize()
                .name(connectionName)
                .verbose()
                .pedantic()
                .secure()
                .noReconnect()
                .maxReconnect(maxReconnect)
                .reconnectBufSize(reconnectBufSize)
                .reconnectWait(reconnectWait, TimeUnit.MILLISECONDS)
                .timeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .pingInterval(pingInterval, TimeUnit.MILLISECONDS)
                .maxPingsOut(maxPingsOut)
                .sslContext(sslContext)
                .tlsDebug()
                .factory(factory)
                .disconnectedCb(disconnectedCb)
                .closedCb(closedCb)
                .reconnectedCb(reconnectedCb)
                .errorCb(asyncErrorCb)
                .build();

        expected.url = url;
        expected.servers = servers;

        Options actual = new Options.Builder(expected).build();

        assertTrue(expected.equals(actual));

    }

    @Test
    public void testGetUrl() {
        String url = "nats://localhost:1234";
        Options opts = Nats.defaultOptions();
        assertEquals(null, opts.getUrl());
        opts.url = url;
        assertEquals(url, opts.getUrl());
    }

    @Test
    public void testUserInfo() {
        String user = "larry";
        String pass = "foo";
        Options opts = new Options.Builder().userInfo(user, pass).build();
        assertEquals(user, opts.getUsername());
        assertEquals(pass, opts.getPassword());
    }

    @Test
    public void testIsTlsDebug() {
        Options opts = new Options.Builder().tlsDebug().build();
        assertTrue(opts.isTlsDebug());
    }

    @Test
    public void testHashcode() {
        int hash = new Options.Builder().build().hashCode();
    }
}
