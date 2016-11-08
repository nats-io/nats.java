/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Nats.*;
import static io.nats.client.Nats.PROP_RECONNECTED_CB;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Category(UnitTest.class)
public class OptionsTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    static final String url = "nats://foobar:4122";
    static final String hostname = "foobar2";
    static final int port = 2323;
    static final String username = "larry";
    static final String password = "password";
    static final String servers = "nats://cluster-host-1:5151 , nats://cluster-host-2:5252";
    static final String[] serverArray = servers.split(",\\s+");
    static List<URI> sList = null;
    static final Boolean noRandomize = true;
    static final String name = "my_connection";
    static final Boolean verbose = true;
    static final Boolean pedantic = true;
    static final Boolean secure = true;
    static final Boolean reconnectAllowed = false;
    static final int maxReconnect = 14;
    static final int reconnectWait = 100;
    static final int reconnectBufSize = 12 * 1024 * 1024;
    static final int timeout = 2000;
    static final int pingInterval = 5000;
    static final int maxPings = 4;
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
        List<URI> servers = Arrays.asList(URI.create("nats://foobar:1222"), URI.create("nats://localhost:2222"));
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
                .reconnectWait(reconnectWait)
                .timeout(connectionTimeout)
                .pingInterval(pingInterval)
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

    // @Test(expected = IllegalArgumentException.class)
    // public void testSetUrlURI() {
    // String urlString = "nats://localhost:1234";
    // String badUrlString = "nats://larry:one:two@localhost:5151";
    //
    // Options opts = new Options();
    // URI url = URI.create(urlString);
    // opts.setUrl(url);
    // assertEquals(url, opts.getUrl());
    //
    // try {
    // url = URI.create(badUrlString);
    // } catch (Exception e) {
    // fail("Shouldn't throw exception here");
    // }
    // // This should thrown an IllegalArgumentException due to malformed
    // // user info in the URL
    // opts.setUrl(url);
    // }

    // @Test
    // public void testGetUsername() {
    // URI uri = URI.create("nats://larry:foobar@somehost:5555");
    // Options opts = new Options();
    // opts.setUrl(uri);
    // String[] userTokens = uri.getUserInfo().split(":");
    // String username = userTokens[0];
    // assertEquals(username, opts.getUsername());
    // }

    @Test
    public void testUserInfo() {
        String user = "larry";
        String pass = "foo";
        Options opts = new Options.Builder().userInfo(user, pass).build();
        assertEquals(user, opts.getUsername());
        assertEquals(pass, opts.getPassword());
    }

    // @Test
    // public void testGetServers() {
    // fail("Not yet implemented"); // TODO
    // }
    //
//    @Test
//    public void testSetServersStringArray() {
//        String[] serverArray = { "nats://cluster-host-1:5151", "nats://cluster-host-2:5252" };
//        List<URI> sList = ConnectionFactoryTest.serverArrayToList(serverArray);
//        Options opts = new Options.Builder().servers(serverArray).build();
//
//        assertEquals(sList, opts.getServers());
//
//        String[] badServerArray = { "nats:// cluster-host-1:5151", "nats:// cluster-host-2:5252" };
//        boolean exThrown = false;
//        try {
//            opts = new Options.Builder().servers(badServerArray).build();
//        } catch (IllegalArgumentException e) {
//            exThrown = true;
//        } finally {
//            assertTrue("Should have thrown IllegalArgumentException", exThrown);
//        }
//    }

    // @Test
    // public void testSetServersListOfURI() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsNoRandomize() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetNoRandomize() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetConnectionName() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetConnectionName() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsVerbose() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetVerbose() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsPedantic() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetPedantic() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsSecure() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetSecure() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testIsTlsDebug() {
        Options opts = new Options.Builder().tlsDebug().build();
        assertTrue(opts.isTlsDebug());
    }

    // @Test
    // public void testSetTlsDebug() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    //
    // @Test
    // public void testIsReconnectAllowed() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectAllowed() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetMaxReconnect() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetMaxReconnect() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetReconnectWait() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectWait() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetConnectionTimeout() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetConnectionTimeout() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetPingInterval() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetPingInterval() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetMaxPingsOut() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetMaxPingsOut() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetExceptionHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetExceptionHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetClosedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetClosedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetReconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetDisconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetDisconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetSslContext() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetSslContext() {
    // fail("Not yet implemented"); // TODO
    // }

}
