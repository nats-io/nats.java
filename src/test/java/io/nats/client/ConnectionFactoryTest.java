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

import static io.nats.client.ConnectionImpl.Srv;
import static io.nats.client.Nats.DEFAULT_SSL_PROTOCOL;
import static io.nats.client.Nats.PROP_CLOSED_CB;
import static io.nats.client.Nats.PROP_CONNECTION_NAME;
import static io.nats.client.Nats.PROP_CONNECTION_TIMEOUT;
import static io.nats.client.Nats.PROP_DISCONNECTED_CB;
import static io.nats.client.Nats.PROP_EXCEPTION_HANDLER;
import static io.nats.client.Nats.PROP_HOST;
import static io.nats.client.Nats.PROP_MAX_PINGS;
import static io.nats.client.Nats.PROP_MAX_RECONNECT;
import static io.nats.client.Nats.PROP_NORANDOMIZE;
import static io.nats.client.Nats.PROP_PASSWORD;
import static io.nats.client.Nats.PROP_PEDANTIC;
import static io.nats.client.Nats.PROP_PING_INTERVAL;
import static io.nats.client.Nats.PROP_PORT;
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
import static io.nats.client.Nats.PROP_USE_GLOBAL_MSG_DELIVERY;
import static io.nats.client.UnitTestUtilities.newMockedTcpConnectionFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

@Category(UnitTest.class)
public class ConnectionFactoryTest extends BaseUnitTest
        implements ExceptionHandler, ClosedCallback, DisconnectedCallback, ReconnectedCallback {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void testConnectionFactoryNullProperties() {
        new ConnectionFactory((Properties) null);
    }

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
    static final Boolean useGlobalMsgDelivery = true;

    @Test
    public void testConnectionFactoryProperties() {
        final ClosedCallback ccb = mock(ClosedCallback.class);
        final DisconnectedCallback dcb = mock(DisconnectedCallback.class);
        final ReconnectedCallback rcb = mock(ReconnectedCallback.class);
        final ExceptionHandler eh = mock(ExceptionHandler.class);

        Properties props = new Properties();
        props.setProperty(PROP_HOST, hostname);
        props.setProperty(PROP_PORT, Integer.toString(port));
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
        props.setProperty(PROP_USE_GLOBAL_MSG_DELIVERY, Boolean.toString(useGlobalMsgDelivery));

        ConnectionFactory cf = new ConnectionFactory(props);
        assertEquals(hostname, cf.getHost());
        assertEquals(port, cf.getPort());
        assertEquals(username, cf.getUsername());
        assertEquals(password, cf.getPassword());
        List<URI> s1 = ConnectionFactoryTest.serverArrayToList(serverArray);
        List<URI> s2 = cf.getServers();
        assertEquals(s1, s2);
        assertEquals(noRandomize, cf.isNoRandomize());
        assertEquals(name, cf.getConnectionName());
        assertEquals(verbose, cf.isVerbose());
        assertEquals(pedantic, cf.isPedantic());
        assertEquals(secure, cf.isSecure());
        assertEquals(reconnectAllowed, cf.isReconnectAllowed());
        assertEquals(maxReconnect, cf.getMaxReconnect());
        assertEquals(reconnectWait, cf.getReconnectWait());
        assertEquals(reconnectBufSize, cf.getReconnectBufSize());
        assertEquals(timeout, cf.getConnectionTimeout());
        assertEquals(pingInterval, cf.getPingInterval());
        assertEquals(maxPings, cf.getMaxPingsOut());
        assertEquals(eh.getClass().getName(), cf.getExceptionHandler().getClass().getName());
        assertEquals(ccb.getClass().getName(), cf.getClosedCallback().getClass().getName());
        assertEquals(dcb.getClass().getName(), cf.getDisconnectedCallback().getClass().getName());
        assertEquals(rcb.getClass().getName(), cf.getReconnectedCallback().getClass().getName());
        assertEquals(useGlobalMsgDelivery, cf.isUsingGlobalMessageDelivery());

        ConnectionFactory cf2 = new ConnectionFactory(cf);
        assertTrue(cf2.equals(cf));
    }

    @Test
    public void testConnectionFactoryEmptyServersProp() {
        Properties props = new Properties();
        props.setProperty(PROP_SERVERS, "");
        boolean exThrown = false;
        try {
            new ConnectionFactory(props);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        } finally {
            assertTrue(exThrown);
        }
    }

    @Test
    public void testConnectionFactoryBadCallbackProps() {
        String[] propNames = {PROP_EXCEPTION_HANDLER, PROP_CLOSED_CB, PROP_DISCONNECTED_CB,
                PROP_RECONNECTED_CB};
        for (String p : propNames) {
            Properties props = new Properties();
            props.setProperty(p, "foo.bar.baz");

            boolean exThrown = false;
            try {
                new ConnectionFactory(props);
            } catch (IllegalArgumentException e) {
                exThrown = true;
            } finally {
                assertTrue(exThrown);
            }
        }

    }

    @Test
    public void testConnectionFactory() {
        try {
            new ConnectionFactory();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConnectionFactoryString() {
        final String theUrl = "localhost:1234";
        ConnectionFactory cf = new ConnectionFactory(theUrl);
        assertNotNull(cf);
        assertFalse(theUrl.equals(cf.getUrlString()));
        new ConnectionFactory("nota:wellformed/uri");
    }

    static List<URI> serverArrayToList(String[] sarray) {
        List<URI> rv = new ArrayList<URI>();
        for (String s : serverArray) {
            rv.add(URI.create(s.trim()));
        }
        return rv;
    }

    @Test
    public void testConnectionFactoryStringArray() {
        sList = serverArrayToList(serverArray);
        ConnectionFactory cf = new ConnectionFactory(serverArray);
        assertEquals(sList, cf.getServers());
        assertEquals(null, cf.getUrlString());
    }

    @Test
    public void testConnectionFactoryStringStringArray() throws Exception {
        String url = "nats://localhost:1222";
        String[] servers = {"nats://localhost:1234", "nats://localhost:5678"};
        List<URI> s1 = new ArrayList<URI>(10);
        List<URI> s2 = new ArrayList<URI>(10);
        List<URI> connServerPool = new ArrayList<URI>();
        List<Srv> srvPool = null;
        List<URI> serverList = null;

        final TcpConnectionFactory mcf = newMockedTcpConnectionFactory();

        for (String s : servers) {
            s1.add(URI.create(s));
            s2.add(URI.create(s));
        }
        // test null, servers
        ConnectionFactory cf = new ConnectionFactory(null, servers);
        assertNull(cf.getUrlString());
        assertNotNull(cf.getServers());
        assertEquals(s1, cf.getServers());
        cf.setNoRandomize(true);
        cf.setTcpConnectionFactory(mcf);
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            // test passed-in options
            serverList = conn.getOptions().getServers();
            assertEquals(s1, serverList);
        }

        // TcpConnectionMock mock = (TcpConnectionMock) c.getTcpConnection();
        // mock.bounce();

        // test url, null
        cf = new ConnectionFactory(url, null);
        cf.setNoRandomize(true);
        assertEquals(url, cf.getUrlString());
        // System.err.println("Connecting to: " + url);
        cf.setTcpConnectionFactory(newMockedTcpConnectionFactory());
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            assertEquals(URI.create(url), conn.getUrl());
            // test passed-in options
            assertNull(conn.getOptions().getServers());
        }

        s1.clear();
        s1.addAll(s2);
        assertEquals(s1, s2);

        // mock.bounce();

        // test url, servers
        cf = new ConnectionFactory(url, servers);
        cf.setNoRandomize(true);
        assertNotNull(cf.getUrlString());
        assertEquals(url, cf.getUrlString());
        cf.setTcpConnectionFactory(mcf);
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            // test passed-in options
            serverList = conn.getOptions().getServers();
            assertEquals(s1, serverList);
            assertEquals(url, conn.getOptions().getUrl());
        }
    }

    @Test
    public void testOptions() throws IOException {
        String[] servers = new String[] {"nats://somehost:1010", "nats://somehost2:1020"};
        String host = "localhost";
        int port = 7272;
        URI uri = URI.create(String.format("nats://%s:%d", host, port));
        String urlString = uri.toString();
        String username = "foo";
        String password = "bar";
        boolean dontRandomize = true;
        String name = "BAR";
        boolean verbose = true;
        boolean pedantic = true;
        boolean secure = true;
        boolean reconnectAllowed = false;
        int maxReconnect = 14;
        int reconnectWait = 55000;
        int timeout = 99;
        int pingInterval = 9900;
        int maxPings = 11;
        boolean tlsDebug = true;
        SSLContext sslContext = null;
        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        boolean useGlobalMsgDeliveryPool = true;


        ReconnectedCallback rcb = new ReconnectedCallback() {
            public void onReconnect(ConnectionEvent event) {
            }
        };
        DisconnectedCallback dcb = new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
            }
        };
        ClosedCallback ccb = new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
            }
        };
        ExceptionHandler ecb = new ExceptionHandler() {
            public void onException(NATSException ex) {
            }
        };

        ConnectionFactory cf = new ConnectionFactory();
        cf.setTcpConnectionFactory(tcf);
        cf.setUrl(urlString);
        cf.setUsername(username);
        cf.setPassword(password);
        cf.setHost(host);
        cf.setPort(port);
        cf.setServers(servers);
        cf.setNoRandomize(dontRandomize);
        cf.setConnectionName(name);
        cf.setVerbose(verbose);
        cf.setPedantic(pedantic);
        cf.setSecure(secure);
        cf.setReconnectAllowed(reconnectAllowed);
        cf.setMaxReconnect(maxReconnect);
        cf.setReconnectBufSize(reconnectBufSize);
        cf.setReconnectWait(reconnectWait);
        cf.setConnectionTimeout(timeout);
        cf.setPingInterval(pingInterval);
        cf.setMaxPingsOut(maxPings);
        try {
            sslContext = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);
            cf.setSSLContext(sslContext);
        } catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        }
        cf.setTlsDebug(tlsDebug);

        cf.setReconnectedCallback(rcb);
        cf.setClosedCallback(ccb);
        cf.setDisconnectedCallback(dcb);
        cf.setExceptionHandler(ecb);
        cf.setUseGlobalMessageDelivery(useGlobalMsgDelivery);

        Options opts = cf.options();
        assertEquals(tcf, opts.getFactory());
        assertEquals(urlString, opts.getUrl());
        assertEquals(username, opts.getUsername());
        assertEquals(password, opts.getPassword());
        assertEquals(dontRandomize, opts.isNoRandomize());
        assertEquals(name, opts.getConnectionName());
        assertEquals(verbose, opts.isVerbose());
        assertEquals(pedantic, opts.isPedantic());
        assertEquals(secure, opts.isSecure());
        assertEquals(reconnectAllowed, opts.isReconnectAllowed());
        assertEquals(maxReconnect, opts.getMaxReconnect());
        assertEquals(reconnectBufSize, opts.getReconnectBufSize());
        assertEquals(reconnectWait, opts.getReconnectWait());
        assertEquals(timeout, opts.getConnectionTimeout());
        assertEquals(pingInterval, opts.getPingInterval());
        assertEquals(maxPings, opts.getMaxPingsOut());
        assertEquals(sslContext, opts.getSslContext());
        assertEquals(uri.toString(), cf.getUrlString());
        assertEquals(tlsDebug, opts.isTlsDebug());
        assertEquals(useGlobalMsgDeliveryPool, opts.isUsingGlobalMsgDelivery());

        assertEquals(dcb, opts.getDisconnectedCallback());
        assertEquals(rcb, opts.getReconnectedCallback());
        assertEquals(ccb, opts.getClosedCallback());

        ConnectionImpl conn = new ConnectionImpl(opts);
        assertEquals(opts, conn.getOptions());
        assertTrue(opts.isVerbose());
        assertTrue(conn.getOptions().isVerbose());
    }

    @Test
    public void testCreateConnection() throws Exception {
        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        ConnectionFactory cf = new ConnectionFactory();
        cf.setTcpConnectionFactory(tcf);
        Options opts = cf.options();
        assertEquals(opts.factory, cf.getTcpConnectionFactory());
        try (Connection conn = cf.createConnection()) {
            assertTrue(conn.isConnected());
        }
    }

    @Test
    public void testCopyConstructor() {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setUri(URI.create("nats://localhost:7272"));
        cf.setHost("localhost");
        cf.setPort(7272);
        cf.setServers(new String[] {"nats://somehost:1010", "nats://somehost2:1020"});
        cf.setUsername("foo");
        cf.setPassword("bar");
        cf.setNoRandomize(true);
        cf.setConnectionName("BAR");
        cf.setVerbose(true);
        cf.setPedantic(true);
        cf.setSecure(true);
        cf.setReconnectAllowed(false);
        cf.setMaxReconnect(14);
        cf.setReconnectWait(55000);
        cf.setConnectionTimeout(99);
        cf.setPingInterval(9900);
        cf.setMaxPingsOut(11);
        try {
            cf.setSSLContext(SSLContext.getInstance(DEFAULT_SSL_PROTOCOL));
        } catch (NoSuchAlgorithmException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        cf.setTlsDebug(true);
        cf.setUseGlobalMessageDelivery(true);

        ConnectionFactory cf2 = null;
        cf2 = new ConnectionFactory(cf);
        assertEquals(cf.getUrlString(), cf2.getUrlString());
        assertEquals(cf.getHost(), cf2.getHost());
        assertEquals(cf.getPort(), cf2.getPort());
        assertEquals(cf.getUsername(), cf2.getUsername());
        assertEquals(cf.getPassword(), cf2.getPassword());
        assertEquals(cf.getServers(), cf2.getServers());
        assertEquals(cf.isNoRandomize(), cf2.isNoRandomize());
        assertEquals(cf.getConnectionName(), cf2.getConnectionName());
        assertEquals(cf.isVerbose(), cf2.isVerbose());
        assertEquals(cf.isPedantic(), cf2.isPedantic());
        assertEquals(cf.isSecure(), cf2.isSecure());
        assertEquals(cf.isReconnectAllowed(), cf2.isReconnectAllowed());
        assertEquals(cf.getMaxReconnect(), cf2.getMaxReconnect());
        assertEquals(cf.getReconnectWait(), cf2.getReconnectWait());
        assertEquals(cf.getConnectionTimeout(), cf2.getConnectionTimeout());
        assertEquals(cf.getPingInterval(), cf2.getPingInterval());
        assertEquals(cf.getMaxPingsOut(), cf2.getMaxPingsOut());
        assertEquals(cf.getSSLContext(), cf2.getSSLContext());
        assertEquals(cf.getExceptionHandler(), cf2.getExceptionHandler());
        assertEquals(cf.getClosedCallback(), cf2.getClosedCallback());
        assertEquals(cf.getDisconnectedCallback(), cf2.getDisconnectedCallback());
        assertEquals(cf.getReconnectedCallback(), cf2.getReconnectedCallback());
        assertEquals(cf.getUrlString(), cf2.getUrlString());
        assertEquals(cf.isTlsDebug(), cf2.isTlsDebug());
        assertEquals(cf.isUsingGlobalMessageDelivery(), cf2.isUsingGlobalMessageDelivery());
    }

    @Test
    public void testSetUriBadScheme() {
        URI uri = URI.create("file:///etc/fstab");

        ConnectionFactory cf = new ConnectionFactory();
        boolean exThrown = false;
        try {
            cf.setUri(uri);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }
        assertTrue("Should have thrown exception.", exThrown);
        exThrown = false;

        uri = URI.create("nats:///etc/fstab");
        cf.setUri(uri);
        assertNull("Host should be null.", cf.getHost());
        assertEquals(-1, cf.getPort());

        uri = URI.create("tcp://localhost:4222");
        try {
            cf.setUri(uri);
        } catch (Exception e) {
            fail("Should not have thrown exception.");
        }
    }

    @Test
    public void testSetUriWithToken() {
        String secret = "$2a$11$3kIDaCxw.Glsl1.u5nKa6eUnNDLV5HV9tIuUp7EHhMt6Nm9myW1aS";
        String urlString = String.format("nats://%s@natshost:2222", secret);

        try {
            new ConnectionFactory(urlString);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        URI goodUri = URI.create(urlString);
        // System.err.println(goodUri.toString());
        assertEquals(secret, goodUri.getRawUserInfo());
        ConnectionFactory cf = new ConnectionFactory();
        try {
            cf.setUri(goodUri);
            assertEquals(secret, cf.getUsername());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetUriBadUserInfo() {
        final String urlString = "nats://foo:bar:baz@natshost:2222";
        final String urlString2 = "nats://foo:@natshost:2222";
        final String urlString3 = "nats://:pass@natshost:2222";
        ConnectionFactory cf = new ConnectionFactory();

        boolean exThrown = false;
        URI uri = URI.create(urlString);
        try {
            cf.setUri(uri);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }
        assertTrue(exThrown);

        cf.setUsername(null);
        cf.setPassword(null);

        exThrown = false;
        uri = URI.create(urlString2);
        try {
            cf.setUri(uri);
            assertEquals("foo", cf.getUsername());
            assertNull(cf.getPassword());
        } catch (IllegalArgumentException e) {
            exThrown = true;
            fail(e.getMessage());
        }
        assertFalse(exThrown);

        cf.setUsername(null);
        cf.setPassword(null);

        exThrown = false;
        uri = URI.create(urlString3);
        try {
            cf.setUri(uri);
            assertNull(cf.getUsername());
            assertNull(cf.getPassword());
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }
        assertFalse(exThrown);
    }

    @Test
    public void testGetUrlString() {
        String urlString = "nats://natshost:2222";
        ConnectionFactory cf = new ConnectionFactory(urlString);
        assertEquals(urlString, cf.getUrlString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMalformedUrl() {
        ConnectionFactory cf = new ConnectionFactory("this is a : badly formed url");
        assertNotNull(cf);
    }

    @Test
    public void testGetHost() {
        String url = "nats://foobar:1234";
        ConnectionFactory cf = new ConnectionFactory(url);
        assertEquals("foobar", cf.getHost());
    }

    @Test
    public void testSetHost() {
        String url = "nats://foobar:1234";
        ConnectionFactory cf = new ConnectionFactory(url);
        cf.setHost("anotherhost");
        assertEquals("anotherhost", cf.getHost());
    }


    @Test
    public void testGetPort() {
        String url = "nats://foobar:1234";
        ConnectionFactory cf = new ConnectionFactory(url);
        assertEquals(1234, cf.getPort());
    }

    // @Test
    // public void testSetPort() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetUsername() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetUsername() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetPassword() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetPassword() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetServers() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testSetServersListOfUri() {
        String[] servers = {"nats://localhost:1234", "nats://localhost:5678"};
        List<URI> s1 = new ArrayList<URI>();

        for (String s : servers) {
            s1.add(URI.create(s));
        }

        ConnectionFactory cf = new ConnectionFactory();
        cf.setServers(s1);
        assertEquals(s1, cf.getServers());
    }

    @Test
    public void testSetServersStringArray() throws Exception {
        String[] servers = {"nats://localhost:1234", "nats://localhost:5678"};
        List<URI> s1 = new ArrayList<URI>();
        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();

        // Initial setup
        ConnectionFactory cf = new ConnectionFactory(servers);
        cf.setServers(servers);
        cf.setNoRandomize(true);
        cf.setTcpConnectionFactory(mcf);

        assertNull(cf.getUrlString());

        for (String s : servers) {
            s1.add(URI.create(s));
        }
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            List<URI> serverList = c.getOptions().getServers();
            assertEquals(s1, serverList);
        }

        try {
            new ConnectionFactory((String[]) null);
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        }

        try {
            ConnectionFactory cf2 = new ConnectionFactory();
            cf2.setServers((String[]) null);
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        }

        String[] badServers = {"foo bar", "bar"};
        boolean exThrown = false;
        try {
            cf = new ConnectionFactory(badServers);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        } finally {
            assertTrue(exThrown);
        }
    }

    @Test
    public void testConnectionFactoryCommaDelimitedString() {
        String servers = "nats://localhost:1234, nats://localhost:5678";
        List<URI> s1 = new ArrayList<URI>();
        ConnectionFactory cf = new ConnectionFactory(servers);
        cf.setNoRandomize(true);

        for (String s : servers.trim().split(",")) {
            s1.add(URI.create(s.trim()));
        }

        assertNull(cf.getUrlString());
        assertEquals(s1, cf.getServers());


    }

    @Test
    public void testSetUrl() {
        String servers = "nats://localhost:1234, nats://localhost:5678";
        List<URI> s1 = new ArrayList<URI>();
        ConnectionFactory cf = new ConnectionFactory();
        cf.setNoRandomize(true);

        cf.setUrl(servers);

        for (String s : servers.trim().split(",")) {
            s1.add(URI.create(s.trim()));
        }

        assertNull(cf.getUrlString());
        assertEquals(s1, cf.getServers());

    }

    @Test
    public void testSetAndGetTcpConnectionFactory() throws Exception {
        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        ConnectionFactory cf = new ConnectionFactory();
        cf.setTcpConnectionFactory(tcf);
        assertEquals(tcf, cf.getTcpConnectionFactory());
    }

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

    @Test
    public void testIsTlsDebug() throws Exception {
        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();
        ConnectionFactory cf = new ConnectionFactory();
        cf.setTlsDebug(true);
        cf.setTcpConnectionFactory(mcf);
        assertTrue(cf.isTlsDebug());
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            assertTrue(c.getOptions().isTlsDebug());
        }
    }

    @Test
    public void testSetExceptionHandler() {
        ConnectionFactory cf = new ConnectionFactory();
        boolean exThrown = false;
        try {
            cf.setExceptionHandler(null);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        } finally {
            assertTrue(exThrown);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetSslContext() {
        ConnectionFactory cf = new ConnectionFactory();
        try {
            SSLContext ctx = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);
            cf.setSSLContext(ctx);
            assertEquals(ctx, cf.getSSLContext());
            assertEquals(cf.getSSLContext(), cf.getSslContext());
            cf.setSslContext(ctx);
            assertEquals(ctx, cf.getSSLContext());
            assertEquals(cf.getSSLContext(), cf.getSslContext());
        } catch (NoSuchAlgorithmException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetSslContext() {
        ConnectionFactory cf = new ConnectionFactory();
        try {
            SSLContext ctx = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);
            cf.setSSLContext(ctx);
            assertEquals(ctx, cf.getSSLContext());
        } catch (NoSuchAlgorithmException e) {
            fail(e.getMessage());
        }
    }

    @Override
    public void onDisconnect(ConnectionEvent event) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onReconnect(ConnectionEvent event) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onClose(ConnectionEvent event) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onException(NATSException e) {
        // TODO Auto-generated method stub

    }

    @Test
    public void testConstructUri() {
        final String host = "foobar";
        final int port = 7272;
        final String user = "derek";
        final String pass = "derek";
        final String expectedUrl =
                String.format("nats://%s:%d", host, Nats.DEFAULT_PORT);
        final String expectedUrl1 = String.format("nats://%s:%d", host, port);
        final String expectedUrl2 = String.format("nats://%s@%s:%d", user, host, port);
        final String expectedUrl3 = String.format("nats://%s:%s@%s:%d", user, pass, host, port);

        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost(host);
        assertEquals(host, cf.getHost());
        assertEquals(-1, cf.getPort());
        assertEquals(expectedUrl, cf.constructUri().toString());

        cf.setPort(port);
        assertEquals(port, cf.getPort());
        assertNull(cf.getUrlString());
        assertEquals(expectedUrl1, cf.constructUri().toString());

        assertNull(cf.getUsername());
        assertNull(cf.getPassword());
        cf.setUsername(user);
        assertEquals(user, cf.getUsername());
        assertEquals(expectedUrl2, cf.constructUri().toString());

        cf.setPassword(pass);
        assertEquals(pass, cf.getPassword());
        assertEquals(expectedUrl3, cf.constructUri().toString());

    }

    @Test
    public void testSetReconnectBufSizeNegative() {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setReconnectBufSize(-14);
        assertEquals(Nats.DEFAULT_RECONNECT_BUF_SIZE, cf.getReconnectBufSize());
    }
}
