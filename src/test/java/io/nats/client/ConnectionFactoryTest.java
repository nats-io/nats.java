package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static io.nats.client.Constants.*;

public class ConnectionFactoryTest implements ExceptionHandler,
ClosedEventHandler, DisconnectedEventHandler, ReconnectedEventHandler {
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

	@Test(expected=IllegalArgumentException.class)
	public void testConnectionFactoryNullProperties() {
		Properties nullProps = null;
		ConnectionFactory cf = new ConnectionFactory(nullProps);
	}

	final static String url = "nats://foobar:4122";
	final static String hostname = "foobar2";
	final static int port = 2323;
	final static String username = "larry";
	final static String password = "password";
	final static String servers = "nats://cluster-host-1:5151 , nats://cluster-host-2:5252";
	final static String[] serverArray = servers.split(",\\s+");
	static List<URI> sList = null;
	final static Boolean noRandomize = true;
	final static String name = "my_connection";
	final static Boolean verbose = true;
	final static Boolean pedantic = true;
	final static Boolean secure = true;
	final static Boolean reconnectAllowed = false;
	final static int maxReconnect = 14;
	final static int reconnectWait = 100;
	final static int timeout = 2000;
	final static int pingInterval = 5000;
	final static int maxPings = 4;

	@Test
	public void testConnectionFactoryProperties() {
		final ClosedEventHandler ccb = this;
		final DisconnectedEventHandler dcb = this;
		final ReconnectedEventHandler rcb = this;
		final ExceptionHandler eh = this;

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
		props.setProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(reconnectAllowed));
		props.setProperty(PROP_MAX_RECONNECT, Integer.toString(maxReconnect));
		props.setProperty(PROP_RECONNECT_WAIT, Integer.toString(reconnectWait));
		props.setProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(timeout));
		props.setProperty(PROP_PING_INTERVAL, Integer.toString(pingInterval));
		props.setProperty(PROP_MAX_PINGS, Integer.toString(maxPings));
		System.out.println(eh.getClass().getName());
		props.setProperty(PROP_EXCEPTION_HANDLER, 
				eh.getClass().getName());
		props.setProperty(PROP_CLOSED_HANDLER, ccb.getClass().getName());
		props.setProperty(PROP_DISCONNECTED_HANDLER, dcb.getClass().getName());
		props.setProperty(PROP_RECONNECTED_HANDLER, rcb.getClass().getName());


		ConnectionFactory cf = new ConnectionFactory(props);
		assertEquals(hostname, cf.getHost());

		assertEquals(port, cf.getPort());

		assertEquals(username, cf.getUsername());
		assertEquals(password, cf.getPassword());
		List<URI> s1 = this.serverArrayToList(serverArray);
		List<URI> s2 = cf.getServers();
		assertEquals(s1, s2);
		assertEquals(noRandomize, cf.isNoRandomize());
		assertEquals(name, cf.getConnectionName());
		assertEquals(verbose,cf.isVerbose());
		assertEquals(pedantic, cf.isPedantic());
		assertEquals(secure, cf.isSecure());
		assertEquals(reconnectAllowed, cf.isReconnectAllowed());
		assertEquals(maxReconnect, cf.getMaxReconnect());
		assertEquals(reconnectWait, cf.getReconnectWait());
		assertEquals(timeout, cf.getConnectionTimeout());
		assertEquals(pingInterval, cf.getPingInterval());
		assertEquals(maxPings, cf.getMaxPingsOut());
		assertEquals(eh.getClass().getName(), cf.getExceptionHandler().getClass().getName());
		assertEquals(ccb.getClass().getName(), cf.getClosedEventHandler().getClass().getName());
		assertEquals(dcb.getClass().getName(), cf.getDisconnectedEventHandler().getClass().getName());
		assertEquals(rcb.getClass().getName(), cf.getReconnectedEventHandler().getClass().getName());

		cf.setSecure(false);
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (Connection c = cf.createConnection(mock))
			{
				ConnectionImpl ci = (ConnectionImpl)c;
				assertFalse(ci.isClosed());

				assertEquals(hostname, ci.opts.getHost());
				assertEquals(password, ci.opts.getPassword());
				List<URI> s3 = ci.opts.getServers();
				assertEquals(s1, s3);
				assertEquals(noRandomize, ci.opts.isNoRandomize());
				assertEquals(name, ci.opts.getConnectionName());
				assertEquals(verbose,ci.opts.isVerbose());
				assertEquals(pedantic, ci.opts.isPedantic());
				// Setting to default just to ensure we can connect
				assertEquals(false, ci.opts.isSecure());
				assertEquals(reconnectAllowed, ci.opts.isReconnectAllowed());
				assertEquals(maxReconnect, ci.opts.getMaxReconnect());
				assertEquals(reconnectWait, ci.opts.getReconnectWait());
				assertEquals(timeout, ci.opts.getConnectionTimeout());
				assertEquals(pingInterval, ci.opts.getPingInterval());
				assertEquals(maxPings, ci.opts.getMaxPingsOut());
				assertEquals(eh.getClass().getName(), ci.opts.getExceptionHandler().getClass().getName());
				assertEquals(ccb.getClass().getName(), ci.opts.getClosedEventHandler().getClass().getName());
				assertEquals(dcb.getClass().getName(), ci.opts.getDisconnectedEventHandler().getClass().getName());
				assertEquals(rcb.getClass().getName(), ci.opts.getReconnectedEventHandler().getClass().getName());
			} catch (IOException | TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Didn't connect");
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Test
	public void testConnectionFactory() {
		new ConnectionFactory();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testConnectionFactoryString() {
		final String theUrl = "localhost:1234";
		IOException e;
		ConnectionFactory cf = new ConnectionFactory(theUrl);
		assertNotNull(cf);

		assertFalse(theUrl.equals(cf.getUrlString()));

		ConnectionFactory cf2 = new ConnectionFactory("nota:wellformed/uri");
		fail("Should have thrown exception");
	}

	public static List<URI> serverArrayToList(String[] sarray)
	{
		List<URI> rv = new ArrayList<URI>();
		for (String s : serverArray)
		{
			try {
				rv.add(new URI(s.trim()));
			} catch (URISyntaxException e) {
				e.printStackTrace();
				fail("URI input problem.");
			}
		}
		return rv;
	}

	@Test
	public void testConnectionFactoryStringArray() {
		sList = serverArrayToList(serverArray);
		ConnectionFactory cf = new ConnectionFactory(serverArray);
		assertEquals(sList, cf.getServers());
	}

	@Test
	public void testConnectionFactoryStringStringArray() {
		String url = "nats://localhost:1222";
		String[] servers = { "nats://localhost:1234", "nats://localhost:5678" }; 
		List<URI> s1 = new ArrayList<URI>();
		ConnectionFactory cf = new ConnectionFactory(url, servers);
		cf.setServers(servers);
		assertNotNull(cf.getUrlString());
		assertEquals(url, cf.getUrlString());

		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			for (String s : servers) {
				s1.add(new URI(s));
			}
			try (ConnectionImpl c = cf.createConnection(mock))
			{
				List<URI> serverList = c.opts.getServers();
				assertEquals(s1, serverList);
				assertEquals(url, c.opts.getUrl().toString());
			} catch (IOException | TimeoutException e) {
				fail("Couldn't connect");
				e.printStackTrace();
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	//	@Test
	//	public void testCreateConnection() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testCreateConnectionTCPConnection() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetSubChanLen() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetSubChanLen() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testClone() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetUri() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	@Test
	public void testGetUrlString() {
		String urlString = "nats://natshost:2222";
		ConnectionFactory cf = new ConnectionFactory(urlString);
		assertEquals(urlString, cf.getUrlString());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetMalformedUrl() {
		ConnectionFactory cf = new ConnectionFactory("this is a : badly formed url");
		assertNotNull(cf);
	}

	@Test
	public void testGetHost() {
		String url = "nats://foobar:1234";
		ConnectionFactory cf = new ConnectionFactory(url);
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (Connection c = cf.createConnection(mock))
			{
				assertEquals("foobar", cf.getHost());
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
				e.printStackTrace();
			}
		} catch (Exception e1) {
			fail("Exception thrown");
			e1.printStackTrace();
		}
	}

	@Test
	public void testSetHost() {
		String url = "nats://foobar:1234";
		ConnectionFactory cf = new ConnectionFactory(url);
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (Connection c = cf.createConnection(mock))
			{
				assertEquals("foobar", cf.getHost());
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
				e.printStackTrace();
			}
		} catch (Exception e1) {
			fail("Exception thrown");
			e1.printStackTrace();
		}		
	}


	@Test
	public void testGetPort() {
		String url = "nats://foobar:1234";
		ConnectionFactory cf = new ConnectionFactory(url);
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (Connection c = cf.createConnection(mock))
			{
				assertEquals(1234, cf.getPort());
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
				e.printStackTrace();
			}
		} catch (Exception e1) {
			fail("Exception thrown");
			e1.printStackTrace();
		}		
	}

	//	@Test
	//	public void testSetPort() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetUsername() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetUsername() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetPassword() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetPassword() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetServers() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	@Test
	public void testSetServersListOfURI() {
		String[] servers = { "nats://localhost:1234", "nats://localhost:5678" }; 
		List<URI> s1 = new ArrayList<URI>();

		try {
			for (String s : servers) {
				s1.add(new URI(s));
			}
		} catch (URISyntaxException e) {
			fail(e.getMessage());
		}

		ConnectionFactory cf = new ConnectionFactory();
		cf.setServers(s1);
		assertEquals(s1, cf.getServers());
	}

	@Test
	public void testSetServersStringArray() {
		String[] servers = { "nats://localhost:1234", "nats://localhost:5678" }; 
		List<URI> s1 = new ArrayList<URI>();
		ConnectionFactory cf = new ConnectionFactory(servers);
		cf.setServers(servers);

		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			for (String s : servers) {
				s1.add(new URI(s));
			}
			try (ConnectionImpl c = cf.createConnection(mock))
			{
				List<URI> serverList = c.opts.getServers();
				assertEquals(s1, serverList);
			} catch (IOException | TimeoutException e) {
				fail("Couldn't connect");
				e.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	//	@Test
	//	public void testIsNoRandomize() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetNoRandomize() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetConnectionName() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetConnectionName() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testIsVerbose() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetVerbose() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testIsPedantic() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetPedantic() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testIsSecure() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetSecure() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testIsReconnectAllowed() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetReconnectAllowed() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetMaxReconnect() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetMaxReconnect() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetReconnectWait() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetReconnectWait() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetConnectionTimeout() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetConnectionTimeout() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetPingInterval() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetPingInterval() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetMaxPingsOut() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetClosedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetClosedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetDisconnectedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetDisconnectedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetReconnectedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetReconnectedEventHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetMaxPingsOut() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetExceptionHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetExceptionHandler() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetSslContext() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetSslContext() {
	//		fail("Not yet implemented"); // TODO
	//	}


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
	public void onException(Connection conn, Subscription sub, Throwable e) {
		// TODO Auto-generated method stub

	}
}
