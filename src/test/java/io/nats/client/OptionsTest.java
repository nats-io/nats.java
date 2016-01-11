/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class OptionsTest {

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

	@Test
	public void testGetSubChanLen() {
		Options o = new Options();
		int len = 47;
		o.setSubChanLen(len);
		assertEquals(len, o.getSubChanLen());
	}

	@Test
	public void testSetSubChanLen() {
		Options o = new Options();
		int len = 128;
		o.setSubChanLen(len);
		assertEquals(len, o.getSubChanLen());
	}

	@Test
	public void testGetUrl() {
		String url = "nats://localhost:1234";
		Options o = new Options();
		assertEquals(null, o.getUrl());
		o.setUrl(url);
		String uriString = o.getUrl().toString();
		assertEquals(url, uriString);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetUrlURI() {
		String urlString = "nats://localhost:1234";
		String badUrlString = "nats://larry:one:two@localhost:5151";

		Options o = new Options();
		URI url = URI.create(urlString);
		o.setUrl(url);
		assertEquals(url, o.getUrl());

		try {
			url = URI.create(badUrlString);
		} catch (Exception e) {
			fail("Shouldn't throw exception here");
		}
		// This should thrown an IllegalArgumentException due to malformed
		// user info in the URL
		o.setUrl(url);
	}

	@Test
	public void testSetUrlString() {
		String urlString = "nats://localhost:1234";
		Options o = new Options();
		assertEquals(null, o.getUrl());

		o.setUrl("");
		assertNull("Should be null", o.getUrl());

		o.setUrl((String)null);
		assertNull("Should be null", o.getUrl());

		o.setUrl(urlString);
		String uriString = o.getUrl().toString();
		assertEquals(urlString, uriString);



		String badUrlString = "tcp://foobar baz";
		boolean exThrown = false;
		try {
			o.setUrl(badUrlString);
		} catch (IllegalArgumentException e) {
			exThrown=true;
		} catch (Exception e) {
			fail(e.getMessage());
		} finally {
			assertTrue("Should have thrown IllegalArgumentException", exThrown);
		}


	}

	@Test
	public void testGetHost() {
		URI uri = URI.create("nats://somehost:5555");
		Options o = new Options();
		o.setUrl(uri);
		assertEquals(uri.getHost(), o.getHost());
	}

	//	@Test
	//	public void testSetHost() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	@Test
	public void testGetPort() {
		URI uri = URI.create("nats://somehost:5555");
		Options o = new Options();
		o.setUrl(uri);
		assertEquals(uri.getPort(), o.getPort());
	}

	//	@Test
	//	public void testSetPort() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	@Test
	public void testGetUsername() {
		URI uri = URI.create("nats://larry:foobar@somehost:5555");
		Options o = new Options();
		o.setUrl(uri);
		String [] userTokens = uri.getUserInfo().split(":");
		String username = userTokens[0];
		assertEquals(username, o.getUsername());
	}

	@Test
	public void testSetUsername() {
		String username = "larry";
		Options o = new Options();
		o.setUsername(username);
		assertEquals(username, o.getUsername());
	}

	@Test
	public void testGetPassword() {
		assertNull(new Options().getPassword());
	}

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
	public void testSetServersStringArray() {
		String[] serverArray = {
				"nats://cluster-host-1:5151",
				"nats://cluster-host-2:5252"
		};
		List<URI> sList = ConnectionFactoryTest.serverArrayToList(serverArray);
		Options o = new Options();
		o.setServers(serverArray);
		assertEquals(sList, o.getServers());

		String[] badServerArray = {
				"nats:// cluster-host-1:5151",
				"nats:// cluster-host-2:5252"
		};
		boolean exThrown = false;
		try {
			o.setServers(badServerArray);
		} catch (IllegalArgumentException e) {
			exThrown=true;
		} finally {
			assertTrue("Should have thrown IllegalArgumentException", exThrown);
		}
	}

	//	@Test
	//	public void testSetServersListOfURI() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
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
		@Test
		public void testIsTlsDebug() {
			Options o = new Options();
			o.setTlsDebug(true);
			assertTrue(o.isTlsDebug());
		}
	
//		@Test
//		public void testSetTlsDebug() {
//			fail("Not yet implemented"); // TODO
//		}
//	
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
	//	public void testGetSslContext() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testSetSslContext() {
	//		fail("Not yet implemented"); // TODO
	//	}

}
