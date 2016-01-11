/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class AuthTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	NATSServer s = null;

	int hitDisconnect;
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


	UnitTestUtilities util = new UnitTestUtilities();

	@Test
	public void testAuth() {
		String noAuthUrl = "nats://localhost:1222";
		String authUrl = "nats://username:password@localhost:1222";

		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
		{
			try(Connection c = new ConnectionFactory(noAuthUrl).createConnection())
			{
				fail("Should have received an error while trying to connect");
			} catch (IOException | TimeoutException e) {
				assertEquals("nats: 'Authorization Violation'", e.getMessage());
			}

			try(Connection c = new ConnectionFactory(authUrl).createConnection())
			{
				assertFalse(c.isClosed());
			} catch (IOException | TimeoutException e) {
				fail("Should have connected successfully, but got: " + e.getMessage());
			}
		} 
	}


	@Test
	public void testAuthFailNoDisconnectCB() {
		String url = "nats://localhost:1222";
		final AtomicInteger receivedDisconnectCB = new AtomicInteger(0);

		ConnectionFactory cf = new ConnectionFactory(url);
		cf.setDisconnectedCallback(new DisconnectedCallback() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				receivedDisconnectCB.incrementAndGet();
			}
		});

		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
		{
			try(Connection c = cf.createConnection())
			{
				fail("Should have received an error while trying to connect");
			} catch (IOException | TimeoutException e) {
				assertEquals("nats: 'Authorization Violation'", e.getMessage());
			}
			assertEquals("Should not have received a disconnect callback on auth failure",
					0, receivedDisconnectCB.get());
		} 
	}

	@Test 
	public void testAuthFailAllowReconnect() {
		String[] servers = {
				"nats://localhost:1221",
				"nats://localhost:1222",
				"nats://localhost:1223",
		};

		UnitTestUtilities utils = new UnitTestUtilities();
		final Channel<Boolean> rcb = new Channel<Boolean>();

		// First server: no auth.
		try	(NATSServer	ts = utils.createServerOnPort(1221))
		{
			// Second server: user/pass auth.
			try	(NATSServer	ts2 = util.createServerWithConfig("auth_1222.conf"))
			{
				// Third server: no auth.
				try	(NATSServer	ts3 = utils.createServerOnPort(1223))
				{
					ConnectionFactory cf = new ConnectionFactory(servers);
					cf.setReconnectAllowed(true);
					cf.setNoRandomize(true);
					cf.setMaxReconnect(10);
					cf.setReconnectWait(100000);
					cf.setReconnectedCallback(new ReconnectedCallback() {
						@Override
						public void onReconnect(ConnectionEvent event) {
							rcb.add(true);
						}
					});
					try(Connection c = cf.createConnection())
					{
						// Stop the server
						ts.shutdown();

						// The client will try to connect to the second server, and that
						// should fail. It should then try to connect to the third and succeed.

						try {
							boolean rcbFired = false;
							rcbFired = rcb.get(5000);
							assertTrue("Reconnect callback should have been triggered", rcbFired);
							assertFalse("Should have reconnected", c.isClosed());
							assertEquals(servers[2], c.getConnectedUrl());
						} catch (TimeoutException e) {
							fail("Reconnect callback should have been triggered");
						}

					} catch (IOException | TimeoutException e) {
						fail("Should have connected ok: " + e.getMessage());
					}
				}
			}
		} 
	}

	@Test
	public void testAuthFailure()
	{
		String[] urls = 
			{ 
					"nats://username@localhost:1222",
					"nats://username:badpass@localhost:1222",
					"nats://localhost:1222", 
					"nats://badname:password@localhost:1222"
			};

		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
		{
			hitDisconnect = 0;
			ConnectionFactory cf = new ConnectionFactory();
			cf.setDisconnectedCallback(new DisconnectedCallback() {
				@Override
				public void onDisconnect(ConnectionEvent event) {
					hitDisconnect++;
				}				
			});

			for (String url : urls)
			{
				boolean exThrown = false;
//				System.err.println("Trying: " + url);
				cf.setUrl(url);
				Thread.sleep(100);
				try (Connection c = cf.createConnection())
				{
					assertTrue(c.isClosed());
					fail("Should not have connected");
				} catch (Exception e) {
					assertTrue(e instanceof ConnectionException);
					assertEquals("nats: 'Authorization Violation'", e.getMessage());
					exThrown = true;
				} finally {
					assertTrue("Should have received an error while trying to connect",
							exThrown);
				}
				Thread.sleep(100);				
			}
		}
		catch (Exception e)
		{
			fail("Unexpected exception thrown: " + e);
		}
		finally
		{
			assertTrue("The disconnect event handler was incorrectly invoked.",
					hitDisconnect == 0);
		}
	}

	@Test
	public void testAuthSuccess() 
			throws IOException, TimeoutException
	{
		String url = ("nats://username:password@localhost:1222");
		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
		{
			try(Connection c = new ConnectionFactory(url)
					.createConnection())
			{
				assertTrue(!c.isClosed());
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
