/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;

import javax.net.SocketFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class TCPConnectionTest {
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

	@Test
	public void testTCPConnection() {
		try (TCPConnection conn = new TCPConnection()) {
			
		}
	}

	@Test(expected=SocketException.class)
	public void testOpen() throws IOException {
		try (TCPConnection conn = new TCPConnection()) {
			Socket sock = mock(Socket.class);
			doThrow(new SocketException("Error setting TCPNoDelay")).when(sock).setTcpNoDelay(false);
			conn.setSocket(sock);
			conn.open();
		} catch (SocketException e) {
			throw(e);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSetConnectTimeout() {
		TCPConnection conn = new TCPConnection();
		conn.setConnectTimeout(41);
		assertEquals(conn.timeout, 41);
	}

	@Test
	public void testIsSetup() {
		TCPConnection conn = new TCPConnection();
		Socket sock = null;
		conn.setSocket(sock);
		assertFalse(conn.isSetup());
		conn.close();

		conn = new TCPConnection();
		sock = mock(Socket.class);
		conn.setSocket(sock);
		assertTrue(conn.isSetup());
		conn.close();
	}

	@Test
	public void testTeardown() {
		TCPConnection conn = new TCPConnection();
		Socket sock = mock(Socket.class);
		conn.setSocket(sock);
		try {
			conn.open();
			conn.teardown();
			assertFalse(conn.isConnected());
		} catch (IOException e) {
			fail("Shouldn't have thrown: " + e.getMessage());
		}
		conn.close();

		conn = new TCPConnection();
		sock = mock(Socket.class);
		try {
			doThrow(new IOException("Error closing socket")).when(sock).close();
			conn.setSocket(sock);
			conn.open();
			conn.teardown();
			assertFalse(conn.isConnected());
		} catch (IOException e) {
			fail("Shouldn't have thrown: " + e.getMessage());
		}
		conn.close();
	}

	//	@Test
	//	public void testGetBufferedInputStreamReader() {
	//	}
	//
	//	@Test
	//	public void testGetReadBufferedStream() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testGetWriteBufferedStream() {
	//		fail("Not yet implemented"); // TODO
	//	}

	@Test
	public void testIsConnected() {
		try (TCPConnection conn = new TCPConnection())
		{
			Socket sock = null;
			conn.setSocket(sock);
			assertFalse(conn.isConnected());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testIsDataAvailableFailure() {
		SocketFactory factory = mock(SocketFactory.class);
		InputStream is = mock(InputStream.class);
		Socket sock = mock(Socket.class);
		try {
			when(factory.createSocket()).thenReturn(sock);
		} catch (IOException e1) {
			fail("Couldn't create mock socket");
		}
		try (NATSServer s = new NATSServer()) {
			try { Thread.sleep(100); } catch (InterruptedException e) {}
			try (TCPConnection conn = new TCPConnection())
			{
				conn.setSocketFactory(factory);
				conn.open("localhost", 4222, 100);
//				assertTrue(conn.isConnected());
				try { Thread.sleep(100); } catch (InterruptedException e) {}

				when(sock.getInputStream()).thenReturn(null);
				assertFalse(conn.isDataAvailable());
				
				when(sock.getInputStream()).thenReturn(is);
				conn.open("localhost", 4222, 100);
				doThrow(new IOException("available() exception")).
					when(is).available();
				boolean exThrown=false;
				try {
					conn.isDataAvailable();
				} catch (IOException e) {
					exThrown=true;
					assertEquals("available() exception", e.getMessage());
				}
				assertTrue("Should have thrown IOException", exThrown);
			} catch (Exception e) {
				fail(e.getMessage());
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}
	@Test
	public void testIsDataAvailable() {
		
		try (NATSServer s = new NATSServer()) {
			try { Thread.sleep(100); } catch (InterruptedException e) {}
			try (TCPConnection conn = new TCPConnection())
			{
				conn.open("localhost", 4222, 100 );
				assertTrue(conn.isConnected());
				try { Thread.sleep(100); } catch (InterruptedException e) {}
				assertTrue(conn.isDataAvailable());
			} catch (Exception e) {
				fail(e.getMessage());
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	//	@Test
	//	public void testSetSocketFactory() {
	//		fail("Not yet implemented"); // TODO
	//	}
	//
	//	@Test
	//	public void testMakeTLS() {
	//		fail("Not yet implemented"); // TODO
	//	}

	@Test
	public void testSetTlsDebug() {
		try (TCPConnection conn = new TCPConnection())
		{
			assertFalse(conn.isTlsDebug());
			conn.setTlsDebug(true);
			assertTrue(conn.isTlsDebug());
			conn.setTlsDebug(false);
			assertFalse(conn.isTlsDebug());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
