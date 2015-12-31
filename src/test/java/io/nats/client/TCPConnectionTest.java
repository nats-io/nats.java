package io.nats.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

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
		TCPConnection conn = new TCPConnection();
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
			e.printStackTrace();
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

		conn = new TCPConnection();
		sock = mock(Socket.class);
		conn.setSocket(sock);
		assertTrue(conn.isSetup());

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
				e.printStackTrace();
				fail(e.getMessage());
			}
		} catch (Exception e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
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
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
