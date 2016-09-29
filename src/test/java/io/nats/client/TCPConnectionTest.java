/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;

import javax.net.SocketFactory;

@Category(UnitTest.class)
public class TCPConnectionTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testTCPConnection() {
        try (TCPConnection conn = new TCPConnection()) {

        }
    }

    @Test(expected = SocketException.class)
    public void testOpen() throws IOException {
        try (TCPConnection conn = new TCPConnection()) {
            Socket sock = mock(Socket.class);
            doThrow(new SocketException("Error setting TCPNoDelay")).when(sock)
                    .setTcpNoDelay(false);
            conn.setSocket(sock);
            conn.open();
        } catch (SocketException e) {
            throw (e);
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

    // @Test
    // public void testGetBufferedInputStreamReader() {
    // }
    //
    // @Test
    // public void testGetReadBufferedStream() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetWriteBufferedStream() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testIsConnected() {
        try (TCPConnection conn = new TCPConnection()) {
            Socket sock = null;
            conn.setSocket(sock);
            assertFalse(conn.isConnected());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testIsDataAvailableThrowsIoEx() throws IOException {
        thrown.expect(IOException.class);
        thrown.expectMessage("available() exception");

        SocketFactory factory = mock(SocketFactory.class);
        InputStream is = mock(InputStream.class);
        Socket sock = mock(Socket.class);
        try {
            when(factory.createSocket()).thenReturn(sock);
        } catch (IOException e1) {
            fail("Couldn't create mock socket");
        }
        TCPConnection conn = new TCPConnection();
        conn.setSocketFactory(factory);
        when(sock.getInputStream()).thenReturn(is);
        conn.open("localhost", 4222, 100);

        doThrow(new IOException("available() exception")).when(is).available();

        conn.isDataAvailable();
        conn.close();
    }

    @Test
    public void testIsDataAvailable() throws IOException {
        SocketFactory factory = mock(SocketFactory.class);
        Socket client = mock(Socket.class);
        InputStream istream = mock(InputStream.class);

        when(factory.createSocket()).thenReturn(client);

        TCPConnection conn = new TCPConnection();
        conn.setSocketFactory(factory);

        // First, test that null readStream returns false
        conn.open("localhost", 4222, 100);

        assertNull(conn.readStream);
        assertFalse(conn.isDataAvailable());

        // Now test for available > 0
        conn.readStream = istream;
        when(istream.available()).thenReturn(100);
        assertTrue(conn.isDataAvailable());

        // Now test for available == 0
        when(istream.available()).thenReturn(0);
        assertFalse(conn.isDataAvailable());
    }

    // @Test
    // public void testSetSocketFactory() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testMakeTLS() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testSetTlsDebug() {
        try (TCPConnection conn = new TCPConnection()) {
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
