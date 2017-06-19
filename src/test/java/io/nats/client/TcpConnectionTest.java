/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

@Category(UnitTest.class)
public class TcpConnectionTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private InputStream readStreamMock;

    @Mock
    private OutputStream writeStreamMock;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
    }

    @SuppressWarnings("resource")
    @Test
    public void testTcpConnection() {
        new TcpConnection();
    }

    @Test
    public void testGetBufferedInputStream() {
        try (TcpConnection t = new TcpConnection()) {
            t.setReadStream(readStreamMock);
            assertNotNull(t.getInputStream(16384));
        }
    }

    @Test
    public void testGetBufferedOutputStream() {
        try (TcpConnection t = new TcpConnection()) {
            t.setWriteStream(writeStreamMock);
            assertNotNull(t.getOutputStream(16384));
        }
    }

    @Test
    public void testGetBufferedReader() {
        try (TcpConnection t = new TcpConnection()) {
            t.setReadStream(readStreamMock);
            assertNotNull(t.getInputStream(16384));
            assertNotNull(t.getBufferedReader());
        }
    }

    @Test
    public void testSetConnectTimeout() {
        TcpConnection conn = new TcpConnection();
        conn.setConnectTimeout(41);
        assertEquals(conn.getTimeout(), 41);
    }

    @Test
    public void testIsSetup() {
        TcpConnection conn = new TcpConnection();
        Socket sock = null;
        conn.setSocket(sock);
        assertFalse(conn.isSetup());
        conn.close();

        conn = new TcpConnection();
        sock = mock(Socket.class);
        conn.setSocket(sock);
        assertTrue(conn.isSetup());
        conn.close();
    }

    @Test
    public void testMakeTls() throws IOException, NoSuchAlgorithmException {
        try (TcpConnection conn = new TcpConnection()) {
            SSLSocketFactory sslSf = mock(SSLSocketFactory.class);
            conn.setSocketFactory(sslSf);
            InetAddress addr = mock(InetAddress.class);
            Socket sock = mock(SSLSocket.class);
            when(sock.getInetAddress()).thenReturn(addr);
            when(addr.getHostAddress()).thenReturn("127.0.0.1");

            when(sslSf.createSocket(any(Socket.class), any(String.class), any(int.class),
                    any(boolean.class))).thenReturn(sock);
            conn.setSocket(sock);
            conn.makeTls();
        }
    }

    @Test
    public void testTeardown() throws Exception {
        TcpConnection conn = new TcpConnection();
        Socket sock = mock(Socket.class);
        SocketFactory socketFactory = mock(SocketFactory.class);
        conn.setSocketFactory(socketFactory);
        doReturn(sock).when(socketFactory).createSocket();
        try {
            conn.open("nats://localhost:42222", 500);
            conn.teardown();
            assertFalse(conn.isConnected());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Shouldn't have thrown: " + e.getMessage());
        }
        conn.close();

        conn = new TcpConnection();
        conn.setSocketFactory(socketFactory);
        try {
            doThrow(new IOException("Error closing socket")).when(sock).close();
            conn.setSocket(sock);
            conn.open("nats://localhost:4222", 2000);
            conn.teardown();
            assertFalse(conn.isConnected());
        } catch (IOException e) {
            fail("Shouldn't have thrown: " + e.getMessage());
        }
        conn.close();
    }

    @Test
    public void testIsConnected() {
        try (TcpConnection conn = new TcpConnection()) {
            Socket sock = null;
            conn.setSocket(sock);
            assertFalse(conn.isConnected());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
