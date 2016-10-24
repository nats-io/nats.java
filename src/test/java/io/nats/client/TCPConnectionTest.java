/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.nats.client.TCPConnection.HandshakeListener;

import ch.qos.logback.classic.Level;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

@Category(UnitTest.class)
public class TCPConnectionTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(TCPConnectionTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private InputStream readStreamMock;

    @Mock
    private OutputStream writeStreamMock;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @SuppressWarnings("resource")
    @Test
    public void testTcpConnection() {
        new TCPConnection();
    }

    @Test
    public void testGetBufferedInputStream() {
        try (TCPConnection t = new TCPConnection()) {
            t.readStream = readStreamMock;
            assertNotNull(t.getInputStream(16384));
        }
    }

    @Test
    public void testGetBufferedOutputStream() {
        try (TCPConnection t = new TCPConnection()) {
            t.writeStream = writeStreamMock;
            assertNotNull(t.getOutputStream(16384));
        }
    }

    @Test
    public void testGetBufferedReader() {
        try (TCPConnection t = new TCPConnection()) {
            t.readStream = readStreamMock;
            assertNotNull(t.getInputStream(16384));
            assertNotNull(t.getBufferedReader());
        }
    }

    @Test(expected = SocketException.class)
    public void testOpen() throws IOException {
        try (TCPConnection conn = new TCPConnection()) {
            Socket sock = mock(Socket.class);
            doThrow(new SocketException("Error getting OutputStream")).when(sock).getOutputStream();
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
    public void testMakeTls() throws IOException, NoSuchAlgorithmException {
        try (TCPConnection conn = new TCPConnection()) {
            SSLSocketFactory sslSf = mock(SSLSocketFactory.class);
            conn.setSocketFactory(sslSf);
            conn.setTlsDebug(true);
            InetAddress addr = mock(InetAddress.class);
            Socket sock = mock(SSLSocket.class);
            when(sock.getInetAddress()).thenReturn(addr);
            when(addr.getHostAddress()).thenReturn("127.0.0.1");

            when(sslSf.createSocket(any(Socket.class), any(String.class), any(int.class),
                    any(boolean.class))).thenReturn(sock);
            conn.setSocket(sock);
            conn.makeTLS();
        }
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

    @Test
    public void testHandshakeListener() throws SSLPeerUnverifiedException {
        try (TCPConnection conn = new TCPConnection()) {
            final HandshakeListener hcb = conn.new HandshakeListener();
            HandshakeCompletedEvent event = mock(HandshakeCompletedEvent.class);
            Certificate[] certs = new Certificate[2];
            certs[0] = mock(Certificate.class);
            certs[1] = mock(Certificate.class);

            SSLSession session = mock(SSLSession.class);
            when(event.getSession()).thenReturn(session);
            when(session.getPeerHost()).thenReturn("127.0.0.1");
            when(session.getCipherSuite()).thenReturn("TEST_TEST");
            when(session.getPeerCertificates()).thenReturn(certs);

            hcb.handshakeCompleted(event);
        }
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
