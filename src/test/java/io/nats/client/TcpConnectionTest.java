/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.setLogLevel;
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

import ch.qos.logback.classic.Level;
import io.nats.client.TcpConnection.HandshakeListener;

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
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

@Category(UnitTest.class)
public class TcpConnectionTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(TcpConnectionTest.class);

    private static final LogVerifier verifier = new LogVerifier();

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
        new TcpConnection();
    }

    @Test
    public void testGetBufferedInputStream() {
        try (TcpConnection t = new TcpConnection()) {
            t.readStream = readStreamMock;
            assertNotNull(t.getInputStream(16384));
        }
    }

    @Test
    public void testGetBufferedOutputStream() {
        try (TcpConnection t = new TcpConnection()) {
            t.writeStream = writeStreamMock;
            assertNotNull(t.getOutputStream(16384));
        }
    }

    @Test
    public void testGetBufferedReader() {
        try (TcpConnection t = new TcpConnection()) {
            t.readStream = readStreamMock;
            assertNotNull(t.getInputStream(16384));
            assertNotNull(t.getBufferedReader());
        }
    }

    @Test
    public void testSetConnectTimeout() {
        TcpConnection conn = new TcpConnection();
        conn.setConnectTimeout(41);
        assertEquals(conn.timeout, 41);
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
            conn.setTlsDebug(true);
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
    public void testHandshakeListener() throws SSLPeerUnverifiedException {
        try (TcpConnection conn = new TcpConnection()) {
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
        try (TcpConnection conn = new TcpConnection()) {
            Socket sock = null;
            conn.setSocket(sock);
            assertFalse(conn.isConnected());
        } catch (Exception e) {
            fail(e.getMessage());
        }
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
        try (TcpConnection conn = new TcpConnection()) {
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
