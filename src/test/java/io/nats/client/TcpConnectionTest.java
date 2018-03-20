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


import org.junit.Before;
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
public class TcpConnectionTest extends BaseUnitTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private InputStream readStreamMock;

    @Mock
    private OutputStream writeStreamMock;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
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
