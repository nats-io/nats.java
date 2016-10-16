/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_AUTHORIZATION;
import static io.nats.client.Constants.ERR_PROTOCOL;
import static io.nats.client.Constants.ERR_SECURE_CONN_REQUIRED;
import static io.nats.client.Constants.ERR_SECURE_CONN_WANTED;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

// @Category(UnitTest.class)
public class ProtocolTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ProtocolTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    static final String defaultConnect =
            "CONNECT {\"verbose\":false,\"pedantic\":false,\"ssl_required\":false,"
                    + "\"name\":\"\",\"lang\":\"java\",\"version\":\"0.3.0-SNAPSHOT\"}\r\n";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testMockServerIo() {
        try (TCPConnectionMock conn = new TCPConnectionMock()) {
            conn.open("localhost", 2222, 200);
            assertTrue(conn.isConnected());

            OutputStream bw = conn.getBufferedOutputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE);
            assertNotNull(bw);

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    conn.getBufferedInputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE)));
            assertNotNull(br);

            String str = br.readLine().trim();
            assertEquals("INFO strings not equal.", TCPConnectionMock.defaultInfo.trim(), str);

            bw.write(defaultConnect.getBytes());
        } catch (Exception e1) {
            fail(e1.getMessage());
        }
    }

    @Test
    public void testMockServerConnection() {
        try (Connection c = newMockedConnection()) {
            assertTrue(!c.isClosed());

            try (SyncSubscription sub = c.subscribeSync("foo")) {
                c.publish("foo", "Hello".getBytes());
                sub.nextMessage();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        } catch (IOException | TimeoutException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testVerbose() {
        // TODO fix this?
        ConnectionFactory cf = new ConnectionFactory();
        cf.setVerbose(true);
        try (ConnectionImpl c = cf.createConnection(new TCPConnectionFactoryMock())) {
            // try (ConnectionImpl c = cf.createConnection()) {
            assertTrue(!c.isClosed());
            SyncSubscription sub = c.subscribeSync("foo");
            c.flush();
            c.close();
            assertTrue(c.isClosed());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testServerToClientPingPong() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            assertFalse(c.isClosed());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.warn("Interrupted", e);
            }
            TCPConnectionMock mock = (TCPConnectionMock) c.getTcpConnection();
            // TODO mock this with Mockito instead
            mock.sendPing();
        }
    }

    @Test
    public void testServerParseError() {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            assertTrue(!c.isClosed());
            byte[] data = "Hello\r\n".getBytes();
            c.sendProto(data, data.length);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                /* NOOP */ }
            assertTrue(c.isClosed());
        } catch (IOException | TimeoutException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testErrOpConnectionEx() {
        // TODO do this with normal mock
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setSendGenericError(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertEquals("nats: generic error message", e.getMessage());
        }
    }

    @Test
    public void testErrOpAuthorization() {
        // TODO do this with normal mock
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setSendAuthorizationError(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertEquals(ERR_AUTHORIZATION, e.getMessage());
        }
    }

    @Test
    public void testNoInfoSent() {
        // TODO do this with normal mock
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setNoInfo(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertEquals(ERR_PROTOCOL + ", INFO not received", e.getMessage());
        }
    }

    @Test
    public void testTlsMismatchServer() {
        // TODO do this with normal mock
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setTlsRequired(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertNotNull(e.getMessage());
            assertEquals(ERR_SECURE_CONN_REQUIRED, e.getMessage());
        }
    }

    @Test
    public void testTlsMismatchClient() {
        // TODO do this with normal mock
        ConnectionFactory cf = new ConnectionFactory("tls://localhost:4222");
        cf.setSecure(true);
        try (ConnectionImpl c = cf.createConnection(new TCPConnectionFactoryMock())) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertNotNull(e.getMessage());
            assertEquals(ERR_SECURE_CONN_WANTED, e.getMessage());
        }
    }

}
