/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.assertArrayEquals;
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
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

@Category(IntegrationTest.class)
public class ITTLSTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    UnitTestUtilities utils = new UnitTestUtilities();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testTlsSuccessWithCert() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();

        try (NATSServer srv = utils.createServerWithConfig("tls_1222_verify.conf")) {
            UnitTestUtilities.sleep(2000);

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");

            final char[] keyPassPhrase = "password".toCharArray();
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(classLoader.getResourceAsStream("keystore.jks"), keyPassPhrase);
            assertNotNull(kmf);
            kmf.init(ks, keyPassPhrase);

            final char[] trustPassPhrase = "password".toCharArray();
            final KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(classLoader.getResourceAsStream("cacerts"), trustPassPhrase);
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            assertNotNull(tmf);
            tmf.init(tks);

            SSLContext c = SSLContext.getInstance(ConnectionFactory.DEFAULT_SSL_PROTOCOL);
            assertNotNull(c);
            c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            ConnectionFactory cf = new ConnectionFactory();
            cf.setUrl("tls://localhost:1222");
            cf.setSecure(true);
            cf.setTlsDebug(true);
            cf.setSSLContext(c);

            try (Connection connection = cf.createConnection()) {
                assertFalse(connection.isClosed());

                connection.publish("foo", "Hello".getBytes());
                connection.close();
            } finally {
                srv.shutdown();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
            throw (e1);
        }
    }

    @Test
    public void testTlsSuccessSecureConnect() {
        ClassLoader classLoader = getClass().getClassLoader();

        try (NATSServer srv = utils.createServerWithConfig("tls_1222.conf")) {
            UnitTestUtilities.sleep(2000);

            final char[] trustPassPhrase = "password".toCharArray();
            final KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(classLoader.getResourceAsStream("cacerts"), trustPassPhrase);
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            assertNotNull(tmf);
            tmf.init(tks);

            SSLContext context = SSLContext.getInstance(ConnectionFactory.DEFAULT_SSL_PROTOCOL);
            assertNotNull(context);
            context.init(null, tmf.getTrustManagers(), new SecureRandom());

            // we can't call create secure connection w/ the certs setup as they are
            // so we'll override the
            ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
            cf.setSecure(true);
            cf.setTlsDebug(true);
            cf.setSSLContext(context);

            String subj = "foo";
            byte[] omsg = "Hello TLS!".getBytes();

            try (Connection c = cf.createConnection()) {
                UnitTestUtilities.sleep(200);

                try (SyncSubscription s = c.subscribeSync(subj)) {
                    c.publish(subj, omsg);
                    c.flush();
                    Message m = s.nextMessage();
                    assertNotNull(m);
                    assertEquals(subj, m.getSubject());
                    assertArrayEquals(omsg, m.getData());
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            } finally {
                srv.shutdown();
            }
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException
                | KeyManagementException e1) {
            // TODO Auto-generated catch block
            fail(e1.getMessage());
        }
    }

    @Test
    public void testTlsSuccessSecureReconnect() {
        ClassLoader classLoader = getClass().getClassLoader();

        final Channel<Boolean> dch = new Channel<Boolean>();
        final Channel<Boolean> rch = new Channel<Boolean>();

        try (NATSServer srv = utils.createServerWithConfig("tls_1222.conf")) {
            UnitTestUtilities.sleep(2000);

            final char[] trustPassPhrase = "password".toCharArray();
            final KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(classLoader.getResourceAsStream("cacerts"), trustPassPhrase);
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            assertNotNull(tmf);
            tmf.init(tks);

            SSLContext context = SSLContext.getInstance(ConnectionFactory.DEFAULT_SSL_PROTOCOL);
            assertNotNull(context);
            context.init(null, tmf.getTrustManagers(), new SecureRandom());

            // we can't call create secure connection w/ the certs setup as they are
            // so we'll override the
            ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
            cf.setSecure(true);
            cf.setTlsDebug(true);
            cf.setSSLContext(context);
            cf.setReconnectAllowed(true);
            cf.setMaxReconnect(10000);
            cf.setReconnectWait(100);

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dch.add(true);
                }
            });

            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rch.add(true);
                }
            });

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());

                srv.shutdown();

                // We should wait to get the disconnected callback to ensure
                // that we are in the process of reconnecting.
                try {
                    assertTrue("DisconnectedCB should have been triggered.",
                            dch.get(5, TimeUnit.SECONDS));
                    assertEquals("dch should be empty", 0, dch.getCount());
                } catch (TimeoutException e) {
                    fail("DisconnectedCB triggered incorrectly. " + e.getMessage());
                }

                assertTrue("Expected to be in a reconnecting state", c.isReconnecting());

                assertEquals(Constants.ConnState.RECONNECTING, c.getState());

                // Wait until we get the reconnect callback
                try (NATSServer srv2 = utils.createServerWithConfig("tls_1222.conf")) {
                    UnitTestUtilities.sleep(2000);

                    try {
                        assertTrue("ReconnectedCB callback wasn't triggered.",
                                rch.get(3, TimeUnit.SECONDS));
                    } catch (TimeoutException e) {
                        fail("ReconnectedCB callback wasn't triggered. " + e.getMessage());
                    }

                    assertFalse("isReconnecting returned true after the client was reconnected.",
                            c.isReconnecting());

                    assertEquals(Constants.ConnState.CONNECTED, c.getState());

                    // Close the connection, reconnecting should still be false
                    c.close();

                    assertFalse("isReconnecting returned true after close() was called.",
                            c.isReconnecting());

                    assertTrue("Status returned " + c.getState()
                            + " after close() was called instead of CLOSED", c.isClosed());
                }
            } catch (IOException | TimeoutException e) {
                fail("Should have connected OK: " + e.getMessage());
            } finally {
                srv.shutdown();
            }
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException
                | KeyManagementException e1) {
            // TODO Auto-generated catch block
            fail(e1.getMessage());
        }
    }
}
