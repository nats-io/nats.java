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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@Category(UnitTest.class)
public class TLSTest {
    final static Logger logger = LoggerFactory.getLogger(TLSTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    UnitTestUtilities util = new UnitTestUtilities();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {}

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testTlsSuccessWithCert() throws Exception {
        try (NATSServer srv = util.createServerWithConfig("tls_1222_verify.conf")) {
            UnitTestUtilities.sleep(2000);
            ClassLoader classLoader = getClass().getClassLoader();

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

        try (NATSServer srv = util.createServerWithConfig("tls_1222.conf")) {
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
    public void testTlsReconnect() {
        ClassLoader classLoader = getClass().getClassLoader();

        try (NATSServer ns1 = util.createServerWithConfig("tls_1222.conf", true)) {

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

            ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
            cf.setMaxReconnect(10);
            cf.setReconnectWait(100);
            cf.setSecure(true);
            cf.setTlsDebug(true);
            cf.setSSLContext(context);

            final Channel<Boolean> ch = new Channel<Boolean>();
            final Channel<Boolean> dch = new Channel<Boolean>();

            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    // dch.add(true);
                    logger.info("rcb triggered");
                }
            });

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dch.add(true);
                    logger.info("dcb triggered");
                }
            });

            String subj = "foo";
            final String omsg = "Hello TLS!";
            byte[] payload = omsg.getBytes();


            try (Connection c = cf.createConnection()) {
                logger.info("Connected");
                UnitTestUtilities.sleep(200);
                AsyncSubscription s = c.subscribeAsync(subj, new MessageHandler() {
                    public void onMessage(Message msg) {
                        String s = new String(msg.getData());
                        if (!s.equals(omsg)) {
                            fail("String doesn't match");
                        }
                        ch.add(true);
                    }
                });

                try {
                    c.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }

                logger.debug("Shutting down ns1");
                ns1.shutdown();
                // server is stopped here...

                logger.debug("Waiting for disconnected callback");
                assertTrue("Did not get the disconnected callback on time",
                        dch.get(5, TimeUnit.SECONDS));

                // UnitTestUtilities.sleep(2000);
                logger.debug("Publishing test message");
                c.publish(subj, payload);

                // restart the server.
                logger.debug("Spinning up ns2");
                try (NATSServer ns2 = util.createServerWithConfig("tls_1222.conf", true)) {
                    UnitTestUtilities.sleep(2000);

                    c.setClosedCallback(null);
                    c.setDisconnectedCallback(null);
                    logger.debug("Flushing connection");
                    try {
                        c.flush(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    assertTrue("Did not receive our message", ch.get(5, TimeUnit.SECONDS));
                    assertEquals("Wrong number of reconnects.", 1, c.getStats().getReconnects());
                } // ns2
            } // Connection
            catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
