package io.nats.client;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class TLSTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	UnitTestUtilities util = new UnitTestUtilities();

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
	public void testTlsSuccessWithCert() throws Exception {
		try (NATSServer srv = util.createServerWithConfig("tls_1222_verify.conf")) {
			try {Thread.sleep(2000);} catch (InterruptedException e) {}
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

			SSLContext c = SSLContext.getInstance(Constants.DEFAULT_SSL_PROTOCOL);
			assertNotNull(c);
			c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

			ConnectionFactory cf = new ConnectionFactory();
			cf.setUrl("nats://localhost:1222");
			cf.setSecure(true);
			cf.setTlsDebug(true);
			cf.setSslContext(c);

			try (Connection connection = cf.createConnection()) {
				assertFalse(connection.isClosed());
				
				connection.publish("foo", "Hello".getBytes());
				connection.close();
			} finally {
				srv.shutdown();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
			throw(e1);
		}
	}

	@Test
	public void testTlsSuccessSecureConnect() throws Exception
	{
		ClassLoader classLoader = getClass().getClassLoader();

		try (NATSServer srv = util.createServerWithConfig("tls_1222.conf"))
		{
			try {Thread.sleep(2000);} catch (InterruptedException e) {}

			final char[] trustPassPhrase = "password".toCharArray();
			final KeyStore tks = KeyStore.getInstance("JKS");
			tks.load(classLoader.getResourceAsStream("cacerts"), trustPassPhrase);
			final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			assertNotNull(tmf);
			tmf.init(tks);

			SSLContext context = SSLContext.getInstance(Constants.DEFAULT_SSL_PROTOCOL);
			assertNotNull(context);
			context.init(null, tmf.getTrustManagers(), new SecureRandom());

			// we can't call create secure connection w/ the certs setup as they are
			// so we'll override the 
			ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
			cf.setSecure(true);
			cf.setTlsDebug(true);
			cf.setSslContext(context);

			try (Connection c = cf.createConnection())
			{
				try (SyncSubscription s = c.subscribeSync("foo"))
				{
					c.publish("foo", null);
					c.flush();
					Message m = s.nextMessage();
					System.out.println("Received msg over TLS conn.");
				} catch (Exception e) {
					fail(e.getMessage());
				}
			} catch (IOException | TimeoutException e) {
				fail(e.getMessage());
			} finally {
				srv.shutdown();
			}
		} 
	}
}
