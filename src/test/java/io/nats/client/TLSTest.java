package io.nats.client;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Certificate;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.xml.bind.DatatypeConverter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TLSTest {

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
	public void testTlsSuccessWithCert() throws NoSuchAlgorithmException, 
	CertificateException, FileNotFoundException, IOException, 
	KeyStoreException, KeyManagementException, TimeoutException, UnrecoverableKeyException {
		NATSServer srv = util.createServerWithConfig("tls_1222_verify.conf");
		try {Thread.sleep(2000);} catch (InterruptedException e) {}
		ClassLoader classLoader = getClass().getClassLoader();

		final char[] keyPassPhrase = "password".toCharArray();
		final KeyStore ks = KeyStore.getInstance("JKS");
		ks.load(classLoader.getResourceAsStream("keystore.jks"), keyPassPhrase);
		final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
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

		ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
		cf.setSecure(true);
		cf.setSslContext(c);

		Connection connection = cf.createConnection();
		assertFalse(connection.isClosed());
		
		try {Thread.sleep(1000);} catch (InterruptedException e) {}
		connection.publish("foo", "Hello".getBytes());
		connection.close();
		srv.shutdown();

	}
}
