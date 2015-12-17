package io.nats.client;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;

import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Convenience class representing the TCP connection to prevent 
/// managing two variables throughout the NATs client code.
class TCPConnection {
	final Logger logger = LoggerFactory.getLogger(TCPConnection.class);

	/// TODO: Test various scenarios for efficiency. Is a
	/// BufferedReader directly over a network stream really
	/// more efficient for NATS?
	///
	ReentrantLock mu = new ReentrantLock();
	private SocketFactory factory = SocketFactory.getDefault();
	private SSLContext sslContext;
	Socket client = null;
	protected OutputStream writeStream = null;
	protected InputStream readStream = null;
	protected InetSocketAddress addr = null;
	protected int timeout = 0;

	public TCPConnection() {
	}

	public TCPConnection(String host, int port, int timeoutMillis) 
			throws IOException {
		//		this.addr = new InetSocketAddress(host, port);
		//		this.timeout = timeoutMillis;
		this.open(host, port, timeoutMillis);
	}

	public void open(String host, int port, int timeoutMillis)
			throws IOException
	{
		logger.debug("TCPConnection.open({},{},{})", host, port, timeoutMillis);
		mu.lock();
		try {

			this.addr = new InetSocketAddress(host, port);
			client = new Socket();
			client.connect(addr, timeout);

			client.setTcpNoDelay(false);
			client.setReceiveBufferSize(ConnectionImpl.DEFAULT_BUF_SIZE);
			client.setSendBufferSize(ConnectionImpl.DEFAULT_BUF_SIZE);

			writeStream = client.getOutputStream();
			readStream = client.getInputStream();
		} catch (IOException e) {
			throw e;
		} finally {
			mu.unlock();
		}
	}

	protected void setConnectTimeout(int value) {
		this.timeout = value;
	}

	// setSendTimeout?

	public boolean isSetup() {
		return (client != null);
	}

	public void teardown() {
		mu.lock();
		try {
			Socket c = client;

			client = null;
			writeStream = null;
			readStream = null;

			c.close();
		} catch (Exception e) {
			e.printStackTrace();
			// ignore
		} finally {
			mu.unlock();
		}
	}

	public InputStreamReader getInputStreamReader() {
		return new InputStreamReader(readStream);
	}

	public InputStream getReadBufferedStream(int size) {
		return new BufferedInputStream(readStream, size);
	}

	public OutputStream getWriteBufferedStream(int size) {
		return new BufferedOutputStream(writeStream, size);
	}

	public boolean isConnected() {
		if (client == null)
			return false;
		return client.isConnected();
	}

	public boolean isDataAvailable() {
		boolean rv = false;
		if (readStream == null)
			return false;

		try {
			rv = (readStream.available() > 0);
		} catch (IOException e) {
			// ignore
		}

		return rv;
	}

//	class TraceEnabledBufferedOutputStream  extends BufferedOutputStream {
//
//		public TraceEnabledBufferedOutputStream(OutputStream out) {
//			super(out);
//		}
//		public TraceEnabledBufferedOutputStream(OutputStream out, int size) {
//			super(out, size);
//		}		
//	}
//
//	class TraceEnabledBufferedInputStream  extends BufferedInputStream {
//		public TraceEnabledBufferedInputStream(InputStream in) {
//			super(in);
//		}
//		public TraceEnabledBufferedInputStream(InputStream in, int size) {
//			super(in, size);
//		}		
//	}

	/**
	 * Set the socket factory used to make connections with. Can be
	 * used to enable SSL connections by passing in a
	 * javax.net.ssl.SSLSocketFactory instance.
	 *
	 * @see #useSslProtocol
	 */
	protected void setSocketFactory(SocketFactory factory) {
		this.factory = factory;
	}

	protected void makeTLS(SSLContext context) throws IOException {
		this.sslContext = context;
		setSocketFactory(sslContext.getSocketFactory());
		SSLSocketFactory sslSf = (SSLSocketFactory)factory;
		SSLSocket sslSocket = (SSLSocket) sslSf.createSocket(client,        
				client.getInetAddress().getHostAddress(),
				client.getPort(),
				true);
		this.readStream = sslSocket.getInputStream();
		this.writeStream = sslSocket.getOutputStream();

		if (logger.isTraceEnabled())
			sslSocket.addHandshakeCompletedListener(new HandshakeListener());
		
		sslSocket.startHandshake();

	}
	
	class HandshakeListener implements HandshakeCompletedListener
	{
		public void handshakeCompleted(javax.net.ssl.HandshakeCompletedEvent
				event)
		{
			SSLSession session = event.getSession();
			logger.trace("Handshake Completed with peer {}",
					session.getPeerHost());
			logger.trace("   cipher: {}", session.getCipherSuite());
			Certificate[] certs = null;
			try
			{
				certs = session.getPeerCertificates();
			}
			catch (SSLPeerUnverifiedException puv)
			{
				certs = null;
			}
			if  (certs != null)
			{
				logger.trace("   peer certificates:");
				for (int z=0; z<certs.length; z++) 
					logger.trace("      certs[{}]: {}", z, certs[z]);
			}
			else
			{
				logger.trace("No peer certificates presented");
			}
		}
	}
}
