/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
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
import java.io.BufferedReader;
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
class TCPConnection implements AutoCloseable {
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
	boolean tlsDebug = false;
	
	public TCPConnection() {
	}

	public void open(String host, int port, int timeoutMillis)
			throws IOException
	{
		logger.debug("TCPConnection.open({},{},{})", host, port, timeoutMillis);
		mu.lock();
		try {

			this.addr = new InetSocketAddress(host, port);
			client = factory.createSocket();
			client.connect(addr, timeout);

			open();
			
		} catch (IOException e) {
			throw e;
		} finally {
			mu.unlock();
		}
	}

	public void open() throws IOException {
		mu.lock();
		try {
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
			if (client != null) {	
				client.close();
			}
		} catch (IOException e) {
			// ignore
		} finally {
			mu.unlock();
		}
	}

	public BufferedReader getBufferedInputStreamReader() {
		return new BufferedReader(new InputStreamReader(readStream));
	}

	public BufferedInputStream getBufferedInputStream(int size) {
		return new BufferedInputStream(readStream, size);
	}

	public BufferedOutputStream getBufferedOutputStream(int size) {
		return new BufferedOutputStream(writeStream, size);
	}

	public boolean isConnected() {
		if (client == null)
			return false;
		return client.isConnected();
	}

	public boolean isDataAvailable() throws IOException {
		if (readStream == null)
			return false;

		return (readStream.available() > 0);
	}

	/**
	 * Set the socket factory used to make connections with. Can be
	 * used to enable SSL connections by passing in a
	 * javax.net.ssl.SSLSocketFactory instance.
	 *
	 * @see #makeTLS
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

		if (isTlsDebug())
			sslSocket.addHandshakeCompletedListener(new HandshakeListener());

		sslSocket.startHandshake();

	}

	protected void setSocket(Socket sock) {
		mu.lock();
		try {
			client = sock;
		} finally {
			mu.unlock();
		}
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

	/**
	 * @return the tlsDebug
	 */
	public boolean isTlsDebug() {
		return tlsDebug;
	}

	/**
	 * @param tlsDebug the tlsDebug to set
	 */
	public void setTlsDebug(boolean tlsDebug) {
		this.tlsDebug = tlsDebug;
	}

	@Override
	public void close() {
		if (client!=null)
			try {
				client.close();
			} catch (IOException e) {
			}
	}
}
