package io.nats.client.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.locks.ReentrantLock;

/// Convenience class representing the TCP connection to prevent 
/// managing two variables throughout the NATs client code.
public class TCPConnection {

	private final static int DEFAULT_BUF_SIZE = 32768;

	/// A note on the use of streams. .NET provides a BufferedStream
	/// that can sit on top of an IO stream, in this case the network
	/// stream. It increases performance by providing an additional
	/// buffer.
	///
	/// So, here's what we have:
	/// Client code
	/// ->BufferedStream (bw)
	/// ->NetworkStream (srvStream)
	/// ->TCPClient (srvClient);
	///
	/// TODO: Test various scenarios for efficiency. Is a
	/// BufferedReader directly over a network stream really
	/// more efficient for NATS?
	///
	ReentrantLock mu = new ReentrantLock();
	Socket client = null;
	private OutputStream writeStream = null;
	private InputStream readStream = null;
	private InetSocketAddress addr = null;
	private int timeout = 0;

	public TCPConnection(String host, int port, int timeoutMillis) {
		this.addr = new InetSocketAddress(host, port);
		this.timeout = timeoutMillis;
	}

	public TCPConnection() {
	}

	public void open(String host, int port, int timeoutMillis) {
		mu.lock();
		try {

			this.addr = new InetSocketAddress(host, port);
			client = new Socket();
			client.connect(addr, timeoutMillis);
			// #if async_connect
			// client = new TcpClient();
			// IAsyncResult r = client.BeginConnect(s.url.Host, s.url.Port,
			// null, null);
			//
			// if (r.AsyncWaitHandle.WaitOne(
			// TimeSpan.FromMilliseconds(timeoutMillis)) == false)
			// {
			// client = null;
			// throw new NATSConnectionException("Timeout");
			// }
			// client.EndConnect(r);
			// #endif

			client.setTcpNoDelay(false);
			client.setReceiveBufferSize(DEFAULT_BUF_SIZE);
			client.setSendBufferSize(DEFAULT_BUF_SIZE);

			writeStream = client.getOutputStream();
			readStream = client.getInputStream();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			OutputStream ws = writeStream;
			InputStream rs = readStream;

			client = null;
			writeStream = null;
			readStream = null;

			rs.close();
			ws.close();
			c.close();
		} catch (IOException e) {
			// ignore
		} finally {
			mu.unlock();
		}
	}
	
	public InputStreamReader getInputStreamReader() {
		InputStreamReader rv = null;
		
		try {
			rv = new InputStreamReader(client.getInputStream());
		} catch (IOException e) {
			// ignore
		}
		return rv;
	}

	public DataInputStream getReadBufferedStream(int size) {
		DataInputStream rv = null;
		try {
			rv = new DataInputStream(new BufferedInputStream(client.getInputStream(), DEFAULT_BUF_SIZE * 6));
		} catch (IOException e) {
			// ignore
		}
		return rv;
	}

	public DataOutputStream getWriteBufferedStream(int size) {
		DataOutputStream rv = null;
		try {
			rv = new DataOutputStream(new BufferedOutputStream(client.getOutputStream(), DEFAULT_BUF_SIZE * 6));
		} catch (IOException e) {
			// ignore
		}
		return rv;
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
}
