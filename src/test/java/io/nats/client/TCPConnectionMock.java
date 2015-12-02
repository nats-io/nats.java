package io.nats.client;

import static org.mockito.Mockito.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import static io.nats.client.ConnectionImpl.*;
class TCPConnectionMock extends TCPConnection implements Runnable {

	volatile boolean shutdown = false;
	
	final static String defaultInfo = "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.6.8\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":1048576}\r\n";
	
	ReentrantLock mu = new ReentrantLock();
	Socket client = null;
	byte[] buffer = new byte[ConnectionImpl.DEFAULT_BUF_SIZE];

	// for the client
//	private InputStream readBufferedStream;
//	private OutputStream writeBufferedStream;

	private InputStream in;
	private OutputStream out;

	private InetSocketAddress addr = null;
	private int timeout = 0;

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#open(java.lang.String, int, int)
	 */
	@Override
	public void open(String host, int port, int timeoutMillis) throws IOException {
		mu.lock();
		try {

			this.addr = new InetSocketAddress(host, port);

			client = mock(Socket.class);
			//		    when(client.getOutputStream()).thenReturn(out);
			//		    when(client.getInputStream()).thenReturn(in);	

			//			client.connect(addr, timeout);
			//			
			//			client.setTcpNoDelay(true);
			//			client.setReceiveBufferSize(ConnectionImpl.DEFAULT_BUF_SIZE);
			//			client.setSendBufferSize(ConnectionImpl.DEFAULT_BUF_SIZE);

			//			writeStream = client.getOutputStream();
			//			readStream = client.getInputStream();
			writeStream = new PipedOutputStream();
			readStream = new PipedInputStream(DEFAULT_BUF_SIZE);
			in = new PipedInputStream((PipedOutputStream)writeStream, DEFAULT_BUF_SIZE);
			out = new PipedOutputStream((PipedInputStream)readStream);

		} finally {
			mu.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#setConnectTimeout(int)
	 */
	@Override
	protected void setConnectTimeout(int value) {
		super.setConnectTimeout(value);
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#isSetup()
	 */
	@Override
	public boolean isSetup() {
		return super.isSetup();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#teardown()
	 */
	@Override
	public void teardown() {
		super.teardown();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getInputStreamReader()
	 */
	@Override
	public InputStreamReader getInputStreamReader() {
		return super.getInputStreamReader();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getReadBufferedStream(int)
	 */
	@Override
	public InputStream getReadBufferedStream(int size) {
		return super.getReadBufferedStream(size);
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getWriteBufferedStream(int)
	 */
	@Override
	public OutputStream getWriteBufferedStream(int size) {
		return super.getWriteBufferedStream(size);
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return super.isConnected();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#isDataAvailable()
	 */
	@Override
	public boolean isDataAvailable() {
		return super.isDataAvailable();
	}

	public void shutdown() {
		this.shutdown = true;
	}
	
	@Override
	public void run() {
		InputStream br = new BufferedInputStream(in, 65536);
		OutputStream bw = new BufferedOutputStream(out,65536);
		int len;
		String strBuffer;
		
		try {
			bw.write(defaultInfo.getBytes());
			
			while(!shutdown)
			{
				len = br.read(buffer, 0, DEFAULT_BUF_SIZE);
				if (len==-1)
					break;
				strBuffer = new String(buffer, 0, len);
				
				if (strBuffer.equalsIgnoreCase(PING_PROTO)) {
					bw.write(PONG_PROTO.getBytes());
					bw.flush();
				}
				else if (strBuffer.startsWith("CONNECT")) {
					//TODO get some info
				}
			}
			bw.close();
			br.close();
		} catch (IOException e) {
		}
		finally {
			this.teardown();				
		}
	}

}
