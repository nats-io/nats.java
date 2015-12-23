package io.nats.client;

import static org.mockito.Mockito.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.nats.client.ConnectionImpl.*;
class TCPConnectionMock extends TCPConnection implements Runnable, AutoCloseable {
	final Logger logger = LoggerFactory.getLogger(TCPConnectionMock.class);

	final ExecutorService executor = Executors.newCachedThreadPool(
			new NATSThreadFactory("mockserver"));

	volatile boolean shutdown = false;

	public final static String defaultInfo = "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":1048576}\r\n";
	String serverInfo = defaultInfo;
	
	ReentrantLock mu = new ReentrantLock();
	Socket client = null;
	char[] buffer = new char[ConnectionImpl.DEFAULT_BUF_SIZE];

	// for the client
	//	private InputStream readBufferedStream;
	//	private OutputStream writeBufferedStream;

	private InputStream in;
	private OutputStream out;

	private InetSocketAddress addr = null;
	//	private int timeout = 0;

	PipedInputStream readStream = null;
	PipedOutputStream writeStream = null;

	BufferedReader br = null;
	OutputStream bw = null;

	Map<String, Integer> subs = new ConcurrentHashMap<String, Integer>();
	Map<String, ArrayList<Object>> groups = new ConcurrentHashMap<String, ArrayList<Object>>();

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#open(java.lang.String, int, int)
	 */
	@Override
	public void open(String host, int port, int timeoutMillis) throws IOException {
		mu.lock();
		try {
			this.addr = new InetSocketAddress(host, port);
			logger.info("opening TCPConnectionMock for {}:{}", 
					addr.getHostName(), addr.getPort());

			client = mock(Socket.class);

			writeStream = new PipedOutputStream();
			in = new PipedInputStream(writeStream, DEFAULT_BUF_SIZE);

			readStream = new PipedInputStream(DEFAULT_BUF_SIZE);
			out = new PipedOutputStream(readStream);

			// For the mock server thread
			//			br = new BufferedReader(in, DEFAULT_BUF_SIZE);
			bw = new BufferedOutputStream(out,DEFAULT_BUF_SIZE);

			executor.execute(this);
			when(client.isConnected()).thenReturn(true);
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
		return client.isConnected();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#teardown()
	 */
	@Override
	public void teardown() {
		super.teardown();
		when(client.isConnected()).thenReturn(false);

		//		executor.shutdown();
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getInputStreamReader()
	 */
	@Override
	public InputStreamReader getInputStreamReader() {
		return new InputStreamReader(readStream);
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getReadBufferedStream(int)
	 */
	@Override
	public InputStream getReadBufferedStream(int size) {
		return new BufferedInputStream(readStream, size);
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#getWriteBufferedStream(int)
	 */
	@Override
	public OutputStream getWriteBufferedStream(int size) {
		//		return new BufferedOutputStream(writeStream, size);
		return writeStream;
	}

	/* (non-Javadoc)
	 * @see io.nats.client.TCPConnection#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return client.isConnected();
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
		String control;

		InputStreamReader is = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(is);

		logger.info("started");

		try {
			bw.write(serverInfo.getBytes());
			bw.flush();
			logger.info("=> {}", new String(serverInfo.getBytes()).trim());
			while(!shutdown)
			{
				control = br.readLine();
				if (control==null)
					break;

				logger.info("<= {}", control);
				if (control.equalsIgnoreCase(PING_PROTO.trim())) {
					bw.write(PONG_PROTO.getBytes());
					bw.flush();
					logger.info("=> PONG");
				} 
				else if (control.equalsIgnoreCase(PONG_PROTO.trim())) {
				}
				else if (control.toUpperCase().startsWith("CONNECT")) {
					//TODO get some info
				}
				else if (control.startsWith("UNSUB")) {
					processUnsub(control);
				}
				else if (control.startsWith("SUB")) {
					processSubscription(control);
				}
				else if (control.startsWith("PUB")) {
					String subj=null;
					String reply=null;
					Integer nBytes=0;
					byte[] payload = null;
					
					String[] tokens = control.split("\\s+");
					
					subj = tokens[1];
					switch (tokens.length) {
					case 3:
						nBytes = Integer.parseInt(tokens[2]);
						break;
					case 4:
						reply = tokens[2];
						nBytes = Integer.parseInt(tokens[3]);
						break;
					default:
						throw new IllegalArgumentException("Wrong number of PUB arguments: " + tokens.length);
					}
					
					if (nBytes>0) {
						payload = br.readLine().getBytes(Charset.forName("UTF-8"));
						if (payload.length > nBytes)
							throw new IllegalArgumentException("actual payload size ("+ payload.length 
									+ "), expected: " + nBytes);							
					}
					
					deliverMessage(subj, reply, payload);
				}
				else {
					sendErr("Unknown Protocol Operation");
//					break;
				}
			}
//			shutdown=true;
//			bw.close();
//			br.close();
		} catch (IOException e) {
		}
		finally {
			this.teardown();				
		}
	}

	private void sendErr(String err) throws IOException {
		String str = String.format("-ERR '%s'\r\n", err);
		bw.write(str.getBytes());
		bw.flush();
		logger.info("=> " + str.trim());		
	}
	private void deliverMessage(String subj, String reply, byte[] payload) {
		String out = null;
		int sid = -1;
		
		if (subs.containsKey(subj))
			sid = subs.get(subj);
		
		if (reply != null)
			out = String.format("MSG %s %d %s %d\r\n", 
				subj, sid, reply, payload.length);
		else
			out = String.format("MSG %s %d %d\r\n", 
					subj, sid, payload.length);
		System.err.println(out);
		try {
			bw.write(out.getBytes());
			bw.write(payload, 0, payload.length);
			bw.write(ConnectionImpl._CRLF_.getBytes());
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processUnsub(String control) {
		String[] tokens = control.split("\\s+");
		int sid = Integer.parseInt(tokens[1]);
		int max = 0;
		if (tokens.length==3)
			max = Integer.parseInt(tokens[2]);
		
		for (String s : subs.keySet())
		{
			if (subs.get(s) == sid) {
				subs.remove(s);
			}
		}
	}
	private void processSubscription(String control) {
//		String buf = control.replaceFirst("SUB\\s+", "");
		String[] tokens = control.split("\\s+");

		String subj = null;
		String qgroup = null;
		int sid = -1;
		
		subj = tokens[1];
		
		switch (tokens.length) {
		case 3:
			sid = Integer.parseInt(tokens[2]);
			break;
		case 4:
			qgroup = tokens[2];
			sid = Integer.parseInt(tokens[3]);
			break;
		default:
			throw new IllegalArgumentException("Wrong number of SUB arguments: " + tokens.length);
		}
		subs.put(subj, sid);
	}
	
	public void sendPing() throws IOException {
		byte[] pingProtoBytes = PING_PROTO.getBytes();
		int pingProtoBytesLen = pingProtoBytes.length;

		bw.write(pingProtoBytes, 0, pingProtoBytesLen);
		logger.trace("=> {}", new String(pingProtoBytes).trim());
		bw.flush();
	}

	public void setServerInfoString(String info) {
		this.serverInfo=info;
	}
	
	@Override
	public void close() throws Exception {
		executor.shutdownNow();
		this.shutdown();
	}
	

}
