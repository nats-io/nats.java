package io.nats.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static io.nats.client.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionImpl implements Connection, AutoCloseable {
	final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

	String version = null;

	//	final static int DEFAULT_SCRATCH_SIZE = 512;	

	private static final String inboxPrefix = "_INBOX.";

	public ConnState status = ConnState.DISCONNECTED;

	protected final static String STALE_CONNECTION     = "Stale Connection";

	// Client version
	protected static final String 	VERSION					= "1.0.0";

	// Default language string for CONNECT message
	protected static final String	LANG_STRING				= "java";

	// Scratch storage for assembling protocol headers
	//	protected static final int SCRATCH_SIZE = 512;

	// The size of the bufio reader/writer on top of the socket.
	//	protected static final int DEFAULT_BUF_SIZE = 32768;
	protected static final int DEFAULT_BUF_SIZE = 65536;
	protected static final int DEFAULT_STREAM_BUF_SIZE = 8192;

	// The size of the bufio while we are reconnecting
	protected static final int DEFAULT_PENDING_SIZE = 1024 * 1024;

	// The buffered size of the flush "kick" channel
	protected static final int FLUSH_CHAN_SIZE = 1024;

	public static final String _CRLF_ = "\r\n";
	public static final String _EMPTY_ = "";
	public static final String _SPC_ = " ";
	public static final String _PUB_P_ = "PUB ";

	// Operations
	public static final String _OK_OP_ = "+OK";
	public static final String _ERR_OP_ = "-ERR";
	public static final String _MSG_OP_ = "MSG";
	public static final String _PING_OP_ = "PING";
	public static final String _PONG_OP_ = "PONG";
	public static final String _INFO_OP_ = "INFO";

	// Message Prototypes
	public static final String CONN_PROTO = "CONNECT %s" + _CRLF_;
	public static final String PING_PROTO = "PING" + _CRLF_;
	public static final String PONG_PROTO = "PONG" + _CRLF_;
	public static final String PUB_PROTO = "PUB %s %s %d" + _CRLF_;
	public static final String SUB_PROTO = "SUB %s %s %d" + _CRLF_;
	public static final String UNSUB_PROTO = "UNSUB %d %d" + _CRLF_;


	//	private ConnectionImpl nc 				= this;
	private final Lock mu 					= new ReentrantLock();

	protected ClosedEventHandler closedEventHandler;
	protected DisconnectedEventHandler disconnectedEventHandler;
	protected ReconnectedEventHandler reconnectedEventHandler;
	protected ExceptionHandler exceptionHandler;


	private AtomicLong sidCounter		= new AtomicLong();
	private URI url 						= null;
	protected Options opts 					= null;

	private TCPConnection conn				= null;

	// Prepare protocol messages for efficiency
	byte[] pubProtoBuf = null;

	// we have a buffered reader for writing, and reading.
	// This is for both performance, and having to work around
	// interlinked read/writes (supported by the underlying network
	// stream, but not the BufferedStream).

	private OutputStream bw 			= null;
	private InputStream br 				= null;
	private ByteArrayOutputStream pending 	= null;

	private ReentrantLock flusherLock		= new ReentrantLock();
	private boolean flusherKicked 			= false;
	private final Condition flusherKickedCondition = flusherLock.newCondition();
	private boolean flusherDone     		= false;

	private Map<Long, SubscriptionImpl> subs = null;
	protected List<Srv> srvPool 				= null;
	private Exception lastEx 				= null;
	private ServerInfo info 				= null;
	private Vector<Thread> socketWatchers	= new Vector<Thread>();
	private ScheduledExecutorService ptmr 	= null;
	private int pout;

	// Initialized in readLoop
	protected Parser ps						= null;
	protected MsgArg msgArgs				= null;
	protected byte[] pingProtoBytes			= null;
	protected int pingProtoBytesLen			= 0;
	protected byte[] pongProtoBytes			= null;
	protected int pongProtoBytesLen			= 0;
	protected byte[] pubPrimBytes			= null;
	protected int pubPrimBytesLen			= 0;

	protected byte[] crlfProtoBytes			= null;
	protected int crlfProtoBytesLen			= 0;

	protected Statistics stats				= null;
	private Queue<Channel<Boolean>> pongs	= new LinkedBlockingQueue<Channel<Boolean>>();

	private final ExecutorService taskExec = Executors.newCachedThreadPool(new NATSThreadFactory("natsthreadfactory"));
	private Random r						= null;

	ConnectionImpl(Options o)
	{
		this(o, null);
	}

	ConnectionImpl(Options o, TCPConnection tcpconn) {
		Properties props = this.getProperties("jnats.properties");
		version = props.getProperty("client.version");

		this.opts = o;
		this.subs = new ConcurrentHashMap<Long, SubscriptionImpl>();
		this.stats = new Statistics();
		this.ps = new Parser(this);
		this.msgArgs = new MsgArg();
		if (tcpconn != null)
			this.conn = tcpconn;
		else 
			this.conn = new TCPConnection();

		sidCounter.set(0);

		pingProtoBytes = PING_PROTO.getBytes();
		pingProtoBytesLen = pingProtoBytes.length;
		pongProtoBytes = PONG_PROTO.getBytes();
		pongProtoBytesLen = pongProtoBytes.length;
		pubPrimBytes = _PUB_P_.getBytes(Charset.forName("UTF-8"));
		pubPrimBytesLen = pubPrimBytes.length;

		crlfProtoBytes = _CRLF_.getBytes(Charset.forName("UTF-8"));
		crlfProtoBytesLen = crlfProtoBytes.length;

		// predefine the start of the publish protocol message.
		buildPublishProtocolBuffer(MAX_CONTROL_LINE_SIZE);

		setupServerPool();
	}

	protected Properties getProperties(InputStream inputStream)
	{
		Properties rv = new Properties();
		try {
			if (inputStream==null)
				rv = null;
			else
				rv.load(inputStream);
		} catch (IOException e) {
			rv = null;
		}
		return rv;
	}

	protected Properties getProperties(String resourceName)
	{
		InputStream is = 
				getClass().getClassLoader().getResourceAsStream(resourceName);
		return getProperties(is);
	}

	private void buildPublishProtocolBuffer(int size)
	{
		pubProtoBuf = new byte[size];
		System.arraycopy(pubPrimBytes, 0, pubProtoBuf, 0, pubPrimBytesLen);
	}


	// Create the server pool using the options given.
	// We will place a Url option first, followed by any
	// Srv Options. We will randomize the server pool unless
	// the NoRandomize flag is set.
	protected void setupServerPool() {

		URI url = opts.getUrl();
		List<URI> servers = opts.getServers();

		srvPool = new ArrayList<Srv>();
		// First, add the supplied url, if not null or empty.
		if (url != null) {
			srvPool.add(new Srv(url));
		}

		if ((servers != null) && (!servers.isEmpty())) {
			Srv srv = null;
			Iterator<URI> it = servers.listIterator();
			while (it.hasNext()) {
				srv = new Srv(it.next());
				srvPool.add(srv);
			}
		}

		if (srvPool.isEmpty()) {
			try {
				srvPool.add(new Srv(new URI(DEFAULT_URL)));
			} catch (URISyntaxException e) {
			}
		}

		/* 
		 * At this point, srvPool being empty would be 
		 * programmer error. 
		 */

		// Return the first server in the list
		this.url = srvPool.get(0).url;
	}

	protected Srv currentServer() {
		Srv rv = null;
		for (Srv s : srvPool) {
			if (s.url.equals(this.url)) {
				rv = s;
				break;
			}
		}
		return rv;
	}

	protected Srv selectNextServer() {
		int maxReconnect = opts.getMaxReconnect();
		Srv s = currentServer();
		logger.trace("In selectNextServer()");
		/**
		 * Remove the current server from the list. remove() is O(N) but the
		 * server list will never be large enough for this to be a huge factor
		 */
		srvPool.remove(s);

		/**
		 * if the maxReconnect is unlimited, or the number of reconnect attempts
		 * is less than maxReconnect, move the current server to the end of the
		 * list.
		 * 
		 */
		if ((maxReconnect < 0) || (s.reconnects < maxReconnect)) {
			logger.trace("s.reconnects: " + s.reconnects + " maxReconnect: " + maxReconnect);
			srvPool.add(s);
		}

		if (srvPool.isEmpty()) {
			this.url = null;
			return null;
		}

		this.url = srvPool.get(0).url;
		return srvPool.get(0);
	}

	protected ConnectionImpl start() throws IOException, TimeoutException {
		// Create actual socket connection
		// For first connect we walk all servers in the pool and try
		// to connect immediately.
		boolean connected = false;

		for (Srv s : srvPool) {
			this.url = s.url;

			try {
				lastEx = null;
				mu.lock();
				try {
					logger.debug("Connecting to {}", this.url);
					// Create the underlying TCP connection
					if (createConn()) {
						logger.debug("Connected to {}", this.url);
						//s.didConnect = true;
						s.reconnects = 0;
						// Initial connection protocol
						processConnectInit();
						connected = true;
					}
				} finally {
					mu.unlock();
				}
			} catch (Exception e) {
				logger.debug("{} Exception: {}", this.url, e.getMessage());
				//				e.printStackTrace();
				lastEx = e;
				close(ConnState.DISCONNECTED, false);
				mu.lock();
				try {
					this.url = null;
				} finally {
					mu.unlock();
				}
			}

			if (connected)
				break;
		} // for

		mu.lock();
		try {
			if (status != ConnState.CONNECTED) {
				if (lastEx == null)
					lastEx = new NoServersException();
				//				lastEx.printStackTrace();
				throw (IOException)this.lastEx;
			}
		} finally {
			mu.unlock();
		}

		return this;
	}

	// createConn will connect to the server and wrap the appropriate
	// bufio structures. It will do the right thing when an existing
	// connection is in place.
	private boolean createConn() {
		currentServer().updateLastAttempt();

		Srv s=null;
		try {
			s = currentServer();
			conn.open(s.url.getHost(), s.url.getPort(), opts.getConnectionTimeout());
			if (!conn.isConnected()) {
				logger.debug("Couldn't connect to " + s.url.getHost() + " port " + s.url.getPort());
				return false;
			}
			if ((pending != null) && (bw != null)) {
				// flush to the pending buffer
				//				logger.trace("Pending = " + pending.toString());
				logger.trace("Flushing old outputstream to pending");
				bw.flush();
			}
			bw = conn.getWriteBufferedStream(DEFAULT_STREAM_BUF_SIZE);
			br = conn.getReadBufferedStream(DEFAULT_STREAM_BUF_SIZE);
		} catch (IOException e) {
			//			lastEx = e;
			logger.trace("Couldn't establish connection to {}: {}", s.url, e.getMessage());
			return false;
		}

		return true;
	}

	// This will clear any pending flush calls and release pending calls.
	private void clearPendingFlushCalls()
	{
		logger.trace("clearPendingFlushCalls()");
		mu.lock();
		try
		{
			// Clear any queued pongs, e.g. pending flush calls.
			for (Channel<Boolean> ch : pongs)
			{
				if (ch != null) {
					ch.add(true);
					logger.trace("Cleared PONG");
				}
			}

			pongs.clear();
		} finally {
			mu.unlock();
		}
	}

	@Override
	public void close() {
		close(ConnState.CLOSED, true);
	}

	// Low level close call that will do correct cleanup and set
	// desired status. Also controls whether user defined callbacks
	// will be triggered. The lock should not be held entering this
	// function. This function will handle the locking manually.
	private void close(ConnState closeState, boolean invokeDelegates)
	{
		logger.trace("close({}, {})", closeState, String.valueOf(invokeDelegates));
		DisconnectedEventHandler disconnectedEventHandler = null;
		ClosedEventHandler closedEventHandler = null;

		mu.lock();
		try
		{
			if (isClosed())
			{
				status = closeState;
				return;
			}

			status = ConnState.CLOSED;
		} finally {
			mu.unlock();
		}

		// Kick the routines so they fall out.
		// fch will be closed on finalizer
		kickFlusher();

		// Clear any queued pongs, e.g. pending flush calls.
		clearPendingFlushCalls();

		mu.lock();
		try
		{
			if (ptmr != null) {
				ptmr.shutdownNow();
			}

			// Close sync subscriber channels and release any
			// pending NextMsg() calls.
			for (Long key : subs.keySet())
			{
				SubscriptionImpl s = subs.get(key);
				s.closeChannel();
			}
			subs.clear();
			//			sidCounter.set(0);

			// perform appropriate callback is needed for a
			// disconnect;
			if (invokeDelegates && conn.isSetup() &&
					opts.disconnectedEventHandler != null)
			{
				// TODO:  Mirror go, but this can result in a callback
				// being invoked out of order
				logger.trace("Calling disconnectedEventHandler from Connection.close()");
				disconnectedEventHandler = opts.getDisconnectedEventHandler();
				taskExec.submit(new DisconnectedEventHandlerTask(disconnectedEventHandler, new ConnectionEvent(this)));
			}

			// Go ahead and make sure we have flushed the outbound buffer.
			status = ConnState.CLOSED;
			if (conn.isSetup())
			{
				if (bw != null)
					try {
						bw.flush();
					} catch (IOException e) {
						// TODO Do something here?
					}
				conn.teardown();
			}

			closedEventHandler = opts.getClosedEventHandler();
		} finally {
			mu.unlock();
		}

		if (invokeDelegates && closedEventHandler != null)
		{
			closedEventHandler.onClose(new ConnectionEvent(this));
		}

		taskExec.shutdownNow();
		try {
			logger.trace("Waiting for executor to shutdown");
			taskExec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
		} finally {
			logger.trace("Thread executor has been shut down");
		}

		mu.lock();
		try {
			status = closeState;
		} finally {
			mu.unlock();
		}
	}

	protected void processConnectInit() throws IOException 
	{
		status = ConnState.CONNECTING;
		logger.trace("processConnectInit(): {}", this.url);
		// Process the INFO protocol that we should be receiving
		processExpectedInfo();

		// Send the CONNECT and PING protocol, and wait for the PONG.
		sendConnect();

		// Start the readLoop and flusher threads
		spinUpSocketWatchers();
	}

	// This will check to see if the connection should be
	// secure. This can be dictated from either end and should
	// only be called after the INIT protocol has been received.
	private void checkForSecure() throws 
	SecureConnectionWantedException, SecureConnectionRequiredException,
	IOException
	{
		// Check to see if we need to engage TLS
		// Check for mismatch in setups
		if (opts.isSecure() && !info.isTlsRequired())
		{
			throw new SecureConnectionWantedException();
		}
		else if (info.isTlsRequired() && !opts.isSecure())
		{
			throw new SecureConnectionRequiredException();
		}

		// Need to rewrap with bufio
		if (opts.isSecure())
		{
			makeTLSConn();
		}
	}

	// makeSecureConn will wrap an existing Conn using TLS
	private void makeTLSConn() throws IOException
	{
		conn.setTlsDebug(opts.isTlsDebug());
		conn.makeTLS(opts.getSslContext());
		bw = conn.getWriteBufferedStream(DEFAULT_BUF_SIZE);
		br = conn.getReadBufferedStream(DEFAULT_BUF_SIZE);
	}

	protected void processExpectedInfo() throws ConnectionException {
		Control c;

		try {
			c = readOp();
		} catch (IOException e) {
			processOpError(e);
			return;
		}
		// TODO why adjust sendtimeouts in .NET?

		if (!c.op.equals(_INFO_OP_)) {
			throw new ConnectionException("Protocol exception, INFO not received");
		}

		processInfo(c.args);
		try {
			checkForSecure();
		} catch (SecureConnectionWantedException e) {
			logger.trace("opts.isSecure is: {}", opts.isSecure());
			throw new ConnectionException(e);
		} catch (IOException e) {
			throw new ConnectionException(e);
		}
	}

	// processPing will send an immediate pong protocol response to the
	// server. The server uses this mechanism to detect dead clients.
	protected void processPing() 
	{
		try {
			sendProto(pongProtoBytes, pongProtoBytesLen);
		} catch (IOException e) {
			lastEx = e;
			//			e.printStackTrace();
		}
	}

	// processPong is used to process responses to the client's ping
	// messages. We use pings for the flush mechanism as well.
	protected void processPong()
	{
		logger.trace("Processing PONG");
		Channel<Boolean> ch = new Channel<Boolean>(1);
		mu.lock();
		try {
			if (pongs.size() > 0)
				ch = pongs.poll();

			pout = 0;

			if (ch != null)
			{
				ch.add(true);
				logger.trace("Processed PONG");
			}
		} finally {
			mu.unlock();
		}
	}

	// processOK is a placeholder for processing OK messages.
	protected void processOK()
	{
		// NOOP;
		return;
	}

	// processInfo is used to parse the info messages sent
	// from the server.
	protected void processInfo(String info) {
		if ((info == null) || info.isEmpty()) {
			return;
		}

		this.info = new ServerInfo(info);

		return;
	}

	private void processOpError(Exception e) {
		logger.trace("processOpError(e={}) state={} reconnectAllowed={} ", e.getClass().getName(), status, opts.isReconnectAllowed());
		//		e.printStackTrace();
		boolean disconnected = false;

		mu.lock();
		try{
			if (isConnecting() || isClosed() || isReconnecting())
			{
				return;
			}

			if (opts.isReconnectAllowed() && status == ConnState.CONNECTED)
			{
				logger.trace("\t\tcalling processReconnect() in state {}", status);
				processReconnect();
			}
			else
			{
				logger.trace("\t\tcalling processReconnect() in state {}", status);
				processDisconnect();
				disconnected = true;
				lastEx = e;
			}
		} finally {
			mu.unlock();
		}

		if (disconnected)
		{
			close();
		}

	}

	private void processDisconnect() {
		logger.trace("processDisconnect(), lastEx={}",
				lastEx==null ? "null" : lastEx.getClass().getName());
		status = ConnState.DISCONNECTED;
		if (lastEx == null)
			return;
	}

	// This will process a disconnect when reconnect is allowed.
	// The lock should not be held on entering this function.
	private void processReconnect()
	{
		logger.trace("processReconnect()");
		mu.lock();
		try {
			// If we are already in the proper state, just return.
			if (isReconnecting())
				return;

			status = ConnState.RECONNECTING;

			if (ptmr != null)
			{
				//TODO is this right?
				ptmr.shutdownNow();         
			}

			if (conn.isSetup())
			{
				logger.trace("Calling conn.teardown()");
				conn.teardown();
			}
			taskExec.submit(new ReconnectTask());

		} finally {
			mu.unlock();
		}
	}

	private class ReconnectTask implements Runnable {

		@Override
		public void run() {
			doReconnect();
		}

	}

	@Override
	public boolean isReconnecting() {
		mu.lock();
		try {
			return (status == ConnState.RECONNECTING);
		} finally {
			mu.unlock();
		}
	}

	// Test if Conn is connected or connecting.
	private boolean isConnected()
	{
		return (status == ConnState.CONNECTING || status == ConnState.CONNECTED);
	}

	@Override
	public boolean isClosed() {
		mu.lock();
		try {
			return (status == ConnState.CLOSED);
		} finally {
			mu.unlock();
		}
	}

	// flushReconnectPending will push the pending items that were
	// gathered while we were in a RECONNECTING state to the socket.
	private void flushReconnectPendingItems()
	{
		logger.trace("flushReconnectPendingItems()");
		if (pending == null)
			return;

		if (pending.size() > 0)
		{
			try {
				logger.trace("flushReconnectPendingItems() writing {} bytes.", pending.size() ); 
				bw.write(pending.toByteArray(), 0, (int)pending.size());
				bw.flush();
			} catch (IOException e) {
			}
		}

		pending = null;
	}

	// Try to reconnect using the option parameters.
	// This function assumes we are allowed to reconnect.
	private void doReconnect()
	{
		logger.trace("doReconnect()");
		// We want to make sure we have the other watchers shutdown properly
		// here before we proceed past this point
		waitForExits();



		// FIXME(dlc) - We have an issue here if we have
		// outstanding flush points (pongs) and they were not
		// sent out, but are still in the pipe.

		// Hold the lock manually and release where needed below.
		mu.lock();

		pending = new ByteArrayOutputStream();
		bw = new DataOutputStream(pending);

		// Clear any errors.
		lastEx = null;

		if (opts.getDisconnectedEventHandler() != null)
		{
			mu.unlock();
			disconnectedEventHandler = opts.getDisconnectedEventHandler();

			taskExec.submit(new DisconnectedEventHandlerTask(disconnectedEventHandler,new ConnectionEvent(this)));

			mu.lock();
		}

		Srv s;
		while ((s = selectNextServer()) != null)
		{
			//			if (lastEx != null) {
			//				logger.trace("Stopped iterating over list due to exception: " + lastEx.getMessage());
			//				lastEx.printStackTrace();
			//				break;
			//			}

			// Sleep appropriate amount of time before the
			// connection attempt if connecting to same server
			// we just got disconnected from.
			double elapsedMillis = s.timeSinceLastAttempt();

			if (elapsedMillis < opts.getReconnectWait())
			{
				double sleepTime = opts.getReconnectWait() - elapsedMillis;

				mu.unlock();
				try {
					sleepMsec((int)sleepTime);
				} finally {
					mu.lock();
				}
			}
			else
			{
				// Yield so other things like unsubscribes can
				// proceed.
				mu.unlock();
				try {
					sleepMsec(50);
				} finally {
					mu.lock();
				}
			}

			if (isClosed())
				break;

			s.reconnects++;

			//			try
			//			{
			logger.trace("doReconnect: trying createConn() for s.url={}, s.reconnects={}", s.url, s.reconnects);
			// try to create a new connection
			if (!createConn())
				continue;
			//			}
			//			catch (Exception ne)
			//			{
			//				logger.trace("doReconnect: createConn() failed({}), looping again", ne.getMessage());
			//				// not yet connected, retry and hold
			//				// the lock.
			//				continue;
			//			}

			// We are reconnected.
			stats.incrementReconnects();

			// TODO Clean this up? It's never used in any of the clients
			// Clear out server stats for the server we connected to..
			// s.didConnect = true;

			// process our connect logic
			try
			{
				processConnectInit();
			}
			catch (IOException e)
			{
				//				e.printStackTrace();
				lastEx = e;
				status = ConnState.RECONNECTING;
				continue;
			}

			s.reconnects = 0;

			// Process CreateConnection logic
			try
			{
				// Send existing subscription state
				resendSubscriptions();

				// Now send off and clear pending buffer
				flushReconnectPendingItems();

				// we are connected.
				status = ConnState.CONNECTED;
			}
			catch (IOException e)
			{
				status = ConnState.RECONNECTING;
				continue;
			}

			// get the event handler under the lock
			ReconnectedEventHandler reconnectedEh = opts.getReconnectedEventHandler();

			// Release the lock here, we will return below
			mu.unlock();

			// flush everything
			try {
				flush();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (reconnectedEh != null)
			{
				reconnectedEh.onReconnect(new ConnectionEvent(this));
			}

			return;
		} // while

		// we have no more servers left to try.
		if (lastEx == null)
			lastEx = new IOException("Unable to reconnect");

		mu.unlock();

		close();
	}
	private boolean isConnecting() {
		mu.lock();
		try {
			return (status == ConnState.CONNECTING);
		} finally {
			mu.unlock();
		}

	}

	// processErr processes any error messages from the server and
	// sets the connection's lastError.
	protected void processErr(ByteBuffer errorStream)
	{
		boolean invokeDelegates = false;
		Exception ex = null;
		String s = new String(errorStream.array(), 0, errorStream.position(), Charset.forName("UTF-8"));
		logger.trace("processErr(errorStream={})", s);

		if (STALE_CONNECTION.equals(s))
		{
			processOpError(new StaleConnectionException());
		}
		else
		{
			ex = new NATSException(s);
			mu.lock();
			try
			{
				lastEx = ex;

				if (status != ConnState.CONNECTING)
				{
					invokeDelegates = true;
				}
			} finally {
				mu.unlock();
			}

			close(ConnState.CLOSED, invokeDelegates);
		}
	}
	// caller must lock
	protected void sendConnect() throws IOException
	{
		String line = null;

		logger.trace("sendConnect()");
		try {
			bw.write(connectProto().getBytes());
			logger.trace("=> {}", connectProto().trim());

			bw.write(pingProtoBytes, 0, pingProtoBytesLen);
			logger.trace("=> {}", new String(pingProtoBytes).trim());

			bw.flush();
		} 
		catch (IOException e) {
			if (lastEx == null)
				throw new IOException("Error sending connect protocol message", e);			
		} 
		finally {}

		try {
			line = readLine();
		} catch (IOException e) {
			//			this.processOpError(e);
			//			return;
			throw new ConnectionException("nats: Connection read error.", e);
		}

		// Handle +OK
		if (opts.isVerbose() && line.equals(_OK_OP_))
		{
			try {
				line = readLine();
			} catch (IOException e) {
				//				this.processOpError(e);
				//				return;
				throw new ConnectionException("nats: Connection read error.", e);
			}
		}

		// We expect a PONG
		if (!PONG_PROTO.trim().equals(line)) {
			// But it could be something else, like -ERR
			if (line.startsWith(_ERR_OP_))
			{
				if (line.contains("Authorization")) {
					throw new AuthorizationException(line.replace(_ERR_OP_, "").trim());
				}
				throw new ConnectionException("nats: " + line.substring(_ERR_OP_.length()).trim());
			} 
			throw new ConnectionException("nats: Unexpected protocol line: [" + line + "]");			
		}

		// This is where we are truly connected.
		status = ConnState.CONNECTED;

		return;
	}

	/* 
	 * This method is only used by processPing. It is also used in the gnatsd 
	 * tests.
	 */
	protected void sendProto(byte[] value, int length) throws IOException
	{
		logger.trace("in sendProto()");
		mu.lock();
		try {
			logger.trace("in sendProto(), writing");
			bw.write(value, 0, length);
			logger.trace("=> {}", new String(value).trim() );
			kickFlusher();
		} finally {
			mu.unlock();
		}
	}

	// Generate a connect protocol message, issuing user/password if
	// applicable. The lock is assumed to be held upon entering.
	private String connectProto() {
		String u = url.getUserInfo();
		String user = null;
		String pass = null;

		if ((u != null) && !u.isEmpty()) {
			if (u.contains(":")) {
				String[] userpass = u.split(":");
				if (userpass.length > 0) {
					user = userpass[0];
				}
				if (userpass.length > 1) {
					pass = userpass[1];
				}
			} else {
				user = u;
			}
		}

		ConnectInfo info = new ConnectInfo(opts.isVerbose(), opts.isPedantic(), user, pass,
				opts.isSecure(), opts.getConnectionName());

		String result = String.format(CONN_PROTO, info.toJson());
		return result;
	}

	protected String readLine () throws IOException {
		BufferedReader br = conn.getBufferedInputStreamReader();

		logger.trace("readLine() Reading from input stream");
		String s = br.readLine();
		if (s==null)
			throw new EOFException("Connection closed");
		logger.trace("<= {}", s != null ? s.trim() : "null");
		return s;
	}

	protected Control readOp() throws IOException {
		// This is only used when creating a connection, so simplify
		// life and just create a BufferedReader to read the incoming
		// info string.		
		//
		// Do not close the BufferedReader - ket TCPConnection manage it.
		//		BufferedReader br = conn.getBufferedInputStreamReader();
		//
		//		logger.trace("readOp() Reading from input stream");
		//		String s = br.readLine();
		String s = readLine();
		//		logger.trace("<= {}", s != null ? s.trim() : "null");
		Control c = new Control(s);
		logger.trace("readOp returning: " + c);
		return c;
	}

	// waitForExits will wait for all socket watcher Go routines to
	// be shutdown before proceeding.
	private void waitForExits()
	{
		// Kick old flusher forcefully.
		setFlusherDone(true);
		kickFlusher();

		for (Thread t : socketWatchers) {
			try {
				t.join();
			} catch (InterruptedException e) {}
		}

		//        taskExec.shutdown();
		//        while (!taskExec.isTerminated()) {
		//        	
		//        }
		//        try {
		//			taskExec.awaitTermination(10, TimeUnit.SECONDS);
		//		} catch (InterruptedException e) {
		//		}
		//        if (taskExec   wg.Count > 0)
		//        {
		//            try
		//            {
		//                Task.WaitAll(this.wg.ToArray());
		//            }
		//            catch (Exception) { }
		//        }
	}

	protected void spinUpSocketWatchers() {
		// Make sure everything has exited.
		waitForExits();

		// We will wait on both going forward.
		// nc.wg.Add(2)

		//taskExec.submit(new ReadLoopTask(this));  
		//taskExec.submit(new Flusher(this));
		Thread t = null;

		t = new NATSThread(new ReadLoopTask(), "readloop");
		t.start();
		socketWatchers.add(t);

		t = new NATSThread(new Flusher(), "flusher");
		t.start();
		socketWatchers.add(t);

		mu.lock();
		try {
			if (opts.getPingInterval() > 0) {
				if (ptmr != null)
					ptmr.shutdownNow();

				ptmr = Executors.newSingleThreadScheduledExecutor(new NATSThreadFactory("pinger"));
				ptmr.scheduleAtFixedRate(
						new PingTimerEventHandler(), opts.getPingInterval(), opts.getPingInterval(), 
						TimeUnit.MILLISECONDS);
			}
		} finally {
			mu.unlock();
		}
	}

	protected class Control {
		String op = null;
		String args = null;

		protected Control(String s) {
			if (s==null)
				return;

			String[] parts = s.split(" ", 2);

			switch (parts.length) {
			case 1:
				op = parts[0].trim();
				break;
			case 2:
				op = parts[0].trim();
				args = parts[1].trim();
				if (args.isEmpty())
					args=null;
				break;
			default:
			}
		}

		public String toString() {
			return "{op=" + op + ", args=" + args + "}";
		}
	}


	class ConnectInfo {
		private Boolean verbose;
		private Boolean pedantic;
		private String user;
		private String pass;
		private Boolean ssl;
		private String name;
		private String lang = ConnectionImpl.LANG_STRING;
		private String version = ConnectionImpl.this.version;

		public ConnectInfo(boolean verbose, boolean pedantic, String username, String password, boolean secure,
				String connectionName) {
			this.verbose = new Boolean(verbose);
			this.pedantic = new Boolean(pedantic);
			this.user = username;
			this.pass = password;
			this.ssl = new Boolean(secure);
			this.name = connectionName;
		}

		public String toJson() {
			StringBuilder sb = new StringBuilder(1024);

			sb.append(String.format("{\"verbose\":%s,\"pedantic\":%s,", 
					verbose.toString(), pedantic.toString()));
			if (user != null) {
				sb.append(String.format( "\"user\":\"%s\",",user));
				if (pass != null) {
					sb.append(String.format( "\"pass\":\"%s\",",pass));
				}
			}
			sb.append(String.format("\"ssl_required\":%s,\"name\":\"%s\",\"lang\":\"%s\",\"version\":\"%s\"}", 
					ssl.toString(), (name != null) ? name : "", lang, version
					));

			return sb.toString();
		}
	}

	class Srv {
		URI url = null;
		int reconnects = 0;
		long lastAttempt = 0L;

		protected Srv(URI url) {
			if (url != null) {
				this.url = url;
			}
		}

		// Mark the last attempt to connect to this Srv
		void updateLastAttempt() {
			lastAttempt = System.nanoTime();
		}

		// Returns time since last attempt, in msec
		long timeSinceLastAttempt() {
			return ((System.nanoTime() - lastAttempt) / 1000000);
		}
	}

	private void readLoop()
	{
		// Stack based buffer.
		byte[] buffer = new byte[DEFAULT_BUF_SIZE];
		//		Parser parser = new Parser(this);
		Parser parser = this.ps;
		int    len;
		boolean sb;

		while (true)
		{
			sb = false;
			mu.lock();
			try
			{
				sb = (isClosed() || isReconnecting());
				if (sb)
					this.ps = parser;
			} finally {
				mu.unlock();
			}

			try
			{
				len = br.read(buffer, 0, DEFAULT_BUF_SIZE);
				if (len==-1) {
					throw new StaleConnectionException("Connection read error (EOF)");
				}
				parser.parse(buffer, len);
			} catch (Exception e)
			{
				logger.trace("Exception in readLoop(): Conn.State was {}", status, e);
				if (status != ConnState.CLOSED)
				{
					processOpError(e);
				}
				break;
			}
		}

		mu.lock();
		try 
		{
			parser = null;
		} finally {
			mu.unlock();
		}
	}

	protected class ReadLoopTask implements Runnable {
		@Override
		public void run() {
			readLoop();
		}
	}

	// deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
	// It is used to deliver messages to asynchronous subscribers.
	protected void deliverMsgs(Channel<Message> ch)
	{
		//		logger.trace("In deliverMsgs");
		Message m = null;

		while (true)
		{
			mu.lock();
			try
			{
				if (isClosed())
					return;
			} finally {
				mu.unlock();
			}

			try {
				logger.trace("Calling ch.get(-1)...");
				m = (Message)ch.get(-1);
				logger.trace("ch.get(-1) returned " + m);
			} catch (TimeoutException e) {
				// This can't really happen, but...
				throw new Error(e);
			} 

			if (m == null)
			{
				// the channel has been closed, exit silently.
				return;
			}

			// Note, this seems odd message having the sub process itself, 
			// but this is good for performance.
			if (!m.sub.processMsg(m))
			{
				mu.lock();
				try
				{
					removeSub(m.sub);
				} finally {
					mu.unlock();
				}
			}
		}
	}

	protected class Flusher implements Runnable {
		@Override
		public void run() {
			flusher();
		}

	}

	protected void processMsgArgs(byte[] buffer, long length) throws ParserException
	{
		String s = new String(buffer, 0, (int)length);
		String[] args = s.split(" ");

		logger.trace("processMsgArgs() for buffer {}", s);
		switch (args.length)
		{
		case 3:
			msgArgs.subject = args[0];
			msgArgs.sid     = Long.parseLong(args[1]);
			msgArgs.reply   = null;
			msgArgs.size    = Integer.parseInt(args[2]);
			break;
		case 4:
			msgArgs.subject = args[0];
			msgArgs.sid     = Long.parseLong(args[1]);
			msgArgs.reply   = args[2];
			msgArgs.size    = Integer.parseInt(args[3]);
			break;
		default:
			throw new ParserException("Unable to parse message arguments: " + s);
		}

		if (msgArgs.size < 0)
		{
			throw new ParserException("Invalid Message - Bad or Missing Size: " + s);
		} else { 
			// OK
		}
	}

	// processMsg is called by parse and will place the msg on the
	// appropriate channel for processing. All subscribers have their
	// their own channel. If the channel is full, the connection is
	// considered a slow subscriber.
	protected void processMsg(byte[] data, long length)
	{
		//		logger.trace("In ConnectionImpl.processMsg(), msg length = {}. msg bytes = [{}]", length, msg);
		//		logger.trace("msg = [{}]", new String(msg));
		//		logger.trace("In ConnectionImpl.processMsg(), msg length = {}. msg = [{}]", length, 
		//				new String(msg));

		boolean maxReached = false;
		SubscriptionImpl s;

		mu.lock();
		try
		{
			stats.incrementInMsgs();
			stats.incrementInBytes(length);

			if (subs.containsKey(msgArgs.sid)) {
				s = subs.get(msgArgs.sid);
				logger.trace("\tfound subscription sid: {} subj: {}",
						s.getSid(), s.getSubject() );
			} else {
				//				String reason = String.format("Subscription id %d not found.", msgArgs.sid);
				//				lastEx = new BadSubscriptionException(reason);
				return;
			}

			s.mu.lock();
			try
			{
				maxReached = s.tallyMessage(length);
				if (!maxReached) {
					Message m = new Message(msgArgs, s, data, length);
					logger.trace("\tcreated message:{} (length:{})", m, m.getData().length);
					if (!s.addMessage(m)) {
						processSlowConsumer(s);
					}
					else {
						logger.trace("\tadded message to channel: " + m);
					}
				} // maxreached == false
			} // lock s.mu
			finally {
				s.mu.unlock();
			}
		} // lock conn.mu
		finally
		{
			mu.unlock();
		}
		if (maxReached)
			removeSub(s);
	}

	void removeSub(Subscription s) {
//		SubscriptionImpl si = (SubscriptionImpl)s;
//		si.mu.lock();
		long key = s.getSid();
		if (subs.containsKey(key))
		{
			subs.remove(key);
			logger.trace("Removed sid={} subj={}", 
					s.getSid(), s.getSubject());
		}
		SubscriptionImpl sub = (SubscriptionImpl) s;

		if (sub.getChannel() != null)
		{
			logger.trace("Closed sid={} subj={}", s.getSid(), 
					s.getSubject());
			sub.closeChannel();
			sub.setChannel(null);
		}

		sub.setConnection(null);
	}

	// processSlowConsumer will set SlowConsumer state and fire the
	// async error handler if registered.
	void processSlowConsumer(SubscriptionImpl s)
	{
		logger.trace("processSlowConsumer() subj={}",
				s.subject
				);
		lastEx = new SlowConsumerException();

		if (this.exceptionHandler != null && !s.sc)
		{
			//TODO fix this; reconcile opts with Connection params
			taskExec.submit(new ExceptionHandlerTask(this.exceptionHandler, this,
					s, new SlowConsumerException()));
		}
		s.sc = true;
	}

	private class PingTimerEventHandler implements Runnable {

		@Override
		public void run() {
			mu.lock();
			try
			{
				if (status != ConnState.CONNECTED)
					return;

				pout++;
				if (pout <= opts.getMaxPingsOut())
				{
					logger.trace("Sending PING after {} seconds.",  
							TimeUnit.MILLISECONDS.toSeconds(opts.getPingInterval()));
					sendPing(null);
					return;
				}
			} finally {
				mu.unlock();
			}
			// We didn't successfully ping, so signal a stale connection
			processOpError(new StaleConnectionException());
		} // run()
	}

	// FIXME: This is a hack
	// removeFlushEntry is needed when we need to discard queued up responses
	// for our pings as part of a flush call. This happens when we have a flush
	// call outstanding and we call close.
	private boolean removeFlushEntry(Channel<Boolean> chan)
	{
		if (pongs.isEmpty())
			return false;

		Channel<Boolean> start = pongs.poll();
		Channel<Boolean> c = start;

		while (true)
		{
			if (c == chan)
			{
				return true;
			}
			else
			{
				pongs.add(c);
			}

			c = pongs.poll();

			if (c == start)
				break;
		}

		return false;
	}

	// The caller must lock this method.
	protected void sendPing(Channel<Boolean> ch)
	{
		if (ch != null)
			pongs.add(ch);

		try {
			bw.write(pingProtoBytes, 0, pingProtoBytesLen);
			logger.trace("=> {}", new String(pingProtoBytes).trim() );
			bw.flush();
		} catch (IOException e) {
			lastEx = e;
			logger.error("Could not send PING", e);
		}
	}

	// unsubscribe performs the low level unsubscribe to the server.
	// Use SubscriptionImpl.unsubscribe()
	protected void unsubscribe(Subscription sub, int max) 
			throws IllegalStateException, IOException 
	{
		mu.lock();
		try
		{
			if (isClosed())
				throw new ConnectionClosedException("Connection has been closed.");

			Subscription s;
			if (subs.containsKey(sub.getSid()))
			{
				s = subs.get(sub.getSid());
			}
			else
			{
				// already unsubscribed
				return;
			}

			if (max > 0)
			{
				s.setMax(max);
			}
			else
			{
				removeSub(s);
			}

			// We will send all subscriptions when reconnecting
			// so that we can suppress here.
			if (!isReconnecting()) {
				String str = String.format(UNSUB_PROTO, s.getSid(), max);
				byte[] unsub = str.getBytes(Charset.forName("UTF-8"));
				bw.write(unsub);
				logger.trace("=> {}", str.trim() );
			}

		} finally {
			mu.unlock();
		}

		kickFlusher();
	}

	protected void kickFlusher()
	{
		flusherLock.lock();
		try
		{
			if (!flusherKicked) {
				flusherKickedCondition.signal();
			}
			flusherKicked = true;
		} finally {
			flusherLock.unlock();
		}
	}

	private boolean waitForFlusherKick()
	{
		flusherLock.lock();
		try 
		{
			if (flusherDone)
				return false;

			// if kicked before we get here meantime, skip
			// waiting.
			if (!flusherKicked)
				flusherKickedCondition.await();

			flusherKicked = false;
		} catch (InterruptedException e) {
		} finally {
			flusherLock.unlock();
		}

		return true;
	}

	private void setFlusherDone(boolean value)
	{
		flusherLock.lock();
		try
		{
			flusherDone = value;

			if (flusherDone)
				kickFlusher();
		} finally {
			flusherLock.unlock();
		}
	}

	private boolean isFlusherDone()
	{
		flusherLock.lock();
		try
		{
			return flusherDone;
		} finally {
			flusherLock.unlock();
		}
	}

	// flusher is a separate task that will process flush requests for the write
	// buffer. This allows coalescing of writes to the underlying socket.
	private void flusher()
	{
		setFlusherDone(false);

		// If TCP connection isn't connected, we are done
		if (!conn.isConnected())
		{
			return;
		}

		while (!isFlusherDone())
		{
			boolean val = waitForFlusherKick();

			if (val == false)
				return;

			if (mu.tryLock()) {
				try
				{
					if (!isConnected())
						return;

					bw.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					mu.unlock();
				}
			}
		}
	}

	@Override
	public void flush(int timeout) throws TimeoutException, IllegalStateException,
	Exception
	{
		if (timeout <= 0)
		{
			throw new IllegalArgumentException(
					"Timeout must be greater than 0");
		}

		Channel<Boolean> ch = new Channel<Boolean>(1);
		mu.lock();
		try
		{
			if (isClosed()) {
				throw new ConnectionClosedException();
			}

			sendPing(ch);
		} finally {
			mu.unlock();
		}

		try {
			boolean rv = ch.get(timeout);
			if (!rv)
			{
				lastEx = new ConnectionClosedException();			
			}
		} catch (TimeoutException te) {
			lastEx = new TimeoutException("Flush channel timeout.");
		}

		if (lastEx != null)
		{
			removeFlushEntry(ch);
			throw lastEx;
		}
	}

	/// Flush will perform a round trip to the server and return when it
	/// receives the internal reply.
	@Override
	public void flush() throws TimeoutException, IllegalStateException, Exception
	{
		// 60 second default.
		flush(60000);
	}

	// resendSubscriptions will send our subscription state back to the
	// server. Used in reconnects
	private void resendSubscriptions() throws IOException
	{
		for (Long key : subs.keySet())
		{
			SubscriptionImpl s = subs.get(key);
			if (s instanceof AsyncSubscription)
				((AsyncSubscriptionImpl)s).enable(); //enableAsyncProcessing()
			logger.trace("Resending subscriptions:");
			sendSubscriptionMessage(s);
		}

		bw.flush();
	}

	class DisconnectedEventHandlerTask implements Runnable {

		private final DisconnectedEventHandler cb;
		private final ConnectionEvent connEvent;

		DisconnectedEventHandlerTask(DisconnectedEventHandler cb, ConnectionEvent eventArgs) {
			this.cb = cb;
			this.connEvent = eventArgs;
		}

		public void run() {
			if (cb==null) logger.trace("disconnectCB is null");
			cb.onDisconnect(connEvent);
		}

	}

	class ExceptionHandlerTask implements Runnable {
		private final ExceptionHandler cb;
		private final Connection conn;
		private final Subscription sub;
		private final Throwable ex;

		ExceptionHandlerTask(ExceptionHandler exceptionHandler, ConnectionImpl conn, Subscription sub, 
				Throwable ex) {
			this.cb = exceptionHandler;
			this.conn = conn;
			this.sub = sub;
			this.ex = ex;
		}

		public void run() {
			cb.onException(this.conn, this.sub, this.ex);
		}
	}

	public Subscription subscribe(String subj, String queue, MessageHandler cb) 
			throws ConnectionClosedException {
		SubscriptionImpl sub = null;
		boolean async = (cb != null);
		mu.lock();
		try {
			if (isClosed()) {
				throw new ConnectionClosedException();
			}
			if (async)
				sub = new AsyncSubscriptionImpl(this, subj, queue, cb, opts.getSubChanLen());
			else
				sub = new SyncSubscriptionImpl(this, subj, queue, opts.getSubChanLen());

			addSubscription(sub);

			if (!isReconnecting()) {
				if (async)
					((AsyncSubscriptionImpl)sub).start();
				else
					sendSubscriptionMessage(sub);
			}

		} finally {
			mu.unlock();
		}
		kickFlusher();
		return sub;
	}

	@Override
	public AsyncSubscription subscribeAsync(String subject, String queue,
			MessageHandler handler) throws ConnectionClosedException
	{
		AsyncSubscription s = null;

		mu.lock();
		try
		{
			if (isClosed())
				throw new ConnectionClosedException();
			
			s = new AsyncSubscriptionImpl(this, subject, queue, null, opts.getSubChanLen());

			addSubscription((SubscriptionImpl)s);

			if (handler != null)
			{
				s.setMessageHandler(handler);
				try {
					s.start();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} finally {
			mu.unlock();
		}

		return s;
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj, String queue) throws ConnectionClosedException {
		return subscribeAsync(subj, queue, null);
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj, MessageHandler cb) throws ConnectionClosedException {
		return subscribeAsync(subj, null, cb);
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj) 
			throws ConnectionClosedException {
		return subscribeAsync(subj, null, null);
	}

	private void addSubscription(SubscriptionImpl s) {
		s.setSid(sidCounter.incrementAndGet());
		subs.put(s.getSid(), s);
		logger.trace("Successfully added subscription to {} [{}]", 
				s.getSubject(), s.getSid());
		//		if (logger.isDebugEnabled())
		//			printSubs(this);
	}

	@Override
	public AsyncSubscription subscribe(String subject, MessageHandler cb) throws ConnectionClosedException {
		return (AsyncSubscription) subscribe(subject, "", cb);
	}

	@Override 
	public SyncSubscription subscribeSync(String subject, String queue) throws ConnectionClosedException {
		return (SyncSubscription)subscribe(subject, queue, (MessageHandler)null);
	}

	@Override 
	public SyncSubscription subscribeSync(String subject) throws ConnectionClosedException {
		return (SyncSubscription)subscribe(subject, "", (MessageHandler)null);
	}

	@Override
	public Subscription QueueSubscribe(String subject, String queue, MessageHandler cb) throws ConnectionClosedException {
		return subscribe(subject, queue, cb);
	}

	@Override
	public SyncSubscription QueueSubscribeSync(String subject, String queue) throws ConnectionClosedException {
		return (SyncSubscription) subscribe(subject, queue, (MessageHandler)null);
	}

	// Use low level primitives to build the protocol for the publish
	// message.
	private int writePublishProto(byte[] dst, String subject, String reply, int msgSize)
	{
		//		logger.info("subject={}, Reply={}, msgSize={}, arraySize={}, msgBytes={}", 
		//				subject, reply, msgSize, dst.length, dst);
		// skip past the predefined "PUB "
		int index = pubPrimBytesLen;

		// Subject
		index = Utilities.stringToBytesASCII(dst, index, subject);

		if (reply != null)
		{
			// " REPLY"
			dst[index] = (byte)' ';
			index++;

			index = Utilities.stringToBytesASCII(dst, index, reply);
		}

		// " "
		dst[index] = (byte)' ';
		index++;

		// " SIZE"
		index = Utilities.stringToBytesASCII(dst, index, Integer.toString(msgSize));

		// "\r\n"
		System.arraycopy(crlfProtoBytes, 0, dst, index, crlfProtoBytesLen);
		index += crlfProtoBytesLen;

		return index;
	}


	//	// Roll our own fast conversion - we know it's the right
	//	// encoding. 
	//	char[] convertToStrBuf = new char[MAX_CONTROL_LINE_SIZE];
	//
	//	// Caller must ensure thread safety.
	//	private String convertToString(byte[] buffer, int length)
	//	{
	//		// expand if necessary
	//		if (length > convertToStrBuf.length)
	//		{
	//			convertToStrBuf = new char[length];
	//		}
	//
	//		for (int i = 0; i < length; i++)
	//		{
	//			convertToStrBuf[i] = (char)buffer[i];
	//		}
	//
	//		// This is the copy operation for msg arg strings.
	//		return new String(convertToStrBuf, 0, (int)length);
	//	}


	// publish is the internal function to publish messages to a gnatsd.
	// Sends a protocol data message by queueing into the bufio writer
	// and kicking the flush go routine. These writes should be protected.
	@Override
	public void publish(String subject, String reply, byte[] data) throws IllegalStateException
	{
		if ((subject==null) || subject.isEmpty())
		{
			throw new BadSubjectException(
					"Subject cannot be null, empty, or whitespace.");
		}

		int msgSize = (data != null) ? data.length : 0;
		//		logger.trace("publish(): acquiring mutex");
		mu.lock();
		//		logger.trace("publish(): acquired mutex");
		try
		{
			// Proactively reject payloads over the threshold set by server.
			if (msgSize > info.getMaxPayload())
				throw new MaxPayloadException("Message payload size (" 
						+ msgSize + ") larger than maxPayloadSize: " 
						+ info.getMaxPayload());

			if (isClosed())
				throw new ConnectionClosedException();

			//TODO Throw the right exception here
			//            if (lastEx != null)
			//                throw lastEx;

			int pubProtoLen;
			// write our pubProtoBuf buffer to the buffered writer.
			try
			{
				pubProtoLen = writePublishProto(pubProtoBuf, subject,
						reply, msgSize);
			}
			catch (IndexOutOfBoundsException e)
			{
				// We can get here if we have very large subjects.
				// Expand with some room to spare.
				int resizeAmount = MAX_CONTROL_LINE_SIZE + subject.length()
				+ (reply != null ? reply.length() : 0);

				buildPublishProtocolBuffer(resizeAmount);

				pubProtoLen = writePublishProto(pubProtoBuf, subject,
						reply, msgSize);
			}

			try {
				bw.write(pubProtoBuf, 0, pubProtoLen);
				logger.trace("=> {}", new String(pubProtoBuf).trim() );

				if (msgSize > 0)
				{
					bw.write(data, 0, msgSize);
					logger.trace("=> {}", new String(data, 0, msgSize).trim() );
				}

				bw.write(crlfProtoBytes, 0, crlfProtoBytesLen);

				stats.incrementOutMsgs();
				stats.incrementOutBytes(msgSize);

			} catch (IOException e) {
				//TODO remove this
				e.printStackTrace();
			}

		} finally {
			mu.unlock();
			//			logger.trace("publish(): released mutex");
		}
		kickFlusher();

	} // publish

	@Override
	public void publish(String subject, byte[] data) throws IllegalStateException
	{
		publish(subject, null, data);
	}

	@Override
	public void publish(Message msg) throws IllegalStateException
	{
		publish(msg.getSubject(), msg.getReplyTo(), msg.getData());
	}

	private Message _request(String subject, byte[] data, long timeout)
			throws TimeoutException, IllegalStateException, IOException 
	{
		Message m		= null;

		String inbox = newInbox();

		SyncSubscription s = subscribeSync(inbox, null);
		s.autoUnsubscribe(1);

		publish(subject, inbox, data);

		m = s.nextMessage(timeout);

		try
		{
			// the auto unsubscribe should handle this.
			s.unsubscribe();
		}
		catch (Exception e) {  /* NOOP */ }

		return m;		
	}

	@Override
	public Message request(String subject, byte[] data, long timeout) 
			throws TimeoutException, IOException {
		logger.trace("#########In request({},{},{})", subject, 
				data==null?"null":new String(data), timeout);
		if (timeout <= 0)
		{
			throw new IllegalArgumentException(
					"Timeout must be greater that 0.");
		}

		return _request(subject, data, timeout);
	}

	@Override
	public Message request(String subject, byte[] data) 
			throws TimeoutException, IOException {

		return _request(subject, data, -1);
	}

	@Override
	public String newInbox() {
		if (r == null)
			r = new Random();

		byte[] buf = new byte[13];

		r.nextBytes(buf);
		String s1 = inboxPrefix;
		String s2 = Utilities.bytesToHex(buf);
		return s1.concat(s2);
	}

	@Override
	public synchronized Statistics getStats() {
		return new Statistics(stats);
	}

	@Override
	public synchronized void resetStats() {
		stats.clear();
	}

	@Override
	public synchronized long getMaxPayload() {
		return info.getMaxPayload();
	}

	protected void sendSubscriptionMessage(Subscription sub) {
		mu.lock();
		try
		{
			// We will send these for all subs when we reconnect
			// so that we can suppress here.
			if (!isReconnecting())
			{
				String s = String.format(SUB_PROTO, sub.getSubject(), 
						sub.getQueue(), sub.getSid());
				try {
					bw.write(Utilities.stringToBytesASCII(s));
					logger.trace("=> {}", s.trim() );
					kickFlusher();
				} catch (IOException e) {
				}
			}
		} finally {
			mu.unlock();
		}
	}

	/**
	 * @return the closedEventHandler
	 */
	@Override
	public ClosedEventHandler getClosedEventHandler() {
		return opts.getClosedEventHandler();
	}

	/**
	 * @param closedEventHandler the closedEventHandler to set
	 */
	@Override
	public void setClosedEventHandler(ClosedEventHandler closedEventHandler) {
		opts.setClosedEventHandler(closedEventHandler);
	}

	/**
	 * @return the disconnectedEventHandler
	 */
	@Override
	public DisconnectedEventHandler getDisconnectedEventHandler() {
		return opts.getDisconnectedEventHandler();
	}

	/**
	 * @param disconnectedEventHandler the disconnectedEventHandler to set
	 */
	@Override
	public void setDisconnectedEventHandler(DisconnectedEventHandler disconnectedEventHandler) {
		opts.setDisconnectedEventHandler(disconnectedEventHandler);
	}

	/**
	 * @return the reconnectedEventHandler
	 */
	@Override
	public ReconnectedEventHandler getReconnectedEventHandler() {
		return opts.getReconnectedEventHandler();
	}

	/**
	 * @param reconnectedEventHandler the reconnectedEventHandler to set
	 */
	@Override
	public void setReconnectedEventHandler(ReconnectedEventHandler reconnectedEventHandler) {
		opts.setReconnectedEventHandler(reconnectedEventHandler);
	}

	/**
	 * @return the exceptionHandler
	 */
	@Override
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	/**
	 * @param exceptionHandler the exceptionHandler to set
	 */
	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public String getConnectedUrl()
	{
		mu.lock();
		try
		{
			if (status != ConnState.CONNECTED)
				return null;

			return url.toString();
		}
		finally
		{
			mu.unlock();
		}
	}

	@Override
	public String getConnectedId()
	{
		mu.lock();
		try
		{
			if (status != ConnState.CONNECTED)
				return null;

			return info.getId();
		}
		finally
		{
			mu.unlock();
		}
	}

	@Override 
	public ConnState getState()
	{
		mu.lock();
		try {
			return this.status;
		} finally {
			mu.unlock();
		}
	}
	@Override
	public void finalize(){
		try {
			this.close();
		} catch (Exception e) {

		}
	}

	@Override
	public ServerInfo getConnectedServerInfo() {
		return this.info;
	}

	void setConnectedServerInfo(ServerInfo info) {
		this.info = info;
	}
	
	void setConnectedServerInfo(String info) {
		processInfo(info);
	}

	@Override
	public Exception getLastError() {
		return lastEx;
	}

	static void printSubs(ConnectionImpl c) {
		c.logger.trace("SUBS:");
		for (long key : c.subs.keySet())
		{
			c.logger.trace("\tkey: " + key + " value: " + c.subs.get(key));
		}
	}

	protected void sleepMsec(long msec) {
		try {
			Thread.sleep(msec);
		} catch (InterruptedException e) {
			// NOOP
		}
	}
	
	void setOutputStream(OutputStream out)
	{
		mu.lock();
		try {
			this.bw = out;
		} finally {
			mu.unlock();
		}
	}

}
