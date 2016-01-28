/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static io.nats.client.Constants.*;
import static io.nats.client.Constants.ConnState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionImpl implements Connection {
	final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

	String version = null;

	//	final static int DEFAULT_SCRATCH_SIZE = 512;	

	private static final String inboxPrefix = "_INBOX.";

	public ConnState status = ConnState.DISCONNECTED;

	protected final static String STALE_CONNECTION     = "Stale Connection";
	protected final static String THREAD_POOL = "natsthreadpool";
	// Client version
//	protected static final String 	VERSION					= "1.0.0";

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
	public static final String _OK_OP_   = "+OK";
	public static final String _ERR_OP_  = "-ERR";
	public static final String _MSG_OP_  = "MSG";
	public static final String _PING_OP_ = "PING";
	public static final String _PONG_OP_ = "PONG";
	public static final String _INFO_OP_ = "INFO";

	// Message Prototypes
	public static final String CONN_PROTO  = "CONNECT %s" + _CRLF_;
	public static final String PING_PROTO  = "PING" + _CRLF_;
	public static final String PONG_PROTO  = "PONG" + _CRLF_;
	public static final String PUB_PROTO   = "PUB %s %s %d" + _CRLF_;
	public static final String SUB_PROTO   = "SUB %s%s %d" + _CRLF_;
	public static final String UNSUB_PROTO = "UNSUB %d %s" + _CRLF_;
	public static final String OK_PROTO    = _OK_OP_ + _CRLF_;


	//	private ConnectionImpl nc 				= this;
	protected final Lock mu 					= new ReentrantLock();
//	protected final Lock mu 					= new AlternateDeadlockDetectingLock(true, true);

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

	private BufferedOutputStream bw 		= null;
	private BufferedInputStream br 			= null;
	private ByteArrayOutputStream pending 	= null;

	private ReentrantLock flusherLock		= new ReentrantLock();
	private boolean flusherDone     		= false;

	private Map<Long, SubscriptionImpl> subs = new ConcurrentHashMap<Long, SubscriptionImpl>();
	protected List<Srv> srvPool 				= null;
	private Exception lastEx 				= null;
	private ServerInfo info 				= null;
//	private Vector<Thread> socketWatchers	= new Vector<Thread>();
	private int pout;

	// Initialized in readLoop
	protected Parser parser					= new Parser(this);
	protected Parser.ParseState ps			= null;

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
	private Queue<Channel<Boolean>> pongs 	=  new LinkedBlockingQueue<Channel<Boolean>>();;

	private ExecutorService executor 		= Executors.newCachedThreadPool(new NATSThreadFactory(THREAD_POOL));
//	private ExecutorService reconexecutor 	= null;
	private ScheduledExecutorService ptmr 	= null;
	private Random r						= null;
	private Phaser phaser					= new Phaser();
//	private Phaser ioPhaser					= new Phaser();
	private Channel<Boolean> fch			= new Channel<Boolean>();

	ConnectionImpl() {
		
	}
	
	ConnectionImpl(Options o)
	{
		this(o, null);
	}

	ConnectionImpl(Options o, TCPConnection tcpconn) {
		Properties props = this.getProperties("jnats.properties");
		version = props.getProperty("client.version");

		this.opts = o;
		this.stats = new Statistics();
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
		pubPrimBytes = _PUB_P_.getBytes();
		pubPrimBytesLen = pubPrimBytes.length;

		crlfProtoBytes = _CRLF_.getBytes();
		crlfProtoBytesLen = crlfProtoBytes.length;

		// predefine the start of the publish protocol message.
		buildPublishProtocolBuffer(Parser.MAX_CONTROL_LINE_SIZE);

		setupServerPool();
	}

	private void setup() {
		subs.clear();
		pongs.clear();
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

		if (servers != null) {
			if (!opts.isNoRandomize()) {
				// Randomize the order
				Collections.shuffle(servers, new Random(System.nanoTime()));
			}
			
			for (URI s : servers) {
				srvPool.add(new Srv(s));
			}
		}

		if (srvPool.isEmpty()) {
			srvPool.add(new Srv(URI.create(DEFAULT_URL)));
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

	protected Srv selectNextServer() throws IOException {
		logger.trace("In selectNextServer()");
		Srv s = currentServer();
		if (s==null) {
			throw new IOException(ERR_NO_SERVERS);
		}
		/**
		 * Pop the current server and put onto the end of the list. Select head of list as long
		 * as number of reconnect attempts under MaxReconnect.
		 */
		logger.trace("selectNextServer, removing {}", s);
		srvPool.remove(s);

		/**
		 * if the maxReconnect is unlimited, or the number of reconnect attempts
		 * is less than maxReconnect, move the current server to the end of the
		 * list.
		 * 
		 */
		int maxReconnect = opts.getMaxReconnect();
		if ((maxReconnect < 0) || (s.reconnects < maxReconnect)) {
			logger.trace("selectNextServer: maxReconnect: {}", maxReconnect);
			logger.trace("selectNextServer adding {}", s);
			srvPool.add(s);
		}

		if (srvPool.isEmpty()) {
			this.url = null;
			throw new IOException(ERR_NO_SERVERS);
		}

		this.url = srvPool.get(0).url;
		return srvPool.get(0);
	}

	protected void connect() throws IOException, TimeoutException {
		//	protected void connect() throws Exception {
		// Create actual socket connection
		// For first connect we walk all servers in the pool and try
		// to connect immediately.
		//		boolean connected = false;

		mu.lock();
		try {
			for (Srv s : srvPool) {
				this.url = s.url;

				try {
					logger.debug("Connecting to {}", this.url);
					createConn();
					logger.debug("Connected to {}", this.url);
					this.setup();
					try {
						processConnectInit();	
						logger.trace("connect() Resetting reconnects for {}", s);
						s.reconnects = 0;
						break;
					} catch (IOException e) {
						logger.trace("{} Exception: {}", this.url, e.getMessage());
//						if (e.getMessage()==null) {
//							setLastError(new IOException(ERR_CONNECTION_READ, e));
//						}
//						else {
							setLastError(e);
//						}
						mu.unlock();
						close(ConnState.DISCONNECTED, false);
						mu.lock();
						this.url = null;
					}
				} catch (IOException e) { // createConn failed
					if (e instanceof SocketException) {
						if (e.getMessage()!=null) {
							if (e.getMessage().contains("Connection refused")) {
								setLastError(null);
							}
						}
					}
				} 
			} // for
			if ((getLastException() == null) && (this.status != ConnState.CONNECTED)) {
				setLastError(new IOException(ERR_NO_SERVERS));
			}
			Exception e = getLastException();
			if (e != null) {
				if (e instanceof IOException) 
					throw((IOException)e);	
				else if (e instanceof TimeoutException)
					throw((TimeoutException)e);
				else
					throw new Error("Unexpected error", e);
			}
		} finally {
			mu.unlock();
		}
	}

	// createConn will connect to the server and wrap the appropriate
	// bufio structures. It will do the right thing when an existing
	// connection is in place.
	protected void createConn() throws IOException {
		Srv s = currentServer();
		if (s == null)
			throw new IOException(ERR_NO_SERVERS);
		else
			s.updateLastAttempt();

		logger.trace("createConn(): {}", s.url);

		try {
			logger.trace("Opening {}", s.url);
			if (conn == null)
				conn = new TCPConnection();
			conn.open(s.url.getHost(), s.url.getPort(), opts.getConnectionTimeout());
			logger.trace("Opened {}", s.url);
		} catch (IOException e) {
			logger.trace("Couldn't establish connection to {}: {}", s.url, e.getMessage());
			throw(e);
		}

		if ((pending != null) && (bw != null)) {
			logger.trace("Flushing old outputstream to pending");
			try { bw.flush(); } catch (Exception e) { /* NOOP */}
		}
		bw = conn.getBufferedOutputStream(DEFAULT_STREAM_BUF_SIZE);
		br = conn.getBufferedInputStream(DEFAULT_STREAM_BUF_SIZE);
	}

	// This will clear any pending flush calls and release pending calls.
	private void clearPendingFlushCalls()
	{
		logger.trace("clearPendingFlushCalls()");
		mu.lock();
		try
		{
			logger.trace("clearPendingFlushCalls(): acquired lock");
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
	private void close(ConnState closeState, boolean doCBs)
	{
		logger.trace("close({}, {})", closeState, String.valueOf(doCBs));
		final ConnectionImpl nc = this;

		logger.trace("Acquiring lock");
		mu.lock();
		logger.trace("Acquired lock");
		if (isClosed())
		{
			logger.trace("ALREADY CLOSED, WE'RE DONE");
			this.status = closeState;
			mu.unlock();
			return;
		}
		this.status = ConnState.CLOSED;

		// Kick the Flusher routines so they fall out.
		logger.trace("Kicking flusher");
		kickFlusher();
		mu.unlock();
		logger.trace("Kicked flusher");
		
		// Clear any queued pongs, e.g. pending flush calls.
		clearPendingFlushCalls();

		mu.lock();
		if (ptmr != null) {
			ptmr.shutdownNow();
		}

		if (conn != null) {
			try { bw.flush(); } catch (IOException e) { /* NOOP */ }
		}

		logger.trace("Closing subscriptions");
		// Close sync subscriber channels and release any
		// pending nextMsg() calls.
		for (Long key : subs.keySet())
		{
			SubscriptionImpl s = subs.get(key);
			s.mu.lock();
			s.closeChannel();
			// Mark as invalid, for signaling to deliverMsgs
			s.closed = true;
			// Mark connection closed in subscription 
			s.connClosed = true;
			s.mu.unlock();
		}
		subs.clear();

		// perform appropriate callback if needed for a
		// disconnect;
		final DisconnectedCallback dcb = opts.getDisconnectedCallback();
//		logger.trace("Preparing to call DisconnectedCallback, doCBs={}, conn.isSetup()={}, dcb={}",
//				doCBs, conn == null?"null":conn, 
//				dcb == null?"null":dcb);
		if (doCBs && conn != null && dcb != null)
		{
			logger.trace("Calling disconnectedCB from Connection.close()");
			executor.submit(new Runnable() {
				public void run() {
					Thread.currentThread().setName("disconnect-callback");
					dcb.onDisconnect(new ConnectionEvent(nc));
				}
			});
			logger.trace("Spawned disconnectedCB from Connection.close()");
		}

		ClosedCallback ccb = opts.getClosedCallback();
		mu.unlock();
		// Perform appropriate callback if needed for a connection closed.
		if (doCBs && ccb != null)
		{
			logger.trace("In close(), calling ccb, doCBs={}, ccb={}", doCBs, ccb);
			ccb.onClose(new ConnectionEvent(this));
			logger.trace("In close(), ccb executed");
		}

//		if (executor != null) {
//			executor.shutdownNow();
//			if (executor.isTerminated())
//				logger.trace("Thread executor has been shut down");
//		}

		mu.lock();
		try {
			this.status = closeState;
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
			}
			mu.unlock();
		}
	}

	protected void processConnectInit() throws IOException 
	{
		logger.trace("processConnectInit(): {}", this.url);

		// Set our status to connecting.
		status = ConnState.CONNECTING;

		// Process the INFO protocol that we should be receiving
		processExpectedInfo();

		// Send the CONNECT and PING protocol, and wait for the PONG.
		sendConnect();

		// Reset the number of PINGs sent out
		this.pout = 0;

		// Start the readLoop and flusher threads
		spinUpSocketWatchers();
//		go(new Runnable() {
//			public void run() {
//				Thread.currentThread().setName("spinup");
//				spinUpSocketWatchers();
//			}
//		}, "spinup", "ioPhaser", ioPhaser, true);
		
	}

	// This will check to see if the connection should be
	// secure. This can be dictated from either end and should
	// only be called after the INIT protocol has been received.
	private void checkForSecure() throws IOException
	{
		// Check to see if we need to engage TLS
		// Check for mismatch in setups
		if (opts.isSecure() && !info.isTlsRequired())
		{
			throw new IOException(ERR_SECURE_CONN_WANTED);
		}
		else if (info.isTlsRequired() && !opts.isSecure())
		{
			throw new IOException(ERR_SECURE_CONN_REQUIRED);
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
		bw = conn.getBufferedOutputStream(DEFAULT_BUF_SIZE);
		br = conn.getBufferedInputStream(DEFAULT_BUF_SIZE);
	}

	protected void processExpectedInfo() throws IOException {
		Control c;

		try {
			c = readOp();
		} catch (IOException e) {
			processOpError(e);
			return;
		}

		if (!c.op.equals(_INFO_OP_)) {
			throw new IOException(ERR_PROTOCOL+", INFO not received");
		}

		processInfo(c.args);
		checkForSecure();
		
	}

	// processPing will send an immediate pong protocol response to the
	// server. The server uses this mechanism to detect dead clients.
	protected void processPing() 
	{
		try {
			sendProto(pongProtoBytes, pongProtoBytesLen);
		} catch (IOException e) {
			setLastError(e);
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

	// processOpError handles errors from reading or parsing the protocol.
	// This is where disconnect/reconnect is initially handled.
	// The lock should not be held entering this function.
	private void processOpError(Exception e) {
		logger.trace("processOpError(e={}) state={} reconnectAllowed={} ", e.getClass().getName(), status, opts.isReconnectAllowed());
		mu.lock();
		try{
			if (isConnecting() || isClosed() || isReconnecting()) {
				return;
			}

			if (opts.isReconnectAllowed() && status == ConnState.CONNECTED) {
				// Set our new status
				status = ConnState.RECONNECTING;
				
				if (ptmr != null) {
					ptmr.shutdownNow();         
				}

				if (this.conn != null) {
					try { bw.flush(); } catch (IOException e1) {}
					conn.close();
					conn = null;
				}
				logger.trace("\t\tspawning doReconnect() in state {}", status);
				go(new Runnable() {
					public void run() {
						Thread.currentThread().setName("reconnect");
						doReconnect();
					}
				}, "reconnect", "phaser", phaser, true);
				logger.trace("\t\tspawned doReconnect() in state {}", status);
				return;
			} else {
				logger.trace("\t\tcalling processDisconnect() in state {}", status);
				processDisconnect();
				setLastError(e);
				close();
			}
		} finally {
			logger.trace("processOpError() releasing lock");
			mu.unlock();
			logger.trace("processOpError() released lock");
		}
	}

	protected void processDisconnect() {
		logger.trace("processDisconnect()");
		status = ConnState.DISCONNECTED;
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
	protected void flushReconnectPendingItems()
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
		logger.trace("flushReconnectPendingItems() DONE");
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

		// Create a new pending buffer to underpin the buffered output
		// stream while we are reconnecting.
		pending = new ByteArrayOutputStream(DEFAULT_PENDING_SIZE);
		bw = new BufferedOutputStream(pending);

		// Clear any errors.
		setLastError(null);

		// Perform appropriate callback if needed for a disconnect
		if (opts.getDisconnectedCallback() != null)
		{
			mu.unlock();
			DisconnectedCallback dcb = opts.getDisconnectedCallback();
			// TODO This mirrors go, and so does not spawn a thread/task.
			logger.trace("Spawning disconnectCB from doReconnect()");
			dcb.onDisconnect(new ConnectionEvent(this));
			logger.trace("Spawned disconnectCB from doReconnect()");
			mu.lock();
		}

		while (srvPool.size()>0)
		{
			Srv cur = null;
			try {
				cur = selectNextServer();
			} catch (IOException nse){
				logger.trace("doReconnect() calling setLastError({})", nse.getMessage());
				setLastError(nse);
				break;
			}

			// Sleep appropriate amount of time before the
			// connection attempt if connecting to same server
			// we just got disconnected from.

			long elapsedMillis = cur.timeSinceLastAttempt();
			if (elapsedMillis < opts.getReconnectWait())
			{
				long sleepTime = opts.getReconnectWait() - elapsedMillis;
				mu.unlock();
				sleepMsec((int)sleepTime);
				mu.lock();
			}
			// Check if we have been closed first.			
			if (isClosed()) {
				logger.trace("Connection has been closed while in doReconnect()");
				break;
			}
			// Mark that we tried a reconnect
			cur.reconnects++;

			logger.trace("doReconnect() incremented cur.reconnects: {}", cur);
			logger.trace("doReconnect: trying createConn() for {}", cur);

			// try to create a new connection
			try {
				createConn();
				logger.trace("doReconnect: createConn() successful for {}", cur);
			} catch (Exception e) {
				logger.trace("doReconnect: createConn() failed for {}", cur);
				logger.trace("createConn failed", e);
				// not yet connected, retry and hold
				// the lock.
				setLastError(null);
				continue;
			}

			// We are reconnected.
			stats.incrementReconnects();

			// Clear out server stats for the server we connected to..

			// TODO Clean this up? It's never used in any of the clients
			// cur.didConnect = true;
			logger.trace("Successful reconnect; Resetting reconnects for {}", cur); 
			cur.reconnects = 0;

			// Process connect logic
			try
			{
				processConnectInit();
			}
			catch (IOException e)
			{
				setLastError(e);
				status = ConnState.RECONNECTING;
				continue;
			}

			// Send existing subscription state
			resendSubscriptions();

			// Now send off and clear pending buffer
			flushReconnectPendingItems();

			// Flush the buffer
			try {
				bw.flush();
			}
			catch (IOException e)
			{
				setLastError(e);
				status = ConnState.RECONNECTING;
				continue;
			}

			// Done with the pending buffer
			setPending(null);

			// This is where we are truly connected.
			status = ConnState.CONNECTED;

			// snapshot the reconnect callback while lock is held.
			ReconnectedCallback rcb = opts.getReconnectedCallback();

			// Release the lock here, we will return below
			mu.unlock();

			// Make sure to flush everything (doing this inside or outside
			// the lock seems important
			try { flush(); } catch (Exception e) { /* NOOP */ }
			
			// Call reconnectedCB if appropriate. We are already in a
			// separate thread here, so ok to call direct.
logger.trace("doReconnect checking rcb != null");			
			if (rcb != null)
			{
logger.trace("doReconnect invoking rcb");			
				rcb.onReconnect(new ConnectionEvent(this));
			}
logger.trace("doReconnect finished successfully!");			
			return;
		} // while

		logger.trace("Reconnect FAILED");			

		// Call into close.. We have no servers left.
		if (getLastException() == null)
			setLastError(new IOException(ERR_NO_SERVERS));

		// Need to close the TCPConnection here to avoid a double
		// DisconnectedCallback
		conn.close();
		conn = null;
		
		mu.unlock();
		logger.trace("Calling   close() from doReconnect()");
		close();
		logger.trace("Completed close() from doReconnect()");
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
		boolean doCBs = false;
		Exception ex = null;
		String s = new String(errorStream.array(), 0, errorStream.position());
		logger.trace("processErr(errorStream={})", s);

		if (STALE_CONNECTION.equals(s))
		{
			processOpError(new IOException(ERR_STALE_CONNECTION));
		}
		else
		{
			ex = new NATSException(s);
			mu.lock();
			try
			{
				setLastError(ex);

				if (status != ConnState.CONNECTING)
				{
					doCBs = true;
				}
			} finally {
				mu.unlock();
			}

			close(ConnState.CLOSED, doCBs);
		}
	}
	// caller must lock
	protected void sendConnect() throws IOException
	{
		String line = null;

		logger.trace("sendConnect()");

		// Write the protocol into the buffer
		bw.write(connectProto().getBytes());
		logger.trace("=> {}", connectProto().trim());

		// Add to the buffer the PING protocol
		bw.write(pingProtoBytes, 0, pingProtoBytesLen);
		logger.trace("=> {}", new String(pingProtoBytes).trim());

		// Flush the buffer
		bw.flush();
		logger.trace("=> FLUSH");

		// Now read the response from the server.
		try {
			line = readLine();
		} catch (IOException e) {
			//			this.processOpError(e);
			//			return;
			throw new IOException(ERR_CONNECTION_READ, e);
		}

		// If opts.verbose is set, Handle +OK
		if (opts.isVerbose() && line.equals(_OK_OP_))
		{
			// Read the rest now...
			try {
				line = readLine();
			} catch (IOException e) {
				//				this.processOpError(e);
				//				return;
				throw new IOException(ERR_CONNECTION_READ, e);
			}
		}

		// We expect a PONG
		if (!PONG_PROTO.trim().equals(line)) {
			// But it could be something else, like -ERR
			if (line.startsWith(_ERR_OP_))
			{
				throw new IOException("nats: " + line.substring(_ERR_OP_.length()).trim());
			} 
			String msg = line;
			if (line.startsWith("nats: ")) {
				msg = line.replace("nats: ", "");
			}
			throw new IOException("nats: " + msg);			
		}

		// This is where we are truly connected.
		status = ConnState.CONNECTED;
	}

	// This function is only used during the initial connection process
	protected String readLine () throws IOException {
		String s = null;
		logger.trace("readLine() Reading from input stream");
		BufferedReader breader = conn.getBufferedInputStreamReader();
		s = breader.readLine();
		if (s==null)
			throw new EOFException(ERR_CONNECTION_READ);
		logger.trace("<= {}", s != null ? s.trim() : "null");
		return s;
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
		String token = null;

		if (u != null) {
			// if no password, assume username is authToken
			String[] userpass = u.split(":");
			if (userpass[0].length() > 0) {
				switch (userpass.length)
				{
				case 1:
					token = userpass[0];
					break;
				case 2:
					user = userpass[0];
					pass = userpass[1];
					break;
				default:
					break;
				}
			}
		}

		ConnectInfo info = new ConnectInfo(opts.isVerbose(), opts.isPedantic(), user, pass,
				token, opts.isSecure(), opts.getConnectionName());

		String result = String.format(CONN_PROTO, info.toJson());
		return result;
	}

	protected Control readOp() throws IOException {
		// This is only used when creating a connection, so simplify
		// life and just create a BufferedReader to read the incoming
		// info string.		
		//
		// Do not close the BufferedReader; let TCPConnection manage it.
		String s = readLine();
		Control c = new Control(s);
		logger.trace("readOp returning: " + c);
		return c;
	}

	// waitForExits will wait for all socket watcher threads to
	// complete before proceeding.
	private void waitForExits()
	{
		logger.trace("waitForExits()");
		// Kick old flusher forcefully.
		setFlusherDone(true);
		kickFlusher();

		phaser.register();
		int nParties = phaser.getRegisteredParties();
		logger.trace("Num registered parties: {}", nParties);
		while (!phaser.isTerminated()) {
			// phaser.arriveAndAwaitAdvance();
			phaser.arriveAndDeregister();
		}
		logger.trace("Done waiting: Num registered parties: {}", phaser.getRegisteredParties());
	}

	void runTasks(List<Runnable> tasks) {
		final Phaser phaser = new Phaser(1); // "1" to register self
		// create and start threads
		for (final Runnable task : tasks) {
			phaser.register();
			new Thread() {
				public void run() {
					phaser.arriveAndAwaitAdvance(); // await all creation
					task.run();
					phaser.arriveAndDeregister();
				}
			}.start();
		}

		// allow threads to start and deregister self
		phaser.arriveAndDeregister();
	}

	protected void spinUpSocketWatchers() {
		logger.trace("Spinning up threads");
		// Make sure everything has exited.
		if (phaser.getPhase() != 0)
			waitForExits();
		List<Runnable> tasks = new ArrayList<Runnable>();
		
		tasks.add(new Runnable() {
			public void run() {
				logger.trace("READLOOP STARTING");
				Thread.currentThread().setName("readloop");
				readLoop();
				logger.trace("READLOOP EXITING");
			}
		});
		
		tasks.add(new Runnable() {
			public void run() {
				logger.trace("FLUSHER STARTING");
				Thread.currentThread().setName("flusher");
				flusher();
				logger.trace("FLUSHER EXITING");
			}
		});
		
		runTasks(tasks);


		resetPingTimer();
		
	}

	protected Thread go(final Runnable task, final String name, final String group, final Phaser ph,
			final boolean dereg) {
		NATSThread.setDebug(true);
		NATSThread t = 
		new NATSThread(task, name) {
			public void run() {
				if (ph != null) {
					ph.register();
					logger.trace("{} registered in group {}. # registered for phase {} = {}",
							name,
							group,
							ph.getPhase(),
							ph.getRegisteredParties());
					logger.trace(name + " starting");
					ph.arriveAndAwaitAdvance(); // await all creation
				} else {
					logger.trace("Untracked thread " + name + " starting.");
				}
				
				task.run();
				
				if (ph != null) {
					int oldPhase = ph.getPhase();
					if (dereg) {
						logger.trace(name + " arrive and deregister for phase {}", ph.getPhase());
						logger.trace("{} (group {}) ending phase {}: Registered = {}, Arrived = {}, Unarrived={}",
								name,
								group,
								oldPhase,
								ph.getRegisteredParties(),
								ph.getArrivedParties(),
								ph.getUnarrivedParties());
						int phase = ph.arriveAndDeregister();
						logger.trace(name + " deregistered going into phase {}", phase);
					} else {
						logger.trace("{} (group {}) ending phase {}: Registered = {}, Arrived = {}, Unarrived={}",
								name,
								group,
								ph.getPhase(),
								ph.getRegisteredParties(),
								ph.getArrivedParties(),
								ph.getUnarrivedParties());
						logger.trace("phase before: {}", ph.getPhase());
						int phase = ph.arrive();
//						int phase = ph.arriveAndAwaitAdvance();
						logger.trace(name + " advanced to phase {} after phase {} arrival", phase, oldPhase);
						logger.trace("{} (group {}) beginning phase {}: Registered = {}",
								name,
								group,
								ph.getPhase(),
								ph.getRegisteredParties(),
								ph.getArrivedParties(),
								ph.getUnarrivedParties());
					} 
				} else {
					logger.trace("Untracked thread " + name + " completed.");
				}
			}
		};
		t.start();
		return t;
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
		private String token;
		private Boolean ssl;
		private String name;
		private String lang = ConnectionImpl.LANG_STRING;
		private String version = ConnectionImpl.this.version;

		public ConnectInfo(boolean verbose, boolean pedantic, String username, String password, String token,
				boolean secure,
				String connectionName) {
			this.verbose = new Boolean(verbose);
			this.pedantic = new Boolean(pedantic);
			this.user = username;
			this.pass = password;
			this.token = token;
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
			if (token != null) {
				sb.append(String.format( "\"auth_token\":\"%s\",", token));
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
		long lastAttemptNanos = 0L;

		protected Srv(URI url) {
			this.url = url;
		}

		// Mark the last attempt to connect to this Srv
		void updateLastAttempt() {
			lastAttemptNanos = System.nanoTime();
			lastAttempt = System.currentTimeMillis();
		}

		// Returns time since last attempt, in msec
		long timeSinceLastAttempt() {
			return (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAttemptNanos));
		}

		public String toString() {
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
			String DateToStr = format.format(new Date(lastAttempt));

			return String.format("{url=%s, reconnects=%d, lastAttempt=%s, timeSinceLastAttempt=%dms}",
					url.toString(), reconnects,DateToStr,
					timeSinceLastAttempt());
		}
	}

	private void readLoop()
	{
		Parser parser = null;
		int    len;
		boolean sb;
		// stack copy
		TCPConnection conn = null;

		mu.lock();
		try {
			parser = this.parser;
			this.ps = parser.ps;
		} finally {
			mu.unlock();
		}

		// Stack based buffer.
		byte[] buffer = new byte[DEFAULT_BUF_SIZE];

		while (true)
		{
			mu.lock();
			try
			{
				sb = (isClosed() || isReconnecting());
				if (sb) {
					this.ps = parser.ps;
				}
				conn = this.conn;
			} finally {
				mu.unlock();
			}

			if (sb || conn == null) {
				break;
			}
				
			try
			{
				len = br.read(buffer, 0, DEFAULT_BUF_SIZE);
				if (len==-1) {
					throw new IOException(ERR_STALE_CONNECTION);
				}
				parser.parse(buffer, len);
			} catch (IOException | ParseException e)
			{
				logger.trace("Exception in readLoop(): ConnState was {}", status, e);
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
			this.ps = null;
		} finally {
			mu.unlock();
		}
	}

	// deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
	// It is used to deliver messages to asynchronous subscribers.
	// This function is the run() method of the AsyncSubscription's msgFeeder thread.
	protected void deliverMsgs(Channel<Message> ch)
	{
		//		logger.trace("In deliverMsgs");
		Message m = null;

		mu.lock();
		try
		{
			// Slightly faster to do this directly vs call isClosed
			if (status == ConnState.CLOSED)
				return;
		} finally {
			mu.unlock();
		}

		while (true)
		{
			//				logger.trace("Calling ch.get()...");
			m = ch.get();
			//				logger.trace("ch.get() returned " + m);

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

	protected void processMsgArgs(byte[] buffer, long length) throws ParseException
	{
		String s = new String(buffer, 0, (int)length);
		String[] args = s.split(" ");

//		logger.trace("processMsgArgs() for buffer {}", s);
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
			throw new ParseException("Unable to parse message arguments: " + s, 0);
		}

		if (msgArgs.size < 0)
		{
			throw new ParseException("Invalid Message - Bad or Missing Size: " + s, 9);
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

			s = subs.get(msgArgs.sid);
			if (s==null)
			{
				return;
			}

			s.mu.lock();
			try
			{
				maxReached = s.tallyMessage(length);
				if (!maxReached) {
					Message m = new Message(msgArgs, s, data, length);
					if (!s.addMessage(m)) {
						processSlowConsumer(s);
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

	void removeSub(SubscriptionImpl sub) {
		subs.remove(sub.getSid());
//		logger.trace("Removed sid={} subj={}", 
//				sub.getSid(), sub.getSubject());
		sub.getLock().lock();
		try {
			if (sub.getChannel() != null)
			{
				sub.mch.close();
				sub.mch = null;
//				logger.trace("Closed sid={} subj={}", sub.getSid(), 
//						sub.getSubject());
			}

			// Mark as invalid
			sub.setConnection(null);
			sub.closed=true;
		} finally {
			sub.getLock().unlock();
		}
	}

	void processAsyncException(SubscriptionImpl s, Throwable e) {
		final Subscription sub = s;
		final Connection nc = this; 
		final ExceptionHandler eh = opts.getExceptionHandler();
		final NATSException nex = new NATSException(e, nc, sub);
		
		if (opts.getExceptionHandler() != null && !s.isSlowConsumer())
		{
			executor.execute(new Runnable() {
				public void run() {
					eh.onException(nex);
				}
			});
		}
	}
	
	// processSlowConsumer will set SlowConsumer state and fire the
	// async error handler if registered.
	void processSlowConsumer(SubscriptionImpl s)
	{
//		logger.trace("processSlowConsumer() subj={}",
//				s.getSubject()
//				);
		final IOException ex = new IOException(ERR_SLOW_CONSUMER);
		setLastError(ex);
		processAsyncException(s, ex);
		s.setSlowConsumer(true);
	}

	// FIXME: This is a hack
	// removeFlushEntry is needed when we need to discard queued up responses
	// for our pings as part of a flush call. This happens when we have a flush
	// call outstanding and we call close.
	protected boolean removeFlushEntry(Channel<Boolean> chan)
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
			setLastError(e);
//			logger.error("Could not send PING", e);
		}
	}

	protected void resetPingTimer() {
		mu.lock();
		try {
			Runnable pingRunnable = new Runnable() {
				public void run() {
					processPingTimer();
				}
			};
			
			if (opts.getPingInterval() > 0) {
				if (ptmr != null)
					ptmr.shutdownNow();

				ptmr = Executors.newSingleThreadScheduledExecutor(new NATSThreadFactory("pinger"));
				ptmr.scheduleAtFixedRate(pingRunnable, 
						opts.getPingInterval(), opts.getPingInterval(), 
						TimeUnit.MILLISECONDS);
			}
		} finally {
			mu.unlock();
		}	
	}
	
	protected void processPingTimer() {
		mu.lock();
		try
		{
			if (status != ConnState.CONNECTED)
				return;

			pout++;
			if (pout > opts.getMaxPingsOut()) {
				processOpError(new IOException(ERR_STALE_CONNECTION));
			} else {
				logger.trace("Sending PING after {} seconds.",  
						TimeUnit.MILLISECONDS.toSeconds(opts.getPingInterval()));
				sendPing(null);
				
				return;
			}
		} finally {
			mu.unlock();
		}
	}
	
	// unsubscribe performs the low level unsubscribe to the server.
	// Use SubscriptionImpl.unsubscribe()
	protected void unsubscribe(SubscriptionImpl sub, int max) 
			throws IOException 
	{
		mu.lock();
		try
		{
			if (isClosed())
				throw new IllegalStateException(ERR_CONNECTION_CLOSED);

			// already unsubscribed
			if (!subs.containsKey(sub.getSid()))
			{
				return;
			}

			// If the autounsubscribe max is > 0, set that on the subscription
			if (max > 0)
			{
				sub.setMax(max);
			}
			else
			{
				removeSub((SubscriptionImpl)sub);
			}

			// We will send all subscriptions when reconnecting
			// so that we can suppress here.
			if (!isReconnecting()) {
				String str = String.format(UNSUB_PROTO, 
						sub.getSid(), 
						max>0 ? Integer.toString(max) :"");
				str = str.replaceAll(" +\r\n", "\r\n");
				byte[] unsub = str.getBytes();
				bw.write(unsub);
				logger.trace("=> {}", str.trim() );
			}
		} finally {
			kickFlusher();
			mu.unlock();
		}
	}
	

	protected void kickFlusher()
	{
		if (bw != null) {
			if (fch.getCount()==0) {
				fch.add(true);
			}
		}
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

	// This is the loop of the flusher thread
	private void flusher()
	{
		BufferedOutputStream bw = null;
		TCPConnection conn = null;
		Channel<Boolean> fch = null;
		
		setFlusherDone(false);

		// snapshot the bw and conn since they can change from underneath of us.
		mu.lock();
		bw = this.bw;
		conn = this.conn;
		fch = this.fch;
		mu.unlock();
		
		if (conn == null || bw == null || !conn.isConnected()) {
			return;
		}
		
		while (!isFlusherDone())
		{
			// Wait to be triggered
			if (!fch.get())
				return;

			// Be reasonable about how often we flush
			try { Thread.sleep(1); } catch (InterruptedException e) {}

//			try {
//				if (!mu.tryLock(1, TimeUnit.MILLISECONDS)) {
//					//					System.err.println("FLUSHER CONTINUING");
//					continue;
//				}
//			} catch (InterruptedException e1) {
//			}
			mu.lock();
			try
			{
				
				// Check to see if we should bail out.
				if (!isConnected() || isConnecting() || bw != this.bw || conn != this.conn)
					return;
				bw.flush();
				stats.incrementFlushes();
			} catch (IOException e) {
			} finally {
				mu.unlock();
			}
		}
	}
	
	/* 
	 * (non-Javadoc)
	 * @see io.nats.client.AbstractConnection#flush(int)
	 */
	@Override
	public void flush(int timeout) throws Exception
	{
		long t0 = System.nanoTime();
		long elapsed = 0L;
		boolean timedOut = false;
		Exception err = null;
		if (timeout <= 0)
		{
			throw new IllegalArgumentException(
					"nats: Bad timeout value");
		}

		Channel<Boolean> ch = null;
		mu.lock();
		try
		{
			if (isClosed()) {
				throw new IllegalStateException(ERR_CONNECTION_CLOSED);
			}

			ch = new Channel<Boolean>(1);
logger.trace("flush({}): calling sendPing(ch)", timeout);
			sendPing(ch);
logger.trace("flush({}): returned from sendPing(ch)", timeout);
		} finally {
			mu.unlock();
		}

logger.trace("flush({}): awaiting PONG", timeout);
		Boolean rv = null;
		while (!timedOut && !isClosed() && (rv == null)) {
			elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
			timedOut = (elapsed >= timeout);
			rv = ch.poll();
		}
logger.trace("elapsed = {}", elapsed);
//			Boolean rv = ch.get(timeout);
		if (rv != null && !rv)
		{
			err = new IllegalStateException(ERR_CONNECTION_CLOSED);
		} else {
logger.trace("flush({}): received PONG", timeout);
			mu.lock();
			err = getLastException();
			mu.unlock();
			ch.close();
		}

		if (err != null)
		{
logger.trace("flush({}): calling removeFlushEntry(ch)", timeout, timeout);
			this.removeFlushEntry(ch);
logger.trace("flush({}): after removeFlushEntry(ch), throwing", timeout, timeout);
			throw err;
		}
	}


	/// Flush will perform a round trip to the server and return when it
	/// receives the internal reply.
	@Override
	public void flush() throws Exception
	{
		logger.trace("FLUSH");
		// 60 second default.
		flush(60000);
		logger.trace("FLUSHED!");
	}

	// resendSubscriptions will send our subscription state back to the
	// server. Used in reconnects
	private void resendSubscriptions()
	{
		for (Long key : subs.keySet())
		{
			SubscriptionImpl s = subs.get(key);
			if (s instanceof AsyncSubscription)
				((AsyncSubscriptionImpl)s).enable(); //enableAsyncProcessing()
			logger.trace("Resending subscriptions:");
			sendSubscriptionMessage(s);
			if (s.getMaxPending() > 0) {
				try {
					unsubscribe(s, s.getMaxPending()); 
				} catch (Exception e) { /* NOOP */}
			}
		}
		//			bw.flush();
	}

	public Subscription subscribe(String subj, String queue, MessageHandler cb) 
	{
		SubscriptionImpl sub = null;
		boolean async = (cb != null);
		mu.lock();
		try {
			if (isClosed()) {
				throw new IllegalStateException(ERR_CONNECTION_CLOSED);
			}
			if (async)
				sub = new AsyncSubscriptionImpl(this, subj, queue, cb, opts.getMaxPendingMsgs());
			else
				sub = new SyncSubscriptionImpl(this, subj, queue, opts.getMaxPendingMsgs());

			addSubscription(sub);

			if (!isReconnecting()) {
				if (async)
					((AsyncSubscriptionImpl)sub).start();
				else
					sendSubscriptionMessage(sub);
			}

			kickFlusher();
		} finally {
			mu.unlock();
		}
		return sub;
	}

	@Override
	public AsyncSubscription subscribeAsync(String subject, String queue,
			MessageHandler cb)
	{
		AsyncSubscription s = null;

		mu.lock();
		try
		{
			if (isClosed())
				throw new IllegalStateException(ERR_CONNECTION_CLOSED);

			s = new AsyncSubscriptionImpl(this, subject, queue, null, opts.getMaxPendingMsgs());

			addSubscription((SubscriptionImpl)s);

			if (cb != null)
			{
				s.setMessageHandler(cb);
				s.start();
			}
		} finally {
			mu.unlock();
		}

		return s;
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj, String queue) {
		return subscribeAsync(subj, queue, null);
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj, MessageHandler cb) {
		return subscribeAsync(subj, null, cb);
	}

	@Override
	public AsyncSubscription subscribeAsync(String subj) {
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
	public AsyncSubscription subscribe(String subject, MessageHandler cb) {
		return (AsyncSubscription) subscribe(subject, "", cb);
	}

	@Override 
	public SyncSubscription subscribeSync(String subject, String queue) {
		return (SyncSubscription)subscribe(subject, queue, (MessageHandler)null);
	}

	@Override 
	public SyncSubscription subscribeSync(String subject) {
		return (SyncSubscription)subscribe(subject, "", (MessageHandler)null);
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
		index = Utilities.stringToBytesASCII(dst, index, String.valueOf(msgSize));

		// "\r\n"
		System.arraycopy(crlfProtoBytes, 0, dst, index, crlfProtoBytesLen);
		index += crlfProtoBytesLen;

		return index;
	}

	// Sends a protocol data message by queueing into the bufio writer
	// and kicking the flush go routine. These writes should be protected.
	// publish can throw a few different unchecked exceptions:
	// IllegalStateException, IllegalArgumentException, NullPointerException 
	// IOException
	public void publish(String subject, String reply, byte[] data)
	{
		if (subject == null) {
			throw new NullPointerException(ERR_BAD_SUBJECT);
		}
		if (subject.isEmpty())
		{
			throw new IllegalArgumentException(ERR_BAD_SUBJECT);
		}

		int msgSize = (data != null) ? data.length : 0;
		mu.lock();
		try
		{
			// Proactively reject payloads over the threshold set by server.
			if (msgSize > info.getMaxPayload())
				throw new IllegalArgumentException(ERR_MAX_PAYLOAD);

			// Since we have the lock, examine directly for a tiny performance 
			// boost in fastpath
			if (status == ConnState.CLOSED)
				throw new IllegalStateException(ERR_CONNECTION_CLOSED);

			//TODO Throw the right exception here
			//            if (getLastError() != null)
			//                throw getLastError();

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
				int resizeAmount = Parser.MAX_CONTROL_LINE_SIZE + subject.length()
				+ (reply != null ? reply.length() : 0);

				buildPublishProtocolBuffer(resizeAmount);

				pubProtoLen = writePublishProto(pubProtoBuf, subject,
						reply, msgSize);
			}

			try {
				bw.write(pubProtoBuf, 0, pubProtoLen);
				// logger.trace("=> {}", new String(pubProtoBuf).trim() );

				if (msgSize > 0)
				{
					bw.write(data, 0, msgSize);
					// logger.trace("=> {}", new String(data, 0, msgSize).trim() );
				}

				bw.write(crlfProtoBytes, 0, crlfProtoBytesLen);
			} catch (IOException e) {
				setLastError(e);
				//	throw e;
				// ignore for now
				// e.printStackTrace();
				// unnecessary due to finally clause at bottom of method
				// mu.unlock();
				return;
			}

			kickFlusher();

			stats.incrementOutMsgs();
			stats.incrementOutBytes(msgSize);

//			logger.trace("Successfully published to {}", 
//					subject);
		} finally {
			mu.unlock();
		}
	} // publish

	@Override
	public void publish(String subject, byte[] data)
	{
		publish(subject, null, data);
	}

	@Override
	public void publish(Message msg)
	{
		publish(msg.getSubject(), msg.getReplyTo(), msg.getData());
	}

	private Message _request(String subject, byte[] data, long timeout, TimeUnit unit)
			throws TimeoutException, IOException 
	{
		String inbox 	= newInbox();
		Message m		= null;
		try (SyncSubscription s = subscribeSync(inbox, null))
		{
			s.autoUnsubscribe(1);
			publish(subject, inbox, data);
			m = s.nextMessage(timeout, unit);
		} catch (IOException | TimeoutException e) {
			throw(e);
		} 
//		catch (Exception e) {
//			// NOOP -- note, this will ignore IllegalStateException, including
//			// "nats: Connection Closed" 
//		}

		return m;		
	}

	@Override
	public Message request(String subject, byte[] data, long timeout) 
			throws TimeoutException, IOException {
		return request(subject, data, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public Message request(String subject, byte[] data, long timeout, TimeUnit unit) 
			throws TimeoutException, IOException {
//		logger.trace("In request({},{},{})", subject, 
//				data==null?"null":new String(data), timeout);
		if (timeout <= 0)
		{
			throw new IllegalArgumentException(
					"Timeout must be greater that 0.");
		}

		return _request(subject, data, timeout, unit);
	}

	@Override
	public Message request(String subject, byte[] data) 
			throws IOException, TimeoutException {
		return _request(subject, data, -1, TimeUnit.MILLISECONDS);
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

	protected void sendSubscriptionMessage(SubscriptionImpl sub) {
		mu.lock();
		try
		{
			// We will send these for all subs when we reconnect
			// so that we can suppress here.
			if (!isReconnecting())
			{
				String queue = sub.getQueue();
				String s = String.format(SUB_PROTO, 
						sub.getSubject(), 
						queue!=null ? " " + queue : "",
						sub.getSid());
				try {
					bw.write(Utilities.stringToBytesASCII(s));
//					logger.trace("=> {}", s.trim() );
					kickFlusher();
				} catch (IOException e) {
//					e.printStackTrace();
				}
			}
		} finally {
			mu.unlock();
		}
	}

	/**
	 * @return the closedCB
	 */
	@Override
	public ClosedCallback getClosedCallback() {
		return opts.getClosedCallback();
	}

	/**
	 * @param closedCB the closedCB to set
	 */
	@Override
	public void setClosedCallback(ClosedCallback cb) {
		opts.setClosedCallback(cb);
	}

	/**
	 * @return the disconnectedCB
	 */
	@Override
	public DisconnectedCallback getDisconnectedCallback() {
		return opts.getDisconnectedCallback();
	}

	/**
	 * @param disconnectedCB the disconnectedCB to set
	 */
	@Override
	public void setDisconnectedCallback(DisconnectedCallback cb) {
		opts.setDisconnectedCallback(cb);
	}

	/**
	 * @return the reconnectedCB
	 */
	@Override
	public ReconnectedCallback getReconnectedCallback() {
		return opts.getReconnectedCallback();
	}

	/**
	 * @param reconnectedCB the reconnectedCB to set
	 */
	@Override
	public void setReconnectedCallback(ReconnectedCallback cb) {
		opts.setReconnectedCallback(cb);
	}

	/**
	 * @return the exceptionHandler
	 */
	@Override
	public ExceptionHandler getExceptionHandler() {
		return opts.getExceptionHandler();
	}

	/**
	 * @param exceptionHandler the exceptionHandler to set
	 */
	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		opts.setExceptionHandler(exceptionHandler);
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
	public String getConnectedServerId()
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
	public Exception getLastException() {
		return lastEx;
	}

	private void setLastError(Exception e) {
		this.lastEx = e;
//		if (lastEx == null) {
//			logger.trace("LAST ERROR SET TO: null");
//		} else {
//			String name = lastEx.getClass().getName();
//			String msg = lastEx.getMessage();
//			logger.trace("LAST ERROR SET TO: {}: {}", name, msg);
//		}
	}
	
	protected Options getOptions() {
		return this.opts;
	}

	void setPending(ByteArrayOutputStream pending) {
		this.pending = pending;
	}

	ByteArrayOutputStream getPending() {
		return this.pending;
	}

//	static void printSubs(ConnectionImpl c) {
//		c.logger.trace("SUBS:");
//		for (long key : c.subs.keySet())
//		{
//			c.logger.trace("\tkey: " + key + " value: " + c.subs.get(key));
//		}
//	}

	protected void sleepMsec(long msec) {
		try {
			logger.trace("Sleeping for {} ms", msec);
			Thread.sleep(msec);
			logger.trace("Slept    for {} ms", msec);
		} catch (InterruptedException e) {
			// NOOP
		}
	}

	void setOutputStream(BufferedOutputStream out)
	{
		mu.lock();
		try {
			this.bw = out;
		} finally {
			mu.unlock();
		}
	}

	/**
	 * @return the pongs
	 */
	protected Queue<Channel<Boolean>> getPongs() {
		return pongs;
	}

	/**
	 * @param pongs the pongs to set
	 */
	protected void setPongs(Queue<Channel<Boolean>> pongs) {
		this.pongs = pongs;
	}

	/**
	 * @return the subs
	 */
	protected Map<Long, SubscriptionImpl> getSubs() {
		return subs;
	}

	/**
	 * @param subs the subs to set
	 */
	protected void setSubs(Map<Long, SubscriptionImpl> subs) {
		this.subs = subs;
	}
	
	// for testing purposes
	protected List<Srv> getServerPool() {
		return this.srvPool;
	}
	
	// for testing purposes
	protected void setServerPool(List<Srv> pool) {
		this.srvPool = pool;
	}

}
