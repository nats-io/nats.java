package io.nats.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionImpl implements Connection {
	
	final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);
	
//    protected class Channel<T> extends LinkedBlockingQueue<T> {}
	private ConnectionImpl nc = this;
	private static final int DEFAULT_READ_LENGTH = 512;
	private static final String inboxPrefix = "_INBOX.";

	private final Lock mu 					= new ReentrantLock();
	private AtomicInteger sidCounter		= new AtomicInteger();
	private URI url 						= null;
	private Options opts 					= null;

	private TCPConnection conn				= new TCPConnection();
	
	// Prepare protocol messages for efficiency

    byte[] pubProtoBuf = null;
    // we have a buffered reader for writing, and reading.
    // This is for both performance, and having to work around
    // interlinked read/writes (supported by the underlying network
    // stream, but not the BufferedStream).

	private DataOutputStream bw 			= null;
	private DataInputStream br 				= null;
	private ByteArrayOutputStream pending 	= null;
//  private MemoryStream    pending = null;
	
	private ReentrantLock flusherLock		= new ReentrantLock();
    private boolean flusherKicked 			= false;
    private boolean flusherDone     		= false;

	private Map<Long, Subscription> subs 	= null;
	private List<Srv> srvPool 				= null;
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
	protected byte[] pubProtoBytes			= null;
	protected int pubProtoBytesLen			= 0;
	protected byte[] crlfProtoBytes			= null;
	protected int crlfProtoBytesLen			= 0;

	protected Statistics stats				= null;
	private Queue<Channel<Boolean>> pongs	= new LinkedBlockingQueue<Channel<Boolean>>(8);
	
	private static final int NUM_TASK_THREADS = 5;
	private final ExecutorService taskExec = Executors.newFixedThreadPool(NUM_TASK_THREADS, new NATSThreadFactory("natsthreadfactory"));
	private Vector<Void> futures			= new Vector<Void>(); 
	private Random r						= null;
	
	public ConnectionImpl(Options o) throws NoServersException {
		this.opts = o;
		this.subs = new ConcurrentHashMap<Long, Subscription>();
		this.stats = new Statistics();
		this.ps = new Parser(this);
		this.msgArgs = new MsgArg();

		sidCounter.set(0);
		
		pingProtoBytes = PING_PROTO.getBytes();
		pingProtoBytesLen = pingProtoBytes.length;
		pongProtoBytes = PONG_PROTO.getBytes();
		pongProtoBytesLen = pongProtoBytes.length;
		pubProtoBytes = PUB_PROTO.getBytes(Charset.forName("UTF-8"));
		pubProtoBytesLen = pubProtoBytes.length;
		crlfProtoBytes = _CRLF_.getBytes(Charset.forName("UTF-8"));
		
        // predefine the start of the publish protocol message.
        buildPublishProtocolBuffer(SCRATCH_SIZE);

		setupServerPool();
	}

    private void buildPublishProtocolBuffer(int size)
    {
        pubProtoBuf = new byte[size];
        System.arraycopy(pubProtoBytes, 0, pubProtoBuf, 0, pubProtoBytesLen);
    }


	// Create the server pool using the options given.
	// We will place a Url option first, followed by any
	// Srv Options. We will randomize the server pool unless
	// the NoRandomize flag is set.
	protected void setupServerPool() throws NoServersException {

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

		pickServer();

		// TODO the following code only protects from implementer error

		// Place default URL if pool is empty.
		// if (serverPool.isEmpty())
		// {
		// Srv srv = null;
		// srv = new Srv(new URI(ConnectionFactory.DEFAULT_URL));
		// serverPool.add(srv);
		// }
	}

	protected Srv currentServer() {
		Iterator<Srv> it = srvPool.listIterator();
		while (it.hasNext()) {
			Srv s = it.next();
			if (s.url.equals(this.url))
				return s;
		}
		return null;
	}

	protected Srv selectNextServer() {
		int maxReconnect = opts.getMaxReconnect();
		Srv s = currentServer();

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
			srvPool.add(s);
		}

		if (srvPool.isEmpty()) {
			this.url = null;
			return null;
		}

		this.url = srvPool.get(0).url;
		return srvPool.get(0);
	}

	// Will assign the correct server to the nc.Url
	protected void pickServer() throws NoServersException {
		if (srvPool.isEmpty())
			throw new NoServersException();

		// Return the first server in the list
		this.url = srvPool.get(0).url;
	}

	public ConnectionImpl connect() throws Exception {
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
					if (createConn()) {
						s.reconnects = 0;
						processConnectInit();
						connected = true;
					}
				} finally {
					this.url = null;
					mu.unlock();
				}
			} catch (ConnectionException e) {
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
					lastEx = new NoServersException("Unable to connect to a server.");

				throw lastEx;
			}
		} finally {
			mu.unlock();
		}

		// while (!channel.isConnected()) {
		// }

		return this;
	}

	// createConn will connect to the server and wrap the appropriate
	// bufio structures. It will do the right thing when an existing
	// connection is in place.
	private boolean createConn() {
		currentServer().updateLastAttempt();

		try {
		Srv s = currentServer();
		conn.open(s.url.getHost(), s.url.getPort(), opts.getConnectionTimeout());
		
		if ((pending != null) && (bw != null)) {
			// flush to the pending buffer
			bw.flush();
		}
		bw = conn.getWriteBufferedStream(DEFAULT_BUF_SIZE * 6);
		br = conn.getReadBufferedStream((DEFAULT_BUF_SIZE) * 6);
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}

    // This will clear any pending flush calls and release pending calls.
    private void clearPendingFlushCalls()
    {
        mu.lock();
        try
        {
            // Clear any queued pongs, e.g. pending flush calls.
            for (Queue<Boolean> ch : pongs)
            {
                if (ch != null)
                    ch.add(true);
            }

            pongs.clear();
        } finally {
        	mu.unlock();
        }
    }

    public void close() {
    	close(ConnState.CLOSED, true);
    }
    
    // Low level close call that will do correct cleanup and set
    // desired status. Also controls whether user defined callbacks
    // will be triggered. The lock should not be held entering this
    // function. This function will handle the locking manually.
    private void close(ConnState closeState, boolean invokeDelegates)
    {
        ConnEventHandler disconnectedEventHandler = null;
        ConnEventHandler closedEventHandler = null;

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
                ptmr.shutdown();
                try {
					ptmr.awaitTermination(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {

				}
            }

            // Close sync subscriber channels and release any
            // pending NextMsg() calls.
            
//            foreach (AsyncSubscriptionImpl s : subs)
//            {
//                s.closeChannel();
//            }
            Iterator<Map.Entry<Long, Subscription>> it = subs.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, Subscription> pair = (Map.Entry<Long, Subscription>)it.next();
            	AbstractSubscriptionImpl s = (AbstractSubscriptionImpl)pair.getValue();
            	s.closeChannel();
                it.remove(); // avoids a ConcurrentModificationException
            }
            subs.clear();
            sidCounter.set(0);

            // perform appropriate callback is needed for a
            // disconnect;
            if (invokeDelegates && conn.isSetup() &&
                opts.disconnectedEventHandler != null)
            {
                // TODO:  Mirror go, but this can result in a callback
                // being invoked out of order
                disconnectedEventHandler = opts.disconnectedEventHandler;
                taskExec.execute(new ConnEventHandlerTask(disconnectedEventHandler, new ConnEventArgs(this)));
//                new Task(() => { disconnectedEventHandler(new ConnEventArgs(this)); }).Start();
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

            closedEventHandler = opts.disconnectedEventHandler;
        } finally {
        	mu.unlock();
        }

        if (invokeDelegates && closedEventHandler != null)
        {
        	taskExec.execute(new ConnEventHandlerTask(closedEventHandler, new ConnEventArgs(this)));
        }

        mu.lock();
        try {
            status = closeState;
        } finally {
        	mu.unlock();
        }
    }

    private Queue<Channel<Boolean>> createPongs()
    {
    	Queue<Channel<Boolean>> rv = new LinkedBlockingQueue<Channel<Boolean>>();
        return rv;
    }


	protected void processConnectInit() throws ConnectionException {
		status = ConnState.CONNECTING;

		// Process the INFO protocol that we should be receiving
		processExpectedInfo();

		// Send the CONNECT and PING protocol, and wait for the PONG.
		try {
			sendConnect();
		} catch (NATSException e) {
			throw new ConnectionException("Couldn't send CONNECT proto", e);
		}

		// Clear our deadline, regardless of error
		// natsDeadline_Clear(&(nc->deadline));

		// Switch to blocking socket here...
		// s = natsSock_SetBlocking(nc->fd, true);

		// Start the readLoop and flusher threads
		spinUpSocketWatchers();
	}

	protected void processExpectedInfo() throws ConnectionException {
		ControlMsg c;

		try {
			c = readOp();
		} catch (IOException e) {
			processOpError(e);
			return;
		} finally {
			// TODO why adjust sendtimeouts in .NET?
		}

		if (!c.op.equals(_INFO_OP_)) {
			throw new ConnectionException("Protocol exception, INFO not received");
		}

		processInfo(c.args);
	}

    // processPing will send an immediate pong protocol response to the
    // server. The server uses this mechanism to detect dead clients.
    protected void processPing() 
    {
        try {
        	logger.debug("Sending PONG");
			sendProto(pongProtoBytes, pongProtoBytesLen);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    // processPong is used to process responses to the client's ping
    // messages. We use pings for the flush mechanism as well.
    protected void processPong()
    {
        //System.out.println("COLIN:  Processing pong.");
        Channel<Boolean> ch = new Channel<Boolean>(1);
        mu.lock();
        try {
            if (pongs.size() > 0)
                ch = pongs.poll();

            pout = 0;

            if (ch != null)
            {
                ch.add(true);
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
	private void processInfo(String info) {
		if ((info == null) || info.isEmpty()) {
			return;
		}

		this.info = new ServerInfo(info);
		
		return;
	}
	
	private void processOpError(Exception e) {
        boolean disconnected = false;

        mu.lock();
        try{
            if (isConnecting() || isClosed() || isReconnecting())
            {
                return;
            }

            if (opts.isReconnectAllowed() && status == ConnState.CONNECTED)
            {
                processReconnect();
            }
            else
            {
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

//	private void processOpError(StaleConnectionException e) {
//		// TODO Auto-generated method stub
//
//	}

    private void processDisconnect() {
		// TODO Auto-generated method stub
		
	}

    // This will process a disconnect when reconnect is allowed.
    // The lock should not be held on entering this function.
    private void processReconnect()
    {
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
                conn.teardown();
            }

            taskExec.execute(new ReconnectTask());
            
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
        if (pending == null)
            return;

        if (pending.size() > 0)
        {
            try {
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

        if (opts.disconnectedEventHandler != null)
        {
            mu.unlock();

            taskExec.execute(new ConnEventHandlerTask(disconnectedEventHandler,new ConnEventArgs(this)));

            mu.lock();
        }
        
        Srv s;
        while ((s = selectNextServer()) != null)
        {
            if (lastEx != null)
                break;

            // Sleep appropriate amount of time before the
            // connection attempt if connecting to same server
            // we just got disconnected from.
            double elapsedMillis = s.timeSinceLastAttempt();

            if (elapsedMillis < opts.getReconnectWait())
            {
                double sleepTime = opts.getReconnectWait() - elapsedMillis;

                mu.unlock();;
                try {
					Thread.sleep((int)sleepTime);
				} catch (InterruptedException e) { }
                mu.lock();
            }
            else
            {
                // Yield so other things like unsubscribes can
                // proceed.
                mu.unlock();
                try {
					Thread.sleep(50);
				} catch (InterruptedException e) { }
                mu.lock();
            }

            if (isClosed())
                break;

            s.reconnects++;

            try
            {
                // try to create a new connection
                createConn();
            }
            catch (Exception ne)
            {
                // not yet connected, retry and hold
                // the lock.
                continue;
            }

            // We are reconnected.
            stats.reconnects++;

            // TODO Clean this up? It's never used in any of the clients
            // Clear out server stats for the server we connected to..
            // s.didConnect = true;

            // process our connect logic
            try
            {
                processConnectInit();
            }
            catch (Exception e)
            {
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
            catch (Exception e)
            {
                status = ConnState.RECONNECTING;
                continue;
            }

            // get the event handler under the lock
            ConnEventHandler reconnectedEh = opts.reconnectedEventHandler;

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
            	taskExec.execute(new ConnEventHandlerTask(reconnectedEh, new ConnEventArgs(this)));
            }

            return;
        } // while

        // we have no more servers left to try.
        if (lastEx == null)
            lastEx = new NoServersException("Unable to reconnect");

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
	protected void sendConnect() throws NATSException {
		try {
			String cp = connectProto();
			System.err.print("connectProto(): " + cp);
			bw.writeBytes(cp);
			bw.flush();

			bw.write(pingProtoBytes, 0, pingProtoBytesLen);
			bw.flush();
		} catch (Exception e) {
            if (lastEx == null)
                throw new NATSException("Error sending connect protocol message", e);			
		}
		
		ControlMsg c;
		try {
			c = readOp();
		} catch (IOException e) {
			this.processOpError(e);
			return;
		}

		if (c.op.equals(PONG_PROTO.substring(0, PONG_PROTO.indexOf(_CRLF_)))) {
			logger.debug("Received PONG message");
			status = ConnState.CONNECTED;
			return;
		} else {
			if (c.op == null)
			{
				throw new ConnectionException("Connect read protocol error");
			}
            else if (c.op.startsWith(_ERR_OP_))
            {
                throw new ConnectionException(c.args);
            }
            else if (c.op.startsWith("tls:"))
            {
                throw new SecureConnRequiredException(c.args);
            }
            else
            {
                throw new NATSException(c.args);
            }
        }
	}

    private void sendProto(byte[] value, int length) throws IOException
    {
        mu.lock();
        try {
            bw.write(value, 0, length);
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

	protected ControlMsg readOp() throws IOException {
		// This is only used when creating a connection, so simplify
		// life and just create a stream reader to read the incoming
		// info string. If this becomes part of the fastpath, read
		// the string directly using the buffered reader.
		//
		// Do not close or dispose the stream reader - we need the underlying
		// BufferedStream.
		InputStreamReader is = conn.getInputStreamReader();
		BufferedReader br = new BufferedReader(is);
		
		return new ControlMsg(br.readLine());
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

		//taskExec.execute(new ReadLoopTask(this));  
		//taskExec.execute(new Flusher(this));
		Thread t = null;

		t = new NATSThread(new ReadLoopTask(), "readloop");
		t.start();
		socketWatchers.add(t);
		
		t = new NATSThread(new Flusher(this), "flusher");
		t.start();
		socketWatchers.add(t);

		mu.lock();
		try {
			if (opts.getPingInterval() > 0) {
				if (ptmr == null) {
					ptmr = Executors.newSingleThreadScheduledExecutor(new NATSThreadFactory("pinger"));
				} else {
					ptmr.shutdownNow();
				}
				ptmr.scheduleAtFixedRate(
						new PingTimerEventHandler(), 0, opts.getPingInterval(), 
						TimeUnit.MILLISECONDS);
			}
		} finally {
			mu.unlock();
		}
	}

	protected class ControlMsg {
		String op = null;
		String args = null;

		protected ControlMsg(String s) {
			String[] parts = s.split(" ", 2);

			switch (parts.length) {
			case 1:
				op = parts[0].trim();
				break;
			case 2:
				op = parts[0].trim();
				args = parts[1].trim();
				break;
			default:
			}
		}
	}

	protected ConnEventHandler closedEventHandler;
	protected ConnEventHandler disconnectedEventHandler;
	protected ConnEventHandler reconnectedEventHandler;
	protected ExceptionHandler exceptionEventHandler;

	public static enum ConnState {
		DISCONNECTED, CONNECTED, CLOSED, RECONNECTING, CONNECTING
	}
	
	protected final String STALE_CONNECTION     = "State ConnectionImpl";

	public ConnState status = ConnState.DISCONNECTED;
	// Client version
	protected static final String 	VERSION					= "1.0.0";
	// Default language string for CONNECT message
	protected static final String	LANG_STRING				= "java";

	// Scratch storage for assembling protocol headers
	protected static final int SCRATCH_SIZE = 512;

	// The size of the bufio reader/writer on top of the socket.
	protected static final int DEFAULT_BUF_SIZE = 32768;

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

	// MessageImpl Prototypes
	public static final String CONN_PROTO = "CONNECT %s" + _CRLF_;
	public static final String PING_PROTO = "PING" + _CRLF_;
	public static final String PONG_PROTO = "PONG" + _CRLF_;
	public static final String PUB_PROTO = "PUB %s %s %d" + _CRLF_;
	public static final String SUB_PROTO = "SUB %s %s %d" + _CRLF_;
	public static final String UNSUB_PROTO = "UNSUB %d %s" + _CRLF_;

	class ConnectInfo {
		private Boolean verbose;
		private Boolean pedantic;
		private String user;
		private String pass;
		private Boolean ssl;
		private String name;
		private String lang = ConnectionImpl.LANG_STRING;
		private String version = ConnectionImpl.VERSION;

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

	class ServerInfo {
		private String id;
		private String host;
		private int port;
		private String version;
		private boolean authRequired;
		private boolean sslRequired;
		private long maxPayload; // int64 in Go

		private Map<String, String> parameters = new HashMap<String, String>();

		public ServerInfo(String jsonString) {
			try {
				jsonString = jsonString.substring(jsonString.indexOf('{') + 1);
				jsonString = jsonString.substring(0, jsonString.lastIndexOf('}') - 1);
			} catch (IndexOutOfBoundsException iobe) {
				// do nothing
			}

			String[] kvPairs = jsonString.split(",");
			for (String s : kvPairs)
				addKVPair(s);

			
			this.id = parameters.get("server_id");
			this.host = parameters.get("host");
			this.port = Integer.parseInt(parameters.get("port"));
			this.version = parameters.get("version");
			this.authRequired = Boolean.parseBoolean(parameters.get("auth_required"));
			this.sslRequired = Boolean.parseBoolean(parameters.get("ssl_required"));
			this.maxPayload = Long.parseLong(parameters.get("max_payload"));

		}

		private void addKVPair(String kvPair) {
			String key;
			String val;

			kvPair = kvPair.trim();
			String[] parts = kvPair.split(":");
			key = parts[0].trim();
			val = parts[1].trim();

			// trim the quotes
			int lastQuotePos = key.lastIndexOf("\"");
			key = key.substring(1, lastQuotePos);

			// bools and numbers may not have quotes.
			if (val.startsWith("\"")) {
				lastQuotePos = val.lastIndexOf("\"");
				val = val.substring(1, lastQuotePos);
			}
			parameters.put(key, val);
		}

		/**
		 * @return the id
		 */
		String getId() {
			return id;
		}

		/**
		 * @param id the id to set
		 */
		void setId(String id) {
			this.id = id;
		}

		/**
		 * @return the host
		 */
		String getHost() {
			return host;
		}

		/**
		 * @param host the host to set
		 */
		void setHost(String host) {
			this.host = host;
		}

		/**
		 * @return the port
		 */
		int getPort() {
			return port;
		}

		/**
		 * @param port the port to set
		 */
		void setPort(int port) {
			this.port = port;
		}

		/**
		 * @return the version
		 */
		String getVersion() {
			return version;
		}

		/**
		 * @param version the version to set
		 */
		void setVersion(String version) {
			this.version = version;
		}

		/**
		 * @return the authRequired
		 */
		boolean isAuthRequired() {
			return authRequired;
		}

		/**
		 * @param authRequired the authRequired to set
		 */
		void setAuthRequired(boolean authRequired) {
			this.authRequired = authRequired;
		}

		/**
		 * @return the sslRequired
		 */
		boolean isSslRequired() {
			return sslRequired;
		}

		/**
		 * @param sslRequired the sslRequired to set
		 */
		void setSslRequired(boolean sslRequired) {
			this.sslRequired = sslRequired;
		}

		/**
		 * @return the maxPayload
		 */
		long getMaxPayload() {
			return maxPayload;
		}

		/**
		 * @param maxPayload the maxPayload to set
		 */
		void setMaxPayload(long maxPayload) {
			this.maxPayload = maxPayload;
		}

		/**
		 * @return the parameters
		 */
		Map<String, String> getParameters() {
			return parameters;
		}

		/**
		 * @param parameters the parameters to set
		 */
		void setParameters(Map<String, String> parameters) {
			this.parameters = parameters;
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
        byte[] buffer = new byte[DEFAULT_READ_LENGTH];
        Parser parser = new Parser(this);
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
                logger.debug("Reading from buffered reader");            	
                len = br.read(buffer, 0, DEFAULT_READ_LENGTH);
                String s = new String(buffer, 0, len, Charset.forName("UTF-8"));
                logger.debug("Received bytes: " + s);
                parser.parse(buffer, len);
            } catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            catch (Exception e)
            {
            e.printStackTrace();
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
//		private ConnectionImpl nc = null;
		
//		public ReadLoopTask(ConnectionImpl connectionImpl) {
//			this.nc = connectionImpl;
//		}

		@Override
		public void run() {
			readLoop();
		}

	}

    // deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
    // It is used to deliver messages to asynchronous subscribers.
    protected void deliverMsgs(Channel<Message> mch)
    {
        Message m;

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

//            m = ch.get(-1);
            m = mch.poll();
            if (m == null)
            {
                // the channel has been closed, exit silently.
                return;
            }

            // Note, this seems odd message having the sub process itself, 
            // but this is good for performance.
            AbstractSubscriptionImpl s = (AbstractSubscriptionImpl) m.getSubscription();
            if (!s.processMsg(m))
            {
                mu.lock();
                try
                {
                    removeSub(s);
                } finally {
                	mu.unlock();
                }
            }
        }
    }

	
	
	protected class Flusher implements Runnable {

		public Flusher(ConnectionImpl connectionImpl) {
			// TODO Auto-generated constructor stub
		}

		@Override
		public void run() {
			flusher();
		}
		
	}
	
    protected void processMsgArgs(byte[] buffer, long length) throws ParseException
    {
        String s = new String(buffer, 0, (int)length);
        String[] args = s.split(" ");

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
                throw new ParseException("Unable to parse message arguments: " + s);
        }

        if (msgArgs.size < 0)
        {
            throw new ParseException("Invalid MessageImpl - Bad or Missing Size: " + s);
        }
    }
   
    // processMsg is called by parse and will place the msg on the
    // appropriate channel for processing. All subscribers have their
    // their own channel. If the channel is full, the connection is
    // considered a slow subscriber.
    protected void processMsg(byte[] msg, long length)
    {
        boolean maxReached = false;
        
        Subscription sub;
        AbstractSubscriptionImpl s;
       

        mu.lock();
        try
        {
            stats.inMsgs++;
            stats.inBytes += length;

            // In regular message processing, the key should be present,
            // so optimize by using an an exception to handle a missing key.
            // (as opposed to checking with Contains or TryGetValue)
            try
            {
            	sub = subs.get(msgArgs.sid);
                s = (AbstractSubscriptionImpl)sub; 
            }
            catch (Exception e)
            {
            	//TODO Figure out if this can happen with ConcurrentHashMap
                // this can happen when a subscriber is unsubscribing.
                return;
            }

            s.mu.lock();
            try
            {
                maxReached = s.tallyMessage(length);
                if (maxReached == false)
                {
                    if (!s.addMessage(new MessageImpl(msgArgs, sub, msg, length), opts.getSubChanLen()))
                    {
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
    void removeSub(Subscription s) {
		// TODO Auto-generated method stub
		
	}

    // processSlowConsumer will set SlowConsumer state and fire the
    // async error handler if registered.
    void processSlowConsumer(AbstractSubscriptionImpl s)
    {
        if (opts.asyncErrorEventHandler != null && !s.sc)
        {
        	ErrEventArgs args = new ErrEventArgs(this, s, "Slow Consumer");
        	
        	taskExec.execute(new ErrEventHandlerTask(opts.asyncErrorEventHandler, args));
//            new Task(() =>
//            {
//                opts.AsyncErrorEventHandler(this,
//                    new ErrEventArgs(this, s, "Slow Consumer"));
//            }).Start();
        }
        s.sc = true;
    }

	private class PingTimerEventHandler implements Runnable {

    	@Override
    	public void run() {
    		mu.lock();
    		{
    			if (status != ConnState.CONNECTED)
    			{
    				return;
    			}

    			pout++;

    			if (pout > opts.getMaxPingsOut())
    			{
    				processOpError(new StaleConnectionException());
    				return;
    			}

    			try {
    				logger.debug("Sending PING after "+ opts.getPingInterval() + " seconds.");
					sendPing(null);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}	
    	}

    }
    
    // FIXME: This is a hack
    // removeFlushEntry is needed when we need to discard queued up responses
    // for our pings as part of a flush call. This happens when we have a flush
    // call outstanding and we call close.
    private boolean removeFlushEntry(Channel<Boolean> chan)
    {
        if (pongs == null)
            return false;

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
    private void sendPing(Channel<Boolean> ch) throws IOException
    {
        if (ch != null)
            pongs.add(ch);

        bw.write(pingProtoBytes, 0, pingProtoBytesLen);
        bw.flush();
    }

    // unsubscribe performs the low level unsubscribe to the server.
    // Use AsyncSubscriptionImpl.Unsubscribe()
    protected void unsubscribe(Subscription sub, int max) throws ConnectionClosedException, IOException
    {
        mu.lock();
        {
            if (isClosed())
                throw new ConnectionClosedException();

            Subscription s = subs.get(sub.getSid());
            if (s == null)
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
            // so that we can supress here.
            if (!isReconnecting()) {
            	String unsubMsg = String.format(UNSUB_PROTO, s.getSid(), max);
            	bw.writeBytes(unsubMsg);
            }

        }

        kickFlusher();
    }

    private void kickFlusher()
    {
    	//        flusherLock.lock();
    	synchronized (flusherLock) {
    		try
    		{
    			if (!flusherKicked) {
    				flusherLock.notify();
    			}
    			flusherKicked = true;
    			logger.debug("FLUSHER KICKED");
    		} finally {
    			//        	flusherLock.unlock();
    		}
    	}
    }
   
    private boolean waitForFlusherKick()
    {
    	logger.debug("IN waitForFlusherKick()");
    	synchronized(flusherLock) {
    		try 
    		{
    			if (flusherDone)
    				return false;
    			else 
    				logger.debug("IN waitForFlusherKick(), flusherDone =" + flusherDone);

    			// if kicked before we get here meantime, skip
    			// waiting.
    			if (!flusherKicked)
    			{
    				logger.debug("IN waitForFlusherKick(), waiting for lock");
    				flusherLock.wait();
    			}

    			flusherKicked = false;
    		} catch (InterruptedException e) {
    			e.printStackTrace();
    		} finally {
    			//        	flusherLock.unlock();
    		}
    	}
    	logger.debug("LEAVING waitForFlusherKick(), returning true");

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
        logger.debug("###IN flusher() top");
        setFlusherDone(false);

        // If TCP connection isn't connected, we are done
        if (!conn.isConnected())
        {
            return;
        }
        logger.debug("###IN flusher(), conn is CONNECTED");

        while (!isFlusherDone())
        {
            logger.debug("###IN flusher() about to waitForFlusherKick");
            boolean val = waitForFlusherKick();
            logger.debug("###IN flusher(), waitForFlusherKick = " + val);
            if (val == false)
                return;

            mu.lock();
            logger.debug("###IN flusher(), acquired lock");
            try
            {
                if (!isConnected())
                    return;
                logger.debug("###IN flusher(), about to flush");
                // TODO: Check writability of underlying stream?              
                bw.flush();
                logger.debug("###ACTUALLY FLUSHED BUFFER");
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
            	mu.unlock();
            }
        }
    }
    
    public void flush(int timeout) throws Exception
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
            if (isClosed())
                throw new ConnectionClosedException();

            sendPing(ch);
        } finally {
        	mu.unlock();
        }

        try
        {
            boolean rv = ch.poll(timeout, TimeUnit.MILLISECONDS);
            if (!rv)
            {
                lastEx = new ConnectionClosedException();
            }
        }
        catch (Exception e)
        {
            lastEx = new NATSException("Flush channel error.", e);
        }

        if (lastEx != null)
        {
            removeFlushEntry(ch);
            throw lastEx;
        }
    }
    
    /// Flush will perform a round trip to the server and return when it
    /// receives the internal reply.
    public void flush() throws Exception
    {
        // 60 second default.
        flush(60000);
    }

    // resendSubscriptions will send our subscription state back to the
    // server. Used in reconnects
    private void resendSubscriptions() throws IOException
    {
        Iterator<Map.Entry<Long, Subscription>> it = subs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Subscription> pair = (Map.Entry<Long, Subscription>)it.next();
        	Subscription s = (Subscription)pair.getValue();
        	
        	// TODO Fix this
//            if (s is IAsyncSubscription)
//                ((AsyncSubscription)s).enableAsyncProcessing();

        	String command = String.format(SUB_PROTO, s.getSubject(), s.getQueue(), s.getSid());
            bw.write(command.getBytes(),0,command.length());

        	it.remove(); // avoids a ConcurrentModificationException
        }

        bw.flush();
    }

    class ConnEventHandlerTask implements Runnable {

    	private final ConnEventHandler cb;
    	private final ConnEventArgs eventArgs;

    	ConnEventHandlerTask(ConnEventHandler cb, ConnEventArgs eventArgs) {
    		this.cb = cb;
    		this.eventArgs = eventArgs;
    	}

    	public void run() {
    		cb.onEvent(eventArgs);
    	}

    }

    class ErrEventHandlerTask implements Runnable {
    	private final ErrorEventHandler cb;
    	private final ErrEventArgs eventArgs;

    	ErrEventHandlerTask(ErrorEventHandler cb, ErrEventArgs eventArgs) {
    		this.cb = cb;
    		this.eventArgs = eventArgs;
    	}

    	public void run() {
    		cb.onError(eventArgs);
    	}
    	
    }
    
    private Subscription subscribe(String subj, String queue, MessageHandler cb, int chanLen) {
    	Subscription sub = null;
    	mu.lock();
    	try {
    		if (nc.isClosed()) {
    			return null;
    		}
    		if (cb != null)
    			sub = new AsyncSubscriptionImpl(nc, subj, queue, cb);
    		else
    			sub = new SyncSubscriptionImpl(nc, subj, queue);

    		addSubscription(sub);

    		if (!isReconnecting()) {
    			String s = String.format(SUB_PROTO, subj, queue, sub.getSid());
    			try {
					bw.writeBytes(s);
					logger.debug("Sent [" + s + "]" );
				} catch (IOException e) {
				}
    		}

    	} finally {
    		mu.unlock();
    		kickFlusher();
    	}
    	return sub;
    }
    
    @Override
    public Subscription subscribe(String subj, String queue, MessageHandler cb) {
    	Subscription s = subscribe(subj, queue, cb, opts.getSubChanLen());
    	addSubscription(s);
		return s;
    }

	private void addSubscription(Subscription s) {
		subs.put(new Long(sidCounter.getAndIncrement()), s);
		logger.debug("Successfully added subscription to " + s.getSubject() + "[" + s.getSid() + "]");
	}

	@Override
	public Subscription subscribe(String subject, MessageHandler cb) {
		return subscribe(subject, "", cb, opts.getSubChanLen());
	}

	@Override 
	public SyncSubscription subscribeSync(String subject, String queue) {
		return (SyncSubscription)subscribe(subject, queue, (MessageHandler)null, opts.getSubChanLen());
	}

	@Override 
	public SyncSubscription subscribeSync(String subject) {
		return (SyncSubscription)subscribe(subject, "", (MessageHandler)null, opts.getSubChanLen());
	}

	@Override
	public Subscription QueueSubscribe(String subject, String queue, MessageHandler cb) {
		return subscribe(subject, queue, cb, opts.getSubChanLen());
	}

	@Override
	public Subscription QueueSubscribeSync(String subject, String queue) {
		return subscribe(subject, queue, (MessageHandler)null, opts.getSubChanLen());
	}
	
    // Use low level primitives to build the protocol for the publish
    // message.
    private int writePublishProto(byte[] dst, String subject, String reply, int msgSize)
    {
        // skip past the predefined "PUB "
        int index = pubProtoBytesLen;

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


    // Roll our own fast conversion - we know it's the right
    // encoding. 
    char[] convertToStrBuf = new char[SCRATCH_SIZE];

    // Caller must ensure thread safety.
    private String convertToString(byte[] buffer, int length)
    {
        // expand if necessary
        if (length > convertToStrBuf.length)
        {
            convertToStrBuf = new char[length];
        }

        for (int i = 0; i < length; i++)
        {
            convertToStrBuf[i] = (char)buffer[i];
        }

        // This is the copy operation for msg arg strings.
        return new String(convertToStrBuf, 0, (int)length);
    }


    // publish is the internal function to publish messages to a gnatsd.
    // Sends a protocol data message by queueing into the bufio writer
    // and kicking the flush go routine. These writes should be protected.
	@Override
    public void publish(String subject, String reply, byte[] data) throws ConnectionClosedException
    {
		String s = subject.trim();
        if ((s==null) || subject.isEmpty())
        {
            throw new IllegalArgumentException(
                "Subject cannot be null, empty, or whitespace.");
        }

        int msgSize = data != null ? data.length : 0;

        mu.lock();
        try
        {
            // Proactively reject payloads over the threshold set by server.
            if (msgSize > info.getMaxPayload())
                throw new IllegalArgumentException("Message payload size (" 
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
                int resizeAmount = SCRATCH_SIZE + subject.length()
                    + (reply != null ? reply.length() : 0);

                buildPublishProtocolBuffer(resizeAmount);

                pubProtoLen = writePublishProto(pubProtoBuf, subject,
                    reply, msgSize);
            }

            try {
            bw.write(pubProtoBuf, 0, pubProtoLen);

            if (msgSize > 0)
            {
                bw.write(data, 0, msgSize);
            }

            bw.write(crlfProtoBytes, 0, crlfProtoBytesLen);
            } catch (IOException e) {
            	
            }
            
            stats.outMsgs++;
            stats.outBytes += msgSize;

            kickFlusher();
        } finally {
        	mu.unlock();
        }

    } // publish
	
	@Override
	public void publish(String subject, byte[] data) throws ConnectionClosedException {
		publish(subject, null, data);
	}

	@Override
	public void publish(Message msg) throws ConnectionClosedException {
		publish(msg.getSubject(), msg.getReplyTo(), msg.getData());
	}

	@Override
	public Message request(String subject, byte[] data, long timeout) 
			throws ConnectionClosedException, BadSubscriptionException, 
			SlowConsumerException, MaxMessagesException, IOException {
		Message m		= null;
		String inbox 	= newInbox();

		SyncSubscription s = subscribeSync(inbox, null);
		s.autoUnsubscribe(1);

		publish(subject, inbox, data);
		m = s.nextMsg(timeout);
		try
		{
			// the auto unsubscribe should handle this.
			s.unsubscribe();
		}
		catch (Exception e) {  /* NOOP */ }

		return m;	
	}

	@Override
	public Message request(String subject, byte[] data) throws ConnectionClosedException, BadSubscriptionException, SlowConsumerException, MaxMessagesException, IOException {
		
		return request(subject, data, 0);
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
		return info.maxPayload;
	}

}
