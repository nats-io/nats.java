/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBJECT;
import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Constants.ERR_BAD_TIMEOUT;
import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_CONNECTION_READ;
import static io.nats.client.Constants.ERR_MAX_PAYLOAD;
import static io.nats.client.Constants.ERR_NO_SERVERS;
import static io.nats.client.Constants.ERR_PROTOCOL;
import static io.nats.client.Constants.ERR_RECONNECT_BUF_EXCEEDED;
import static io.nats.client.Constants.ERR_SECURE_CONN_REQUIRED;
import static io.nats.client.Constants.ERR_SECURE_CONN_WANTED;
import static io.nats.client.Constants.ERR_SLOW_CONSUMER;
import static io.nats.client.Constants.ERR_STALE_CONNECTION;
import static io.nats.client.Constants.ERR_TCP_FLUSH_FAILED;
import static io.nats.client.Constants.ERR_TIMEOUT;
import static io.nats.client.Constants.PERMISSIONS_ERR;
import static io.nats.client.Constants.TLS_SCHEME;

import io.nats.client.Constants.ConnState;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionImpl implements Connection {
    final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    String version = null;

    private static final String inboxPrefix = "_INBOX.";

    public ConnState status = ConnState.DISCONNECTED;

    protected static final String STALE_CONNECTION = "Stale Connection";
    protected static final String THREAD_POOL = "natsthreadpool";

    // Default language string for CONNECT message
    protected static final String LANG_STRING = "java";

    // The size of the read buffer in readLoop.
    protected static final int DEFAULT_BUF_SIZE = 65536;
    // The size of the BufferedInputStream and BufferedOutputStream on top of the socket.
    protected static final int DEFAULT_STREAM_BUF_SIZE = 65536;

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
    public static final String SUB_PROTO = "SUB %s%s %d" + _CRLF_;
    public static final String UNSUB_PROTO = "UNSUB %d %s" + _CRLF_;
    public static final String OK_PROTO = _OK_OP_ + _CRLF_;

    protected static enum ClientProto {
        CLIENT_PROTO_ZERO(0), // CLIENT_PROTO_ZERO is the original client protocol from 2009.
        CLIENT_PROTO_INFO(1); // clientProtoInfo signals a client can receive more then the original
        // INFO block. This can be used to update clients on other cluster
        // members, etc.
        private final int value;

        private ClientProto(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private ConnectionImpl nc = null;
    protected final Lock mu = new ReentrantLock();
    // protected final Lock mu = new AlternateDeadlockDetectingLock(true, true);

    private AtomicLong sidCounter = new AtomicLong();
    private URI url = null;
    protected Options opts = null;

    private TCPConnectionFactory tcf = null;
    TCPConnection conn = null;

    // Prepare protocol messages for efficiency
    ByteBuffer pubProtoBuf = null;

    // we have a buffered reader for writing, and reading.
    // This is for both performance, and having to work around
    // interlinked read/writes (supported by the underlying network
    // stream, but not the BufferedStream).

    // private BufferedOutputStream bw = null;
    private OutputStream bw = null;

    private InputStream br = null;
    private ByteArrayOutputStream pending = null;

    private ReentrantLock flusherLock = new ReentrantLock();
    private boolean flusherDone = false;

    protected Map<Long, SubscriptionImpl> subs = new ConcurrentHashMap<Long, SubscriptionImpl>();
    protected List<Srv> srvPool = null;
    protected Map<String, URI> urls = null;
    private Exception lastEx = null;
    private ServerInfo info = null;
    // private Vector<Thread> socketWatchers = new Vector<Thread>();
    private int pout;

    protected Parser parser = new Parser(this);
    protected Parser.ParseState ps = parser.new ParseState();

    // protected MsgArg msgArgs = null;
    protected byte[] pingProtoBytes = null;
    protected int pingProtoBytesLen = 0;
    protected byte[] pongProtoBytes = null;
    protected int pongProtoBytesLen = 0;
    protected byte[] pubPrimBytes = null;
    protected int pubPrimBytesLen = 0;

    protected byte[] crlfProtoBytes = null;
    protected int crlfProtoBytesLen = 0;

    protected Statistics stats = null;
    private ArrayList<BlockingQueue<Boolean>> pongs;


    ExecutorService cbexec;
    ExecutorService exec;
    private ScheduledExecutorService ptmr = null;
    private static final int NUM_WATCHER_THREADS = 2;
    private CountDownLatch socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);
    private CountDownLatch socketWatchersDoneLatch = null;
    private BlockingQueue<Boolean> fch;

    ConnectionImpl() {}

    ConnectionImpl(Options opts) {
        this(opts, null);
    }

    ConnectionImpl(Options opts, TCPConnectionFactory connFac) {
        Properties props = this.getProperties(Constants.PROP_PROPERTIES_FILENAME);
        version = props.getProperty(Constants.PROP_CLIENT_VERSION);

        this.nc = this;
        this.opts = opts;
        this.stats = new Statistics();
        // this.msgArgs = new MsgArg();
        if (connFac != null) {
            tcf = connFac;
        } else {
            tcf = new TCPConnectionFactory();
        }
        setTcpConnection(tcf.createConnection());

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

    void setup() {
        cbexec = Executors.newSingleThreadExecutor(new NATSThreadFactory(THREAD_POOL));
        exec = Executors.newCachedThreadPool(new NATSThreadFactory(THREAD_POOL));
        fch = createBooleanChannel(FLUSH_CHAN_SIZE);
        pongs = createPongs();
        subs.clear();
    }

    protected Properties getProperties(InputStream inputStream) {
        Properties rv = new Properties();
        try {
            if (inputStream == null) {
                rv = null;
            } else {
                rv.load(inputStream);
            }
        } catch (IOException e) {
            logger.warn("nats: error loading properties from InputStream", e);
            rv = null;
        }
        return rv;
    }

    protected Properties getProperties(String resourceName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName);
        return getProperties(is);
    }

    private void buildPublishProtocolBuffer(int size) {
        pubProtoBuf = ByteBuffer.allocate(size);
        pubProtoBuf.put(pubPrimBytes, 0, pubPrimBytesLen);
        pubProtoBuf.mark();
        // System.arraycopy(pubPrimBytes, 0, pubProtoBuf, 0, pubPrimBytesLen);
    }


    /*
     * Create the server pool using the options given. We will place a Url option first, followed by
     * any Srv Options. We will randomize the server pool (except Url) unless the NoRandomize flag
     * is set.
     */
    protected void setupServerPool() {

        final URI url = opts.getUrl();
        List<URI> servers = opts.getServers();

        srvPool = new ArrayList<Srv>();
        urls = new ConcurrentHashMap<String, URI>();

        if (servers != null) {
            for (URI s : servers) {
                addUrlToPool(s);
            }
        }

        if (!opts.isNoRandomize()) {
            // Randomize the order
            Collections.shuffle(srvPool, new Random(System.nanoTime()));
        }

        /*
         * Insert the supplied url, if not null or empty, at the beginning of the list. Normally, if
         * this is set, then opts.servers should NOT be set, and vice versa. However, we always
         * allowed both to be set before, so we'll continue to do so.
         */

        if (url != null) {
            srvPool.add(0, new Srv(url));
            urls.put(url.getAuthority(), url);
        }

        // If the pool is empty, add the default URL
        if (srvPool.isEmpty()) {
            addUrlToPool(ConnectionFactory.DEFAULT_URL);
        }

        /*
         * At this point, srvPool being empty would be programmer error.
         */

        // Return the first server in the list
        this.setUrl(srvPool.get(0).url);
    }

    /* Add a string URL to the server pool */
    void addUrlToPool(String srvUrl) {
        URI uri = URI.create(srvUrl);
        srvPool.add(new Srv(uri));
        urls.put(uri.getAuthority(), uri);
    }

    /* Add a URL to the server pool */
    void addUrlToPool(URI uri) {
        srvPool.add(new Srv(uri));
        urls.put(uri.getAuthority(), uri);
    }

    protected Srv currentServer() {
        Srv rv = null;
        for (Srv s : srvPool) {
            if (s.url.equals(this.getUrl())) {
                rv = s;
                break;
            }
        }
        return rv;
    }

    protected Srv selectNextServer() throws IOException {
        logger.trace("In selectNextServer()");
        Srv srv = currentServer();
        if (srv == null) {
            throw new IOException(ERR_NO_SERVERS);
        }
        /**
         * Pop the current server and put onto the end of the list. Select head of list as long as
         * number of reconnect attempts under MaxReconnect.
         */
        logger.trace("selectNextServer, removing {}", srv);
        srvPool.remove(srv);

        /**
         * if the maxReconnect is unlimited, or the number of reconnect attempts is less than
         * maxReconnect, move the current server to the end of the list.
         *
         */
        int maxReconnect = opts.getMaxReconnect();
        if ((maxReconnect < 0) || (srv.reconnects < maxReconnect)) {
            logger.trace("selectNextServer: adding {}, maxReconnect: {}", srv, maxReconnect);
            srvPool.add(srv);
        }

        if (srvPool.isEmpty()) {
            this.setUrl(null);
            throw new IOException(ERR_NO_SERVERS);
        }

        return srvPool.get(0);
    }

    protected void connect() throws IOException, TimeoutException {
        // Create actual socket connection
        // For first connect we walk all servers in the pool and try
        // to connect immediately.
        // boolean connected = false;
        IOException returnedErr = null;
        mu.lock();
        try {
            for (int i = 0; i < srvPool.size(); i++) {
                Srv srv = srvPool.get(i);
                this.setUrl(srv.url);

                try {
                    logger.debug("Connecting to {}", this.getUrl());
                    createConn();
                    logger.debug("Connected to {}", this.getUrl());
                    this.setup();
                    try {
                        processConnectInit();
                        logger.trace("connect() Resetting reconnects for {}", srv);
                        srv.reconnects = 0;
                        returnedErr = null;
                        break;
                    } catch (IOException e) {
                        returnedErr = e;
                        // e.printStackTrace();
                        logger.trace("{} Exception: {}", this.getUrl(), e.getMessage());
                        mu.unlock();
                        close(ConnState.DISCONNECTED, false);
                        mu.lock();
                        this.setUrl(null);
                    }
                } catch (IOException e) { // createConn failed
                    if (e instanceof SocketException) {
                        if (e.getMessage() != null) {
                            if (e.getMessage().contains("Connection refused")) {
                                setLastError(null);
                            }
                        }
                    }
                }
            } // for

            if ((returnedErr == null) && (this.status != ConnState.CONNECTED)) {
                returnedErr = new IOException(ERR_NO_SERVERS);
            }

            if (returnedErr != null) {
                throw (returnedErr);
            }
        } finally {
            mu.unlock();
        }
    }

    // createConn will connect to the server and wrap the appropriate
    // bufio structures. A new connection is always created.
    protected void createConn() throws IOException {
        Srv srv = currentServer();
        if (srv == null) {
            throw new IOException(ERR_NO_SERVERS);
        } else {
            srv.updateLastAttempt();
        }

        logger.trace("createConn(): {}", srv.url);

        try {
            logger.trace("Opening {}", srv.url);
            conn = tcf.createConnection();
            conn.open(srv.url.getHost(), srv.url.getPort(), opts.getConnectionTimeout());
            logger.trace("Opened {}", srv.url);
        } catch (IOException e) {
            logger.trace("Couldn't establish connection to {}: {}", srv.url, e.getMessage());
            throw (e);
        }

        if ((pending != null) && (bw != null)) {
            logger.trace("Flushing old outputstream to pending");
            try {
                bw.flush();
            } catch (IOException e) {
                logger.warn(ERR_TCP_FLUSH_FAILED);
            }
        }
        bw = conn.getBufferedOutputStream(DEFAULT_STREAM_BUF_SIZE);
        br = conn.getBufferedInputStream(DEFAULT_STREAM_BUF_SIZE);
    }


    BlockingQueue<Message> createMsgChannel() {
        return new LinkedBlockingQueue<Message>();
    }

    BlockingQueue<Message> createMsgChannel(int size) {
        int theSize = size;
        if (theSize <= 0) {
            theSize = 1;
        }
        return new LinkedBlockingQueue<Message>(theSize);
    }

    BlockingQueue<Boolean> createBooleanChannel() {
        return new LinkedBlockingQueue<Boolean>();
    }

    BlockingQueue<Boolean> createBooleanChannel(int size) {
        int theSize = size;
        if (theSize <= 0) {
            theSize = 1;
        }
        return new LinkedBlockingQueue<Boolean>(theSize);
    }

    // This will clear any pending flush calls and release pending calls.
    // Lock is assumed to be held by the caller.
    void clearPendingFlushCalls() {
        // Clear any queued pongs, e.g. pending flush calls.
        if (pongs == null) {
            return;
        }
        for (BlockingQueue<Boolean> ch : pongs) {
            if (ch != null) {
                ch.clear();
                // Signal other waiting threads that we're done
                ch.add(false);
            }
        }
        pongs.clear();
        pongs = null;
    }

    @Override
    public void close() {
        close(ConnState.CLOSED, true);
    }

    // Low level close call that will do correct cleanup and set
    // desired status. Also controls whether user defined callbacks
    // will be triggered. The lock should not be held entering this
    // function. This function will handle the locking manually.
    private void close(ConnState closeState, boolean doCBs) {
        logger.debug("close({}, {})", closeState, String.valueOf(doCBs));
        final ConnectionImpl nc = this;

        mu.lock();
        if (_isClosed()) {
            this.status = closeState;
            mu.unlock();
            return;
        }
        this.status = ConnState.CLOSED;

        // Kick the Flusher routines so they fall out.
        kickFlusher();
        mu.unlock();

        mu.lock();
        try {
            // Clear any queued pongs, e.g. pending flush calls.
            clearPendingFlushCalls();

            // Interrupt any blocking operations
            // Thread.currentThread().interrupt();

            if (ptmr != null) {
                ptmr.shutdownNow();
            }

            // Go ahead and make sure we have flushed the outbound
            if (conn != null) {
                try {
                    if (bw != null) {
                        bw.flush();
                    }
                } catch (IOException e) {
                    /* NOOP */
                }
            }

            logger.trace("Closing subscriptions");
            // Close sync subscriber channels and release any
            // pending nextMsg() calls.
            for (Long key : subs.keySet()) {
                SubscriptionImpl sub = subs.get(key);
                sub.lock();
                sub.closeChannel();
                // Mark as invalid, for signaling to deliverMsgs
                sub.closed = true;
                // Mark connection closed in subscription
                sub.connClosed = true;
                // Terminate thread exec
                sub.close();
                sub.unlock();
            }
            subs.clear();

            // perform appropriate callback if needed for a
            // disconnect;
            if (doCBs) {
                if (opts.getDisconnectedCallback() != null && conn != null) {
                    cbexec.execute(new Runnable() {
                        public void run() {
                            opts.getDisconnectedCallback().onDisconnect(new ConnectionEvent(nc));
                            logger.trace("executed DisconnectedCB");
                        }
                    });
                }
                if (opts.getClosedCallback() != null) {
                    cbexec.execute(new Runnable() {

                        public void run() {
                            opts.getClosedCallback().onClose(new ConnectionEvent(nc));
                            logger.trace("executed ClosedCB");
                        }

                    });
                }
            }

            this.status = closeState;
        } finally {
            if (conn != null) {
                conn.close();
            }

            // if (exec != null) {
            // exec.shutdownNow();
            // }
            mu.unlock();
        }
    }

    protected void processConnectInit() throws IOException {
        logger.trace("processConnectInit(): {}", this.getUrl());

        // Set our status to connecting.
        status = ConnState.CONNECTING;

        // Process the INFO protocol that we should be receiving
        processExpectedInfo();

        // Send the CONNECT and PING protocol, and wait for the PONG.
        sendConnect();

        // Reset the number of PINGs sent out
        this.setActualPingsOutstanding(0);

        // Start the readLoop and flusher threads
        spinUpSocketWatchers();
    }

    // This will check to see if the connection should be
    // secure. This can be dictated from either end and should
    // only be called after the INIT protocol has been received.
    private void checkForSecure() throws IOException {
        // Check to see if we need to engage TLS
        // Check for mismatch in setups
        if (opts.isSecure() && !info.isTlsRequired()) {
            throw new IOException(ERR_SECURE_CONN_WANTED);
        } else if (info.isTlsRequired() && !opts.isSecure()) {
            throw new IOException(ERR_SECURE_CONN_REQUIRED);
        }

        // Need to rewrap with bufio
        if (opts.isSecure() || TLS_SCHEME.equals(this.getUrl().getScheme())) {
            makeTLSConn();
        }
    }

    // makeSecureConn will wrap an existing Conn using TLS
    void makeTLSConn() throws IOException {
        conn.setTlsDebug(opts.isTlsDebug());
        conn.makeTLS(opts.getSSLContext());
        bw = conn.getBufferedOutputStream(DEFAULT_STREAM_BUF_SIZE);
        br = conn.getBufferedInputStream(DEFAULT_STREAM_BUF_SIZE);
    }

    protected void processExpectedInfo() throws IOException {
        Control control;

        try {
            control = readOp();
        } catch (IOException e) {
            processOpError(e);
            return;
        }

        if (!control.op.equals(_INFO_OP_)) {
            throw new IOException(ERR_PROTOCOL + ", INFO not received");
        }

        processInfo(control.args);
        checkForSecure();

    }

    // processPing will send an immediate pong protocol response to the
    // server. The server uses this mechanism to detect dead clients.
    protected void processPing() {
        try {
            sendProto(pongProtoBytes, pongProtoBytesLen);
        } catch (IOException e) {
            setLastError(e);
            // e.printStackTrace();
        }
    }

    // processPong is used to process responses to the client's ping
    // messages. We use pings for the flush mechanism as well.
    protected void processPong() {
        logger.trace("Processing PONG");
        BlockingQueue<Boolean> ch = createBooleanChannel(1);
        mu.lock();
        try {
            if (pongs.size() > 0) {
                ch = pongs.get(0);
                pongs.remove(0);
            }
            setActualPingsOutstanding(0);
        } finally {
            mu.unlock();
        }
        if (ch != null) {
            try {
                ch.put(true);
            } catch (InterruptedException e) {
                logger.warn("processPong interrupted");
                Thread.currentThread().interrupt();
            }
        }
        logger.trace("Processed PONG");
    }

    // processOK is a placeholder for processing OK messages.
    protected void processOk() {
        // NOOP;
        return;
    }

    // processInfo is used to parse the info messages sent
    // from the server.
    protected void processInfo(String infoString) {
        if ((infoString == null) || infoString.isEmpty()) {
            return;
        }

        this.info = ServerInfo.createFromWire(infoString);

        boolean updated = false;

        if (info.getConnectUrls() != null) {
            for (String s : info.getConnectUrls()) {
                if (!urls.containsKey(s)) {
                    this.addUrlToPool(String.format("nats://%s", s));
                    updated = true;
                }
            }

            if (updated && !opts.isNoRandomize()) {
                Collections.shuffle(srvPool);
            }
        }
        return;
    }

    // processAsyncInfo does the same than processInfo, but is called
    // from the parser. Calls processInfo under connection's lock
    // protection.
    void processAsyncInfo(String asyncInfoString) {
        mu.lock();
        // Ignore errors, we will simply not update the server pool...
        processInfo(asyncInfoString);
        mu.unlock();
    }

    // processOpError handles errors from reading or parsing the protocol.
    // This is where disconnect/reconnect is initially handled.
    // The lock should not be held entering this function.
    void processOpError(Exception err) {
        mu.lock();
        try {
            if (isConnecting() || _isClosed() || _isReconnecting()) {
                return;
            }

            if (opts.isReconnectAllowed() && status == ConnState.CONNECTED) {
                // Set our new status
                status = ConnState.RECONNECTING;

                if (ptmr != null) {
                    ptmr.shutdownNow();
                }

                if (this.conn != null) {
                    try {
                        bw.flush();
                    } catch (IOException e1) {
                        logger.error("I/O error during flush", e1);
                    }
                    conn.close();
                }

                // Create a new pending buffer to underpin the buffered output
                // stream while we are reconnecting.
                logger.trace("processOpError: redirecting output to pending buffer");

                setPending(new ByteArrayOutputStream(opts.getReconnectBufSize()));
                setOutputStream(getPending());

                logger.trace("\t\tspawning doReconnect() in state {}", status);
                exec.submit(new Runnable() {
                    public void run() {
                        Thread.currentThread().setName("reconnect");
                        doReconnect();
                    }
                });

                logger.trace("\t\tspawned doReconnect() in state {}", status);
                return;
            } else {
                logger.trace("\t\tcalling processDisconnect() in state {}", status);
                processDisconnect();
                setLastError(err);
                close();
            }
        } finally {
            mu.unlock();
        }
    }

    protected void processDisconnect() {
        logger.trace("processDisconnect()");
        status = ConnState.DISCONNECTED;
    }

    @Override
    public boolean isReconnecting() {
        mu.lock();
        boolean rv = _isReconnecting();
        mu.unlock();
        return rv;
    }

    boolean _isReconnecting() {
        return (status == ConnState.RECONNECTING);
    }

    // Test if Conn is connected or connecting.
    private boolean isConnected() {
        return (status == ConnState.CONNECTING || status == ConnState.CONNECTED);
    }

    @Override
    public boolean isClosed() {
        mu.lock();
        boolean rv = _isClosed();
        mu.unlock();
        return rv;
    }

    boolean _isClosed() {
        return (status == ConnState.CLOSED);
    }

    // flushReconnectPending will push the pending items that were
    // gathered while we were in a RECONNECTING state to the socket.
    protected void flushReconnectPendingItems() {
        logger.trace("flushReconnectPendingItems()");
        if (pending == null) {
            return;
        }

        if (pending.size() > 0) {
            try {
                logger.trace("flushReconnectPendingItems() writing {} bytes.", pending.size());
                bw.write(pending.toByteArray(), 0, (int) pending.size());
                bw.flush();
            } catch (IOException e) {
                logger.error("Error flushing pending items", e);
            }
        }

        pending = null;
        logger.trace("flushReconnectPendingItems() DONE");
    }

    // Try to reconnect using the option parameters.
    // This function assumes we are allowed to reconnect.
    void doReconnect() {
        logger.trace("doReconnect()");
        // We want to make sure we have the other watchers shutdown properly
        // here before we proceed past this point
        waitForExits();

        // FIXME(dlc) - We have an issue here if we have
        // outstanding flush points (pongs) and they were not
        // sent out, but are still in the pipe.

        // Hold the lock manually and release where needed below.
        mu.lock();

        // Clear any queued pongs, e.g. pending flush calls.
        nc.clearPendingFlushCalls();

        // Clear any errors.
        setLastError(null);

        // Perform appropriate callback if needed for a disconnect
        if (opts.getDisconnectedCallback() != null) {
            logger.trace("Spawning disconnectCB from doReconnect()");
            cbexec.execute(new Runnable() {
                public void run() {
                    opts.getDisconnectedCallback().onDisconnect(new ConnectionEvent(nc));
                }
            });
            logger.trace("Spawned disconnectCB from doReconnect()");
        }

        while (!srvPool.isEmpty()) {
            Srv cur = null;
            try {
                cur = selectNextServer();
                this.setUrl(cur.url);
            } catch (IOException nse) {
                logger.trace("doReconnect() calling setLastError({})", nse.getMessage());
                setLastError(nse);
                break;
            }

            // Sleep appropriate amount of time before the
            // connection attempt if connecting to same server
            // we just got disconnected from.

            long elapsedMillis = cur.timeSinceLastAttempt();
            if (elapsedMillis < opts.getReconnectWait()) {
                long sleepTime = opts.getReconnectWait() - elapsedMillis;
                mu.unlock();
                sleepMsec((int) sleepTime);
                mu.lock();
            }

            // Check if we have been closed first.
            if (isClosed()) {
                logger.debug("Connection has been closed while in doReconnect()");
                break;
            }

            // Mark that we tried a reconnect
            cur.reconnects++;

            logger.trace("doReconnect() incremented cur.reconnects: {}", cur);
            logger.trace("doReconnect: trying createConn() for {}", cur);

            // try to create a new connection
            try {
                conn.teardown();
                createConn();
                logger.trace("doReconnect: createConn() successful for {}", cur);
            } catch (Exception e) {
                conn.teardown();
                logger.trace("doReconnect: createConn() failed for {}", cur);
                logger.trace("createConn failed", e);
                // not yet connected, retry and hold
                // the lock.
                setLastError(null);
                continue;
            }

            // We are reconnected.
            stats.incrementReconnects();

            // Process connect logic
            try {
                processConnectInit();
            } catch (IOException e) {
                conn.teardown();
                logger.warn("doReconnect: processConnectInit FAILED for {}", cur, e);
                setLastError(e);
                status = ConnState.RECONNECTING;
                continue;
            }

            logger.trace("Successful reconnect; Resetting reconnects for {}", cur);

            // Clear out server stats for the server we connected to..
            // cur.didConnect = true;
            cur.reconnects = 0;

            // Send existing subscription state
            resendSubscriptions();

            // Now send off and clear pending buffer
            flushReconnectPendingItems();
            logger.debug("just called flushReconnectPendingItems");
            // Flush the buffer
            try {
                getOutputStream().flush();
            } catch (IOException e) {
                logger.debug("Error flushing output stream");
                setLastError(e);
                status = ConnState.RECONNECTING;
                continue;
            }

            // Done with the pending buffer
            setPending(null);

            // This is where we are truly connected.
            status = ConnState.CONNECTED;

            // Queue up the reconnect callback.
            if (opts.getReconnectedCallback() != null) {
                // TODO This mirrors go, and so does not spawn a thread/task.
                logger.trace("Spawning reconnectedCB from doReconnect()");
                cbexec.execute(new Runnable() {

                    public void run() {
                        opts.getReconnectedCallback().onReconnect(new ConnectionEvent(nc));

                    }
                });
                logger.trace("Spawned reconnectedCB from doReconnect()");
            }

            // Release the lock here, we will return below
            mu.unlock();

            // Make sure to flush everything
            try {
                flush();
            } catch (Exception e) {
                logger.warn("Error flushing connection", e);
            }
            logger.trace("doReconnect reconnected successfully!");
            return;
        } // while

        logger.trace("Reconnect FAILED");

        // Call into close.. We have no servers left.
        if (getLastException() == null) {
            setLastError(new IOException(ERR_NO_SERVERS));
        }

        mu.unlock();
        logger.trace("Calling   close() from doReconnect()");
        close();
        logger.trace("Completed close() from doReconnect()");
    }

    boolean isConnecting() {
        mu.lock();
        boolean rv = (status == ConnState.CONNECTING);
        mu.unlock();
        return rv;
    }

    static String normalizeErr(String error) {
        String str = error;
        str = str.replaceFirst(_ERR_OP_ + "\\s+", "").toLowerCase();
        str = str.replaceAll("^\'|\'$", "");
        return str;
    }

    static String normalizeErr(ByteBuffer error) {
        String str = Parser.bufToString(error).trim();
        return normalizeErr(str);
    }

    // processErr processes any error messages from the server and
    // sets the connection's lastError.
    protected void processErr(ByteBuffer error) {
        // boolean doCBs = false;
        NATSException ex = null;
        String err = normalizeErr(error);

        logger.trace("processErr(error={})", err);

        if (STALE_CONNECTION.equalsIgnoreCase(err)) {
            processOpError(new IOException(ERR_STALE_CONNECTION));
        } else if (err.startsWith(PERMISSIONS_ERR)) {
            processPermissionsViolation(err);
        } else {
            ex = new NATSException("nats: " + err);
            ex.setConnection(this);
            mu.lock();
            setLastError(ex);
            mu.unlock();
            close();
        }
    }

    // caller must lock
    protected void sendConnect() throws IOException {
        String line = null;

        logger.trace("sendConnect()");

        // Send CONNECT
        bw.write(connectProto().getBytes());
        logger.trace("=> {}", connectProto().trim());
        bw.flush();

        // Process +OK
        if (opts.isVerbose()) {
            line = readLine();
            if (!_OK_OP_.equals(line)) {
                throw new IOException(
                        String.format("nats: expected '%s', got '%s'", _OK_OP_, line));
            }
        }

        // Send PING
        bw.write(pingProtoBytes, 0, pingProtoBytesLen);
        logger.trace("=> {}", new String(pingProtoBytes).trim());
        bw.flush();

        // Now read the response from the server.
        try {
            logger.trace("Awaiting PONG...");
            line = readLine();
        } catch (IOException e) {
            throw new IOException(ERR_CONNECTION_READ, e);
        }

        // We expect a PONG
        if (!PONG_PROTO.trim().equals(line)) {
            // But it could be something else, like -ERR

            // If it's a server error...
            if (line.startsWith(_ERR_OP_)) {
                // Remove -ERR, trim spaces and quotes, and convert to lower case.
                line = normalizeErr(line);
                throw new IOException("nats: " + line);
            }

            // Notify that we got an unexpected protocol.
            throw new IOException(String.format("nats: expected '%s', got '%s'", _PONG_OP_, line));
        }

        // This is where we are truly connected.
        status = ConnState.CONNECTED;
    }

    // This function is only used during the initial connection process
    protected String readLine() throws IOException {
        BufferedReader breader = conn.getBufferedReader();
        String s = null;
        logger.trace("readLine() Reading from input stream");
        s = breader.readLine();
        if (s == null) {
            throw new EOFException(ERR_CONNECTION_CLOSED);
        }
        logger.trace("<= {}", s != null ? s.trim() : "null");
        return s;
    }

    /*
     * This method is only used by processPing. It is also used in the gnatsd tests.
     */
    protected void sendProto(byte[] value, int length) throws IOException {
        logger.trace("in sendProto()");
        mu.lock();
        try {
            logger.trace("in sendProto(), writing");
            bw.write(value, 0, length);
            logger.trace("=> {}", new String(value).trim());
            kickFlusher();
        } finally {
            mu.unlock();
        }
    }

    // Generate a connect protocol message, issuing user/password if
    // applicable. The lock is assumed to be held upon entering.
    String connectProto() {
        String userInfo = getUrl().getUserInfo();
        String user = null;
        String pass = null;
        String token = null;

        if (userInfo != null) {
            // if no password, assume username is authToken
            String[] userpass = userInfo.split(":");
            if (userpass[0].length() > 0) {
                switch (userpass.length) {
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
        } else {
            // Take from options (possibly all empty strings)
            user = opts.getUsername();
            pass = opts.getPassword();
            token = opts.getToken();
        }

        ConnectInfo info = new ConnectInfo(opts.isVerbose(), opts.isPedantic(), user, pass, token,
                opts.isSecure(), opts.getConnectionName(), LANG_STRING, version,
                ClientProto.CLIENT_PROTO_INFO);

        String result = String.format(CONN_PROTO, info);
        return result;
    }

    protected Control readOp() throws IOException {
        // This is only used when creating a connection, so simplify
        // life and just create a BufferedReader to read the incoming
        // info string.
        //
        // Do not close the BufferedReader; let TCPConnection manage it.
        String str = readLine();
        Control control = new Control(str);
        logger.trace("readOp returning: " + control);
        return control;
    }

    // waitForExits will wait for all socket watcher threads to
    // complete before proceeding.
    private void waitForExits() {
        logger.trace("waitForExits()");
        // Kick old flusher forcefully.
        setFlusherDone(true);
        kickFlusher();

        if (socketWatchersDoneLatch != null) {
            try {
                socketWatchersDoneLatch.await();
            } catch (InterruptedException e) {
                logger.warn("nats: interrupted waiting for threads to exit");
                Thread.currentThread().interrupt();
            }
        }
        logger.trace("waitForExits complete");
    }

    protected void spinUpSocketWatchers() {
        logger.trace("Spinning up threads");
        // Make sure everything has exited.
        waitForExits();

        socketWatchersDoneLatch = new CountDownLatch(NUM_WATCHER_THREADS);
        socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);

        exec.execute(new Runnable() {
            public void run() {
                logger.trace("READLOOP STARTING");
                Thread.currentThread().setName("readloop");
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for threads to spin up");
                    Thread.currentThread().interrupt();
                }
                readLoop();
                socketWatchersDoneLatch.countDown();
                logger.trace("READLOOP EXITING");
            }
        });

        exec.execute(new Runnable() {
            public void run() {
                logger.trace("FLUSHER STARTING");
                Thread.currentThread().setName("flusher");
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for start latch");
                    Thread.currentThread().interrupt();
                }
                flusher();
                socketWatchersDoneLatch.countDown();
                logger.trace("FLUSHER EXITING");
            }
        });

        socketWatchersStartLatch.countDown();

        resetPingTimer();

    }

    // protected Thread go(final Runnable task, final String name, final String group,
    // final Phaser ph) {
    // NATSThread.setDebug(true);
    // NATSThread t = new NATSThread(task, name) {
    // public void run() {
    // if (ph != null) {
    // ph.register();
    // logger.trace("{} registered in group {}. # registered for phase {} = {}", name,
    // group, ph.getPhase(), ph.getRegisteredParties());
    // logger.trace(name + " starting");
    // ph.arriveAndAwaitAdvance(); // await all creation
    // } else {
    // logger.trace("Untracked thread " + name + " starting.");
    // }
    //
    // task.run();
    //
    // if (ph != null) {
    // int oldPhase = ph.getPhase();
    // logger.trace(name + " arrive and deregister for phase {}", ph.getPhase());
    // logger.trace(
    // "{} (group {}) ending phase {}: Registered = {}, Arrived = {}, Unarrived={}",
    // name, group, oldPhase, ph.getRegisteredParties(),
    // ph.getArrivedParties(), ph.getUnarrivedParties());
    // int phase = ph.arriveAndDeregister();
    // logger.trace(name + " deregistered going into phase {}", phase);
    // } else {
    // logger.trace("Untracked thread " + name + " completed.");
    // }
    // }
    // };
    // t.start();
    // NATSThread.setDebug(false);
    // return t;
    // }

    protected class Control {
        String op = null;
        String args = null;

        protected Control(String s) {
            if (s == null) {
                return;
            }

            String[] parts = s.split(" ", 2);

            switch (parts.length) {
                case 1:
                    op = parts[0].trim();
                    break;
                case 2:
                    op = parts[0].trim();
                    args = parts[1].trim();
                    if (args.isEmpty()) {
                        args = null;
                    }
                    break;
                default:
            }
        }

        public String toString() {
            return "{op=" + op + ", args=" + args + "}";
        }
    }

    class ConnectInfo {
        @SerializedName("verbose")
        private Boolean verbose;

        @SerializedName("pedantic")
        private Boolean pedantic;

        @SerializedName("user")
        private String user;

        @SerializedName("pass")
        private String pass;

        @SerializedName("auth_token")
        private String token;

        @SerializedName("tls_required")
        private Boolean tlsRequired;

        @SerializedName("name")
        private String name;

        @SerializedName("lang")
        private String lang = ConnectionImpl.LANG_STRING;

        @SerializedName("version")
        private String version = ConnectionImpl.this.version;

        @SerializedName("protocol")
        private int protocol;

        private transient Gson gson = new GsonBuilder().create();

        public ConnectInfo(boolean verbose, boolean pedantic, String username, String password,
                String token, boolean secure, String connectionName, String lang, String version,
                ClientProto proto) {
            this.verbose = new Boolean(verbose);
            this.pedantic = new Boolean(pedantic);
            this.user = username;
            this.pass = password;
            this.token = token;
            this.tlsRequired = new Boolean(secure);
            this.name = connectionName;
            this.lang = lang;
            this.version = version;
            this.protocol = proto.getValue();
        }

        public String toString() {
            return gson.toJson(this);
        }
    }

    class Srv {
        URI url = null;
        int reconnects = 0;
        long lastAttempt = 0L;
        long lastAttemptNanos = 0L;
        boolean secure = false;

        protected Srv(URI url) {
            this.url = url;
            if (url.getScheme().equals(TLS_SCHEME)) {
                this.secure = true;
            }
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
            String dateToStr = format.format(new Date(lastAttempt));

            return String.format(
                    "{url=%s, reconnects=%d, lastAttempt=%s, timeSinceLastAttempt=%dms}",
                    url.toString(), reconnects, dateToStr, timeSinceLastAttempt());
        }
    }

    protected void readLoop() {
        Parser parser = null;
        int len;
        boolean sb;
        // stack copy
        TCPConnection conn = null;

        mu.lock();
        parser = this.parser;
        this.ps = parser.ps;
        mu.unlock();

        // Stack based buffer.
        byte[] buffer = new byte[DEFAULT_BUF_SIZE];

        while (true) {
            mu.lock();
            try {
                sb = (_isClosed() || _isReconnecting());
                if (sb) {
                    this.ps = parser.new ParseState();
                }
                conn = this.conn;
            } finally {
                mu.unlock();
            }

            if (sb || conn == null) {
                break;
            }

            try {
                len = br.read(buffer);
                // len = br.read(buffer, 0, DEFAULT_BUF_SIZE);
                if (len == -1) {
                    throw new IOException(ERR_STALE_CONNECTION);
                }
                parser.parse(buffer, len);
            } catch (IOException | ParseException e) {
                logger.trace("Exception in readLoop(): ConnState was {}", status, e);
                if (status != ConnState.CLOSED) {
                    processOpError(e);
                }
                break;
            }
        }

        mu.lock();
        this.ps = null;
        mu.unlock();
    }

    /**
     * waitForMsgs waits on the conditional shared with readLoop and processMsg. It is used to
     * deliver messages to asynchronous subscribers.
     * 
     * @param sub the asynchronous subscriber
     * @throws InterruptedException if the thread is interrupted
     */
    protected void waitForMsgs(AsyncSubscriptionImpl sub) throws InterruptedException {
        boolean closed;
        long delivered = 0L;
        long max;
        Message msg;
        MessageHandler mcb;
        BlockingQueue<Message> mch;

        while (true) {
            sub.lock();
            try {
                mch = sub.getChannel();
                while (mch.size() == 0 && !sub.isClosed()) {
                    sub.pCond.await();
                }
                msg = mch.poll();
                if (msg != null) {
                    sub.pMsgs--;
                    sub.pBytes -= (msg.getData() == null ? 0 : msg.getData().length);
                }

                mcb = sub.msgHandler;
                max = sub.max;
                closed = sub.isClosed();
                if (!closed) {
                    sub.delivered++;
                    delivered = sub.delivered;
                }
            } finally {
                sub.unlock();
            }

            if (closed) {
                break;
            }
            // Deliver the message.
            if (msg != null && (max <= 0 || delivered <= max)) {
                mcb.onMessage(msg);
            }
            // If we have hit the max for delivered msgs, remove sub.
            if (max > 0 && delivered >= max) {
                mu.lock();
                try {
                    removeSub(sub);
                } finally {
                    mu.unlock();
                }
                break;
            }
        }
    }

    // deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
    // It is used to deliver messages to asynchronous subscribers.
    // This function is the run() method of the AsyncSubscription's msgFeeder thread.
    // protected void deliverMsgs(Channel<Message> ch) {
    // // logger.trace("In deliverMsgs");
    // Message msg = null;
    //
    // mu.lock();
    // // Slightly faster to do this directly vs call isClosed
    // if (_isClosed()) {
    // mu.unlock();
    // return;
    // }
    // mu.unlock();
    //
    // while (true) {
    // msg = ch.get();
    //
    // if (msg == null) {
    // // the channel has been closed, exit silently.
    // logger.debug("Channel closed, exiting msgFeeder loop");
    // return;
    // }
    // // Note, this seems odd message having the sub process itself,
    // // but this is good for performance.
    // if (!msg.sub.processMsg(msg)) {
    // mu.lock();
    // try {
    // removeSub(msg.sub);
    // } finally {
    // mu.unlock();
    // }
    // }
    // }
    // }

    /**
     * processMsg is called by parse and will place the msg on the appropriate channel/pending queue
     * for processing. If the channel is full, or the pending queue is over the pending limits, the
     * connection is considered a slow consumer.
     * 
     * @param data the buffer containing the message body
     * @param offset the offset within this buffer of the beginning of the message body
     * @param length the length of the message body
     */
    protected void processMsg(byte[] data, int offset, int length) {
        SubscriptionImpl sub;

        mu.lock();
        try {
            stats.incrementInMsgs();
            stats.incrementInBytes(length);

            sub = subs.get(ps.ma.sid);
            if (sub == null) {
                return;
            }

            // Doing message create outside of the sub's lock to reduce contention.
            // It's possible that we end up not using the message, but that's ok.
            Message msg = new Message(ps.ma, sub, data, offset, length);

            sub.lock();
            try {
                sub.pMsgs++;
                if (sub.pMsgs > sub.pMsgsMax) {
                    sub.pMsgsMax = sub.pMsgs;
                }
                sub.pBytes += (msg.getData() == null ? 0 : msg.getData().length);
                if (sub.pBytes > sub.pBytesMax) {
                    sub.pBytesMax = sub.pBytes;
                }

                // Check for a Slow Consumer
                if ((sub.pMsgsLimit > 0 && sub.pMsgs > sub.pMsgsLimit)
                        || (sub.pBytesLimit > 0 && sub.pBytes > sub.pBytesLimit)) {
                    handleSlowConsumer(sub, msg);
                    return;
                } else {
                    // We use mch for everything, unlike Go client
                    if (sub.getChannel() != null) {
                        if (sub.getChannel().add(msg)) {
                            sub.pCond.signal();
                        } else {
                            handleSlowConsumer(sub, msg);
                            return;
                        }
                    }
                }

                // Clear Slow Consumer status
                sub.setSlowConsumer(false);
            } finally {
                sub.unlock();
            }
        } finally {
            mu.unlock();
        }
    }

    // Assumes you already have the lock
    protected void handleSlowConsumer(SubscriptionImpl sub, Message msg) {
        sub.dropped++;
        processSlowConsumer(sub);
        sub.pMsgs--;
        if (msg.getData() != null) {
            sub.pBytes -= msg.getData().length;
        }
    }

    void removeSub(SubscriptionImpl sub) {
        subs.remove(sub.getSid());
        sub.lock();
        try {
            if (sub.getChannel() != null) {
                sub.mch.clear();
                sub.mch = null;
            }

            // Mark as invalid
            sub.setConnection(null);
            sub.closed = true;
        } finally {
            sub.unlock();
        }
    }

    // processSlowConsumer will set SlowConsumer state and fire the
    // async error handler if registered.
    void processSlowConsumer(SubscriptionImpl sub) {
        final IOException ex = new IOException(ERR_SLOW_CONSUMER);
        final NATSException nex = new NATSException(ex, this, sub);
        setLastError(ex);
        if (opts.getExceptionHandler() != null && !sub.isSlowConsumer()) {
            cbexec.execute(new Runnable() {
                public void run() {
                    opts.getExceptionHandler().onException(nex);
                }
            });
        }
        sub.setSlowConsumer(true);
    }

    void processPermissionsViolation(String err) {
        final IOException serverEx = new IOException("nats: " + err);
        final NATSException nex = new NATSException(serverEx);
        nex.setConnection(this);
        setLastError(serverEx);
        if (opts.getExceptionHandler() != null) {
            cbexec.execute(new Runnable() {
                public void run() {
                    opts.getExceptionHandler().onException(nex);
                }
            });
        }
    }

    // FIXME: This is a hack
    // removeFlushEntry is needed when we need to discard queued up responses
    // for our pings as part of a flush call. This happens when we have a flush
    // call outstanding and we call close.
    protected boolean removeFlushEntry(BlockingQueue<Boolean> ch) {
        mu.lock();
        try {
            if (pongs == null) {
                return false;
            }

            for (BlockingQueue<Boolean> c : pongs) {
                if (c == ch) {
                    c.clear();
                    pongs.remove(c);
                    return true;
                }

            }
            return false;
        } finally {
            mu.unlock();
        }
    }

    // The lock must be held entering this function.
    protected void sendPing(BlockingQueue<Boolean> ch) {
        if (pongs == null) {
            pongs = createPongs();
        }

        if (ch != null) {
            pongs.add(ch);
        }

        try {
            bw.write(pingProtoBytes, 0, pingProtoBytesLen);
            logger.trace("=> {}", new String(pingProtoBytes).trim());
            bw.flush();
        } catch (IOException e) {
            setLastError(e);
        }
    }

    ArrayList<BlockingQueue<Boolean>> createPongs() {
        return new ArrayList<BlockingQueue<Boolean>>();
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
                if (ptmr != null) {
                    ptmr.shutdownNow();
                }

                ptmr = Executors.newSingleThreadScheduledExecutor(new NATSThreadFactory("pinger"));
                ptmr.scheduleAtFixedRate(pingRunnable, opts.getPingInterval(),
                        opts.getPingInterval(), TimeUnit.MILLISECONDS);
            }
        } finally {
            mu.unlock();
        }
    }

    // This will fire periodically and send a client origin
    // ping to the server. Will also check that we have received
    // responses from the server.
    protected void processPingTimer() {
        mu.lock();
        if (status != ConnState.CONNECTED) {
            mu.unlock();
            return;
        }

        // Check for violation
        setActualPingsOutstanding(getActualPingsOutstanding() + 1);
        if (getActualPingsOutstanding() > opts.getMaxPingsOut()) {
            mu.unlock();
            processOpError(new IOException(ERR_STALE_CONNECTION));
            return;
        }

        logger.trace("Sending PING after {} seconds.",
                TimeUnit.MILLISECONDS.toSeconds(opts.getPingInterval()));
        sendPing(null);

        mu.unlock();
    }

    protected void unsubscribe(SubscriptionImpl sub, int max) throws IOException {
        unsubscribe(sub, (long) max);
    }

    protected void writeUnsubProto(SubscriptionImpl sub, long max) throws IOException {
        String str = String.format(UNSUB_PROTO, sub.getSid(), max > 0 ? Long.toString(max) : "");
        str = str.replaceAll(" +\r\n", "\r\n");
        byte[] unsub = str.getBytes();
        bw.write(unsub);
        logger.trace("=> {}", str.trim());
    }

    // unsubscribe performs the low level unsubscribe to the server.
    // Use SubscriptionImpl.unsubscribe()
    protected void unsubscribe(SubscriptionImpl sub, long max) throws IOException {
        mu.lock();
        try {
            if (isClosed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            SubscriptionImpl s = subs.get(sub.getSid());
            // already unsubscribed
            if (s == null) {
                return;
            }

            // If the autounsubscribe max is > 0, set that on the subscription
            if (max > 0) {
                s.setMax(max);
            } else {
                removeSub(s);
            }

            // We will send all subscriptions when reconnecting
            // so that we can suppress here.
            if (!_isReconnecting()) {
                writeUnsubProto(s, max);
            }
        } finally {
            kickFlusher();
            mu.unlock();
        }
    }

    protected void kickFlusher() {
        if (bw != null) {
            if (fch != null) {
                if (fch.size() == 0) {
                    if (!fch.offer(true)) {
                        logger.warn("Unable to add to flusher channel");
                    }
                }
            }
        }
    }

    private void setFlusherDone(boolean value) {
        flusherLock.lock();
        try {
            flusherDone = value;

            if (flusherDone) {
                kickFlusher();
            }
        } finally {
            flusherLock.unlock();
        }
    }

    boolean isFlusherDone() {
        flusherLock.lock();
        try {
            return flusherDone;
        } finally {
            flusherLock.unlock();
        }
    }

    // This is the loop of the flusher thread
    protected void flusher() {
        OutputStream bw = null;
        TCPConnection conn = null;
        BlockingQueue<Boolean> fch = null;

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

        while (!isFlusherDone()) {
            // Wait to be triggered
            try {
                if (!fch.take()) {
                    return;
                }
            } catch (InterruptedException e1) {
                logger.debug("Interrupted while waiting on flush channel");
                Thread.currentThread().interrupt();
                break;
            }

            // Be reasonable about how often we flush
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            mu.lock();
            try {

                // Check to see if we should bail out.
                if (!isConnected() || isConnecting() || bw != this.bw || conn != this.conn) {
                    return;
                }
                bw.flush();
                stats.incrementFlushes();
            } catch (IOException e) {
                logger.error("I/O exception encountered during flush", e);
            } finally {
                mu.unlock();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.nats.client.AbstractConnection#flush(int)
     */
    @Override
    public void flush(int timeout) throws Exception {
        Exception err = null;
        if (timeout <= 0) {
            throw new IllegalArgumentException(ERR_BAD_TIMEOUT);
        }

        BlockingQueue<Boolean> ch = null;
        mu.lock();
        try {
            if (_isClosed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            ch = createBooleanChannel(1);
            sendPing(ch);
        } finally {
            mu.unlock();
        }

        Boolean rv = null;
        while (!(Thread.currentThread().isInterrupted())) {
            try {
                rv = ch.poll(timeout, TimeUnit.MILLISECONDS);
                if (rv == null) {
                    err = new TimeoutException(ERR_TIMEOUT);
                } else if (rv == true) {
                    ch.clear();
                } else {
                    err = new IllegalStateException(ERR_CONNECTION_CLOSED);
                }
                break;
            } catch (InterruptedException e) {
                // Set interrupted flag.
                logger.debug("flush was interrupted while waiting for PONG", e);
                Thread.currentThread().interrupt();
            }
        }

        if (err != null) {
            this.removeFlushEntry(ch);
            throw err;
        }
    }


    /// Flush will perform a round trip to the server and return when it
    /// receives the internal reply.
    @Override
    public void flush() throws Exception {
        // 60 second default.
        flush(60000);
    }

    // resendSubscriptions will send our subscription state back to the
    // server. Used in reconnects
    protected void resendSubscriptions() {
        long adjustedMax = 0L;
        for (Long key : subs.keySet()) {
            SubscriptionImpl sub = subs.get(key);
            if (sub instanceof AsyncSubscription) {
                ((AsyncSubscriptionImpl) sub).start(); // enableAsyncProcessing()
            }
            logger.trace("Resending subscriptions:");
            sub.lock();
            try {
                logger.trace("Sub = {}", sub);
                if (sub.max > 0) {
                    if (sub.delivered < sub.max) {
                        adjustedMax = sub.max - sub.delivered;
                    }
                    // adjustedMax could be 0 here if the number of delivered msgs
                    // reached the max, if so unsubscribe.
                    if (adjustedMax == 0) {
                        // s.mu.unlock();
                        try {
                            unsubscribe(sub, 0);
                        } catch (Exception e) {
                            /* NOOP */
                        }
                        continue;
                    }
                }
            } finally {
                sub.unlock();
            }

            sendSubscriptionMessage(sub);
            if (adjustedMax > 0) {
                try {
                    // cannot call unsubscribe here. Need to just send proto
                    writeUnsubProto(sub, adjustedMax);
                } catch (Exception e) {
                    /* NOOP */
                }
            }
        }
        // bw.flush();
    }

    /**
     * subscribe is the internal subscribe function that indicates interest in a subject.
     * 
     * @param subj the subject
     * @param queue an optional subscription queue
     * @param cb async callback
     * @param ch channel
     * @return the Subscription object
     */
    SubscriptionImpl subscribe(String subject, String queue, MessageHandler cb,
            BlockingQueue<Message> ch) {
        final SubscriptionImpl sub;
        mu.lock();
        try {
            // Check for some error conditions.
            if (_isClosed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            if (cb == null && ch == null) {
                throw new IllegalArgumentException(ERR_BAD_SUBSCRIPTION);
            }
        } finally {
            kickFlusher();
            mu.unlock();
        }

        // Create subscription with pending limits
        if (cb != null) {
            sub = new AsyncSubscriptionImpl(this, subject, queue, cb);
            // If we have an async callback, start up a sub specific Runnable to deliver the
            // messages
            exec.submit(new Runnable() {
                public void run() {
                    try {
                        waitForMsgs((AsyncSubscriptionImpl) sub);
                    } catch (InterruptedException e) {
                        logger.debug("Interrupted in waitForMsgs");
                        Thread.currentThread().interrupt();
                    }
                }
            });
        } else {
            sub = new SyncSubscriptionImpl(this, subject, queue);
            sub.setChannel(ch);
        }

        // Sets sid and adds to subs map
        addSubscription(sub);

        // Send SUB proto
        sendSubscriptionMessage(sub);

        return sub;
    }

    @Override
    public SyncSubscription subscribe(String subject) {
        return subscribeSync(subject, null);
    }

    @Override
    public SyncSubscription subscribe(String subject, String queue) {
        return subscribeSync(subject, queue);
    }

    @Override
    public AsyncSubscription subscribe(String subject, MessageHandler cb) {
        return (AsyncSubscriptionImpl) subscribe(subject, null, cb);
    }

    @Override
    public AsyncSubscription subscribe(String subj, String queue, MessageHandler cb) {
        return (AsyncSubscriptionImpl) subscribe(subj, queue, cb, null);
    }

    @Override
    @Deprecated
    public AsyncSubscription subscribeAsync(String subject, String queue, MessageHandler cb) {
        return (AsyncSubscriptionImpl) subscribe(subject, queue, cb, null);
    }

    @Override
    @Deprecated
    public AsyncSubscription subscribeAsync(String subj, MessageHandler cb) {
        return (AsyncSubscriptionImpl) subscribe(subj, null, cb);
    }

    private void addSubscription(SubscriptionImpl sub) {
        sub.setSid(sidCounter.incrementAndGet());
        subs.put(sub.getSid(), sub);
        logger.trace("Successfully added subscription to {} [{}]", sub.getSubject(), sub.getSid());
    }

    @Override
    public SyncSubscription subscribeSync(String subject, String queue) {
        return (SyncSubscription) subscribe(subject, queue, (MessageHandler) null,
                createMsgChannel());
    }

    @Override
    public SyncSubscription subscribeSync(String subject) {
        return (SyncSubscription) subscribe(subject, null, (MessageHandler) null,
                createMsgChannel());
    }

    // Use low level primitives to build the protocol for the publish
    // message.
    void writePublishProto(ByteBuffer buffer, byte[] subject, byte[] reply, int msgSize) {
        pubProtoBuf.put(subject, 0, subject.length);
        if (reply != null) {
            pubProtoBuf.put((byte) ' ');
            pubProtoBuf.put(reply, 0, reply.length);
        }
        pubProtoBuf.put((byte) ' ');

        byte[] bytes = new byte[12];
        int idx = bytes.length;
        if (msgSize > 0) {
            for (int l = msgSize; l > 0; l /= 10) {
                idx--;
                bytes[idx] = digits[l % 10];
            }
        } else {
            idx -= 1;
            bytes[idx] = digits[0];
        }
        pubProtoBuf.put(bytes, idx, bytes.length - idx);
        pubProtoBuf.put(crlfProtoBytes, 0, crlfProtoBytesLen);
    }

    // Used for handrolled itoa
    static final byte[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    void _publish(byte[] subject, byte[] reply, byte[] data) throws IOException {
        int msgSize = (data != null) ? data.length : 0;
        mu.lock();
        try {
            // Proactively reject payloads over the threshold set by server.
            if (msgSize > info.getMaxPayload()) {
                throw new IllegalArgumentException(ERR_MAX_PAYLOAD);
            }

            // Since we have the lock, examine directly for a tiny performance
            // boost in fastpath
            if (_isClosed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            // TODO implement reconnect buffer size option
            // Check if we are reconnecting, and if so check if
            // we have exceeded our reconnect outbound buffer limits.
            if (_isReconnecting()) {
                // Flush to underlying buffer
                try {
                    bw.flush();
                } catch (IOException e) {
                    logger.error("I/O exception during flush", e);
                }
                if (pending.size() >= opts.getReconnectBufSize()) {
                    throw new IOException(ERR_RECONNECT_BUF_EXCEEDED);
                }
            }

            // write our pubProtoBuf buffer to the buffered writer.
            try {
                writePublishProto(pubProtoBuf, subject, reply, msgSize);
            } catch (BufferOverflowException e) {
                // We can get here if we have very large subjects.
                // Expand with some room to spare.
                logger.warn("nats: reallocating publish buffer due to overflow");
                int resizeAmount = Parser.MAX_CONTROL_LINE_SIZE + subject.length
                        + (reply != null ? reply.length : 0);

                buildPublishProtocolBuffer(resizeAmount);

                writePublishProto(pubProtoBuf, subject, reply, msgSize);
            }

            try {
                bw.write(pubProtoBuf.array(), 0, pubProtoBuf.position());
                pubProtoBuf.position(pubPrimBytesLen);

                if (msgSize > 0) {
                    bw.write(data, 0, msgSize);
                }

                bw.write(crlfProtoBytes, 0, crlfProtoBytesLen);
            } catch (IOException e) {
                setLastError(e);
                return;
            }

            kickFlusher();

            stats.incrementOutMsgs();
            stats.incrementOutBytes(msgSize);
        } finally {
            mu.unlock();
        }
    }

    // Sends a protocol data message by queueing into the bufio writer
    // and kicking the flush go routine. These writes should be protected.
    // publish can throw a few different unchecked exceptions:
    // IllegalStateException, IllegalArgumentException, NullPointerException
    @Override
    public void publish(String subject, String reply, byte[] data) throws IOException {
        if (subject == null) {
            throw new NullPointerException(ERR_BAD_SUBJECT);
        }
        if (subject.isEmpty()) {
            throw new IllegalArgumentException(ERR_BAD_SUBJECT);
        }

        byte[] subjBytes = subject.getBytes();
        byte[] replyBytes = null;
        if (reply != null) {
            replyBytes = reply.getBytes();
        }
        _publish(subjBytes, replyBytes, data);
    } // publish

    @Override
    public void publish(String subject, byte[] data) throws IOException {
        publish(subject, null, data);
    }

    @Override
    public void publish(Message msg) throws IOException {
        _publish(msg.getSubjectBytes(), msg.getReplyToBytes(), msg.getData());
    }

    private Message _request(String subject, byte[] data, long timeout, TimeUnit unit)
            throws TimeoutException, IOException {
        String inbox = newInbox();
        Message msg = null;
        SyncSubscription sub = subscribeSync(inbox, null);
        sub.autoUnsubscribe(1);
        publish(subject, inbox, data);
        msg = sub.nextMessage(timeout, unit);
        sub.close();

        return msg;
    }

    @Override
    public Message request(String subject, byte[] data, long timeout)
            throws TimeoutException, IOException {
        return request(subject, data, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message request(String subject, byte[] data, long timeout, TimeUnit unit)
            throws TimeoutException, IOException {
        // logger.trace("In request({},{},{})", subject,
        // data==null?"null":new String(data), timeout);
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be greater that 0.");
        }

        return _request(subject, data, timeout, unit);
    }

    @Override
    public Message request(String subject, byte[] data) throws IOException, TimeoutException {
        return _request(subject, data, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public String newInbox() {
        String inbox = String.format("%s%s", inboxPrefix, NUID.nextGlobal());
        return inbox;
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
        try {
            // We will send these for all subs when we reconnect
            // so that we can suppress here.
            if (!_isReconnecting()) {
                String queue = sub.getQueue();
                String subLine = String.format(SUB_PROTO, sub.getSubject(),
                        (queue != null && !queue.isEmpty()) ? " " + queue : "", sub.getSid());
                try {
                    bw.write(Utilities.stringToBytesASCII(subLine));
                    // logger.trace("=> {}", s.trim() );
                    kickFlusher();
                } catch (IOException e) {
                    logger.warn("nats: I/O exception while sending subscription message");
                }
            }
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ClosedCallback getClosedCallback() {
        return opts.getClosedCallback();
    }

    @Override
    public void setClosedCallback(ClosedCallback cb) {
        mu.lock();
        try {
            opts.setClosedCallback(cb);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public DisconnectedCallback getDisconnectedCallback() {
        return opts.getDisconnectedCallback();
    }

    @Override
    public void setDisconnectedCallback(DisconnectedCallback cb) {
        mu.lock();
        try {
            opts.setDisconnectedCallback(cb);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ReconnectedCallback getReconnectedCallback() {
        return opts.getReconnectedCallback();
    }

    @Override
    public void setReconnectedCallback(ReconnectedCallback cb) {
        mu.lock();
        try {
            opts.setReconnectedCallback(cb);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        return opts.getExceptionHandler();
    }

    @Override
    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        mu.lock();
        try {
            opts.setExceptionHandler(exceptionHandler);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public String getConnectedUrl() {
        mu.lock();
        try {
            if (status != ConnState.CONNECTED) {
                return null;
            }
            return getUrl().toString();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public String getConnectedServerId() {
        mu.lock();
        try {
            if (status != ConnState.CONNECTED) {
                return null;
            }
            return info.getId();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ConnState getState() {
        mu.lock();
        try {
            return this.status;
        } finally {
            mu.unlock();
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

    void setLastError(Exception err) {
        this.lastEx = err;
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

    protected void sleepMsec(long msec) {
        try {
            logger.trace("Sleeping for {} ms", msec);
            Thread.sleep(msec);
            logger.trace("Slept    for {} ms", msec);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void setOutputStream(OutputStream out) {
        mu.lock();
        try {
            this.bw = out;
        } finally {
            mu.unlock();
        }
    }

    OutputStream getOutputStream() {
        return bw;
    }

    void setInputStream(InputStream in) {
        mu.lock();
        try {
            this.br = in;
        } finally {
            mu.unlock();
        }
    }

    InputStream getInputStream() {
        return br;
    }


    protected ArrayList<BlockingQueue<Boolean>> getPongs() {
        return pongs;
    }

    protected void setPongs(ArrayList<BlockingQueue<Boolean>> pongs) {
        this.pongs = pongs;
    }

    protected Map<Long, SubscriptionImpl> getSubs() {
        return subs;
    }

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

    @Override
    public int getPendingByteCount() {
        int rv = 0;
        if (getPending() != null) {
            rv = getPending().size();
        }
        return rv;
    }

    protected void setFlushChannel(BlockingQueue<Boolean> fch) {
        this.fch = fch;
    }

    protected BlockingQueue<Boolean> getFlushChannel() {
        return fch;
    }

    protected void setTcpConnection(TCPConnection conn) {
        this.conn = conn;
    }

    protected TCPConnection getTcpConnection() {
        return this.conn;
    }

    protected void setTcpConnectionFactory(TCPConnectionFactory factory) {
        this.tcf = factory;
    }

    protected TCPConnectionFactory getTcpConnectionFactory() {
        return this.tcf;
    }

    URI getUrl() {
        return url;
    }

    void setUrl(URI url) {
        this.url = url;
    }

    int getActualPingsOutstanding() {
        return pout;
    }

    void setActualPingsOutstanding(int pout) {
        this.pout = pout;
    }
}
