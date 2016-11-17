/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.ConnState;
import static io.nats.client.Nats.ConnState.CLOSED;
import static io.nats.client.Nats.ConnState.CONNECTED;
import static io.nats.client.Nats.ConnState.CONNECTING;
import static io.nats.client.Nats.ConnState.DISCONNECTED;
import static io.nats.client.Nats.ConnState.RECONNECTING;
import static io.nats.client.Nats.ERR_BAD_SUBJECT;
import static io.nats.client.Nats.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Nats.ERR_BAD_TIMEOUT;
import static io.nats.client.Nats.ERR_CONNECTION_CLOSED;
import static io.nats.client.Nats.ERR_CONNECTION_READ;
import static io.nats.client.Nats.ERR_MAX_PAYLOAD;
import static io.nats.client.Nats.ERR_NO_INFO_RECEIVED;
import static io.nats.client.Nats.ERR_NO_SERVERS;
import static io.nats.client.Nats.ERR_RECONNECT_BUF_EXCEEDED;
import static io.nats.client.Nats.ERR_SECURE_CONN_REQUIRED;
import static io.nats.client.Nats.ERR_SECURE_CONN_WANTED;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;
import static io.nats.client.Nats.ERR_STALE_CONNECTION;
import static io.nats.client.Nats.ERR_TCP_FLUSH_FAILED;
import static io.nats.client.Nats.ERR_TIMEOUT;
import static io.nats.client.Nats.PERMISSIONS_ERR;
import static io.nats.client.Nats.TLS_SCHEME;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionImpl implements Connection {
    private final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    private String version = null;

    private static final String INBOX_PREFIX = "_INBOX.";

    private ConnState status = DISCONNECTED;

    protected static final String STALE_CONNECTION = "Stale Connection";

    // Default language string for CONNECT message
    protected static final String LANG_STRING = "java";

    // The size of the read buffer in readLoop.
    protected static final int DEFAULT_BUF_SIZE = 65536;
    // The size of the BufferedInputStream and BufferedOutputStream on top of the socket.
    protected static final int DEFAULT_STREAM_BUF_SIZE = 65536;

    // The buffered size of the flush "kick" channel
    protected static final int FLUSH_CHAN_SIZE = 1;

    // The number of msec the flusher will wait between flushes
    private long flushTimerInterval = 1;
    private TimeUnit flushTimerUnit = TimeUnit.MICROSECONDS;


    protected static final String _CRLF_ = "\r\n";
    protected static final String _EMPTY_ = "";
    protected static final String _SPC_ = " ";
    protected static final String _PUB_P_ = "PUB ";

    // Operations
    protected static final String _OK_OP_ = "+OK";
    protected static final String _ERR_OP_ = "-ERR";
    protected static final String _MSG_OP_ = "MSG";
    protected static final String _PING_OP_ = "PING";
    protected static final String _PONG_OP_ = "PONG";
    protected static final String _INFO_OP_ = "INFO";

    // Message Prototypes
    protected static final String CONN_PROTO = "CONNECT %s" + _CRLF_;
    protected static final String PING_PROTO = "PING" + _CRLF_;
    protected static final String PONG_PROTO = "PONG" + _CRLF_;
    protected static final String PUB_PROTO = "PUB %s %s %d" + _CRLF_;
    protected static final String SUB_PROTO = "SUB %s%s %d" + _CRLF_;
    protected static final String UNSUB_PROTO = "UNSUB %d %s" + _CRLF_;
    protected static final String OK_PROTO = _OK_OP_ + _CRLF_;


    enum ClientProto {
        CLIENT_PROTO_ZERO(0), // CLIENT_PROTO_ZERO is the original client protocol from 2009.
        CLIENT_PROTO_INFO(1); // clientProtoInfo signals a client can receive more then the original
        // INFO block. This can be used to update clients on other cluster
        // members, etc.
        private final int value;

        ClientProto(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private ConnectionImpl nc = null;
    final Lock mu = new ReentrantLock();
    // protected final Lock mu = new AlternateDeadlockDetectingLock(true, true);

    private final AtomicLong sidCounter = new AtomicLong();
    private URI url = null;
    private Options opts = null;

    private TcpConnectionFactory tcf = null;
    private TcpConnection conn = null;

    // Prepare protocol messages for efficiency
    private ByteBuffer pubProtoBuf = null;

    // we have a buffered reader for writing, and reading.
    // This is for both performance, and having to work around
    // interlinked read/writes (supported by the underlying network
    // stream, but not the BufferedStream).

    private OutputStream bw = null;

    private InputStream br = null;
    private ByteArrayOutputStream pending = null;

    private Map<Long, SubscriptionImpl> subs = new ConcurrentHashMap<Long, SubscriptionImpl>();
    private List<Srv> srvPool = null;
    private Map<String, URI> urls = null;
    private Exception lastEx = null;
    private ServerInfo info = null;
    private int pout;

    private Parser parser = new Parser(this);

    private byte[] pingProtoBytes = null;
    private int pingProtoBytesLen = 0;
    private byte[] pongProtoBytes = null;
    private int pongProtoBytesLen = 0;
    private byte[] pubPrimBytes = null;
    private int pubPrimBytesLen = 0;

    private byte[] crlfProtoBytes = null;
    private int crlfProtoBytesLen = 0;

    private Statistics stats = null;
    private List<BlockingQueue<Boolean>> pongs;


    private static final int NUM_CORE_THREADS = 4;


    // The main executor service for core threads and timers
    private ScheduledExecutorService exec;
    static final String EXEC_NAME = "jnats-exec";

    // Executor for subscription threads
    private ExecutorService subexec;
    static final String SUB_EXEC_NAME = "jnats-subscriptions";

    // Executor for async connection callbacks
    private ExecutorService cbexec;
    static final String CB_EXEC_NAME = "jnats-callbacks";

    // The ping timer task
    private ScheduledFuture<?> ptmr = null;

    private final List<Future<?>> tasks = new ArrayList<>();
    private static final int NUM_WATCHER_THREADS = 2;
    private CountDownLatch socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);
    private CountDownLatch socketWatchersDoneLatch = null;

    // The flusher signalling channel
    private BlockingQueue<Boolean> fch;

    ConnectionImpl() {
    }

    ConnectionImpl(Options opts) {
        Properties props = this.getProperties(Nats.PROP_PROPERTIES_FILENAME);
        version = props.getProperty(Nats.PROP_CLIENT_VERSION);

        this.nc = this;
        this.opts = opts;
        this.stats = new Statistics();
        if (opts.getFactory() != null) {
            tcf = opts.getFactory();
        } else {
            tcf = new TcpConnectionFactory();
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

    ScheduledExecutorService createScheduler() {
        return Executors.newScheduledThreadPool(NUM_CORE_THREADS, new NatsThreadFactory(EXEC_NAME));
    }

    ExecutorService createSubscriptionScheduler() {
        return Executors.newCachedThreadPool(new NatsThreadFactory(SUB_EXEC_NAME));
    }

    ExecutorService createCallbackScheduler() {
        return Executors.newSingleThreadExecutor(new NatsThreadFactory(CB_EXEC_NAME));
    }

    void setup() {
        exec = createScheduler();
        subexec = createSubscriptionScheduler();
        fch = createFlushChannel();
        pongs = createPongs();
        subs.clear();
    }

    Properties getProperties(InputStream inputStream) {
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

    Properties getProperties(String resourceName) {
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
    void setupServerPool() {

        final URI url;
        if (opts.getUrl() != null) {
            url = URI.create(opts.getUrl());
        } else {
            url = null;
        }
        List<URI> servers = opts.getServers();

        srvPool = new ArrayList<Srv>();
        urls = new ConcurrentHashMap<String, URI>();

        if (servers != null) {
            for (URI s : servers) {
                addUrlToPool(s, false);
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
            srvPool.add(0, new Srv(url, false));
            urls.put(url.getAuthority(), url);
        }

        // If the pool is empty, add the default URL
        if (srvPool.isEmpty()) {
            addUrlToPool(Nats.DEFAULT_URL, false);
        }

        /*
         * At this point, srvPool being empty would be programmer error.
         */

        // Return the first server in the list
        this.setUrl(srvPool.get(0).url);
    }

    /* Add a string URL to the server pool */
    void addUrlToPool(String srvUrl, boolean implicit) {
        URI uri = URI.create(srvUrl);
        srvPool.add(new Srv(uri, implicit));
        urls.put(uri.getAuthority(), uri);
    }

    /* Add a URL to the server pool */
    void addUrlToPool(URI uri, boolean implicit) {
        srvPool.add(new Srv(uri, implicit));
        urls.put(uri.getAuthority(), uri);
    }

    Srv currentServer() {
        Srv rv = null;
        for (Srv s : srvPool) {
            if (s.url.equals(this.getUrl())) {
                rv = s;
                break;
            }
        }
        return rv;
    }

    Srv selectNextServer() throws IOException {
        Srv srv = currentServer();
        if (srv == null) {
            throw new IOException(ERR_NO_SERVERS);
        }
        /*
         * Pop the current server and put onto the end of the list. Select head of list as long as
         * number of reconnect attempts under MaxReconnect.
         */
        srvPool.remove(srv);

        /*
         * if the maxReconnect is unlimited, or the number of reconnect attempts is less than
         * maxReconnect, move the current server to the end of the list.
         *
         */
        int maxReconnect = opts.getMaxReconnect();
        if ((maxReconnect < 0) || (srv.reconnects < maxReconnect)) {
            srvPool.add(srv);
        }

        if (srvPool.isEmpty()) {
            this.setUrl(null);
            throw new IOException(ERR_NO_SERVERS);
        }

        return srvPool.get(0);
    }

    Connection connect() throws IOException {
        // Create actual socket connection
        // For first connect we walk all servers in the pool and try
        // to connect immediately.
        IOException returnedErr = null;
        mu.lock();
        try {
            for (Srv srv : srvPool) {
                this.setUrl(srv.url);

                try {
                    logger.debug("Connecting to {}", this.getUrl());
                    createConn();
                    logger.debug("Connected to {}", this.getUrl());
                    this.setup();
                    try {
                        processConnectInit();
                        srv.reconnects = 0;
                        returnedErr = null;
                        break;
                    } catch (IOException e) {
                        returnedErr = e;
                        mu.unlock();
                        close(DISCONNECTED, false);
                        mu.lock();
                        this.setUrl(null);
                    }
                } catch (IOException e) { // createConn failed
                    // Cancel out default connection refused, will trigger the
                    // No servers error conditional
                    if (e.getMessage() != null && e.getMessage().contains("Connection refused")) {
                            setLastError(null);
                    }
                }
            } // for

            if ((returnedErr == null) && (this.status != CONNECTED)) {
                returnedErr = new IOException(ERR_NO_SERVERS);
            }

            if (returnedErr != null) {
                throw (returnedErr);
            }

            cbexec = createCallbackScheduler();

            return this;
        } finally {
            mu.unlock();
        }
    }

    /*
     * createConn will connect to the server and wrap the appropriate bufio structures. A new
     * connection is always created.
     */
    void createConn() throws IOException {
        if (opts.getConnectionTimeout() < 0) {
            logger.warn("{}: {}", ERR_BAD_TIMEOUT, opts.getConnectionTimeout());
            throw new IOException(ERR_BAD_TIMEOUT);
        }
        Srv srv = currentServer();
        if (srv == null) {
            throw new IOException(ERR_NO_SERVERS);
        } else {
            srv.updateLastAttempt();
        }

        try {
            logger.debug("Opening {}", srv.url);
            conn = tcf.createConnection();
            conn.open(srv.url.toString(), opts.getConnectionTimeout());
            logger.trace("Opened {} as TcpConnection ({})", srv.url, conn);
        } catch (IOException e) {
            logger.debug("Couldn't establish connection to {}: {}", srv.url, e.getMessage());
            throw (e);
        }

        if ((pending != null) && (bw != null)) {
            try {
                bw.flush();
            } catch (IOException e) {
                logger.warn(ERR_TCP_FLUSH_FAILED);
            }
        }
        bw = conn.getOutputStream(DEFAULT_STREAM_BUF_SIZE);
        br = conn.getInputStream(DEFAULT_STREAM_BUF_SIZE);
    }


    BlockingQueue<Message> createMsgChannel() {
        return createMsgChannel(Integer.MAX_VALUE);
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

    BlockingQueue<Boolean> createFlushChannel() {
        return new LinkedBlockingQueue<Boolean>(FLUSH_CHAN_SIZE);
        // return new SynchronousQueue<Boolean>();
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
        close(CLOSED, true);
    }

    /*
     * Low level close call that will do correct cleanup and set desired status. Also controls
     * whether user defined callbacks will be triggered. The lock should not be held entering this
     * method. This method will handle the locking manually.
     */
    private void close(ConnState closeState, boolean doCBs) {
        logger.debug("close({}, {})", closeState, String.valueOf(doCBs));
        final ConnectionImpl nc = this;

        mu.lock();
        try {
            if (closed()) {
                this.status = closeState;
                return;
            }
            this.status = CLOSED;

            // Kick the Flusher routine so it falls out.
            kickFlusher();
        } finally {
            mu.unlock();
        }

        mu.lock();
        try {
            // Clear any queued pongs, e.g. pending flush calls.
            clearPendingFlushCalls();

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

            // Close sync subscribers and release any pending nextMsg() calls.
            for (Map.Entry<Long, SubscriptionImpl> entry : subs.entrySet()) {
                SubscriptionImpl sub = entry.getValue();
                // for (Long key : subs.keySet()) {
                // SubscriptionImpl sub = subs.get(key);
                sub.lock();
                try {
                    sub.closeChannel();
                    // Mark as invalid, for signaling to deliverMsgs
                    sub.closed = true;
                    // Mark connection closed in subscription
                    sub.connClosed = true;
                    // Terminate thread exec
                    sub.close();
                } finally {
                    sub.unlock();
                }
            }
            subs.clear();

            // perform appropriate callback if needed for a disconnect;
            if (doCBs) {
                if (opts.getDisconnectedCallback() != null && conn != null) {
                    cbexec.submit(new Runnable() {
                        @Override
                        public void run() {
                            opts.getDisconnectedCallback().onDisconnect(new ConnectionEvent(nc));
                            logger.trace("executed DisconnectedCB");
                        }
                    });
                }
                if (opts.getClosedCallback() != null) {
                    cbexec.submit(new Runnable() {
                        @Override
                        public void run() {
                            opts.getClosedCallback().onClose(new ConnectionEvent(nc));
                            logger.trace("executed ClosedCB");
                        }
                    });
                }
                if (cbexec != null) {
                    cbexec.shutdown();
                }
            }

            this.status = closeState;

            if (conn != null) {
                conn.close();
            }

            if (exec != null) {
                exec.shutdownNow();
            }

            if (subexec != null) {
                subexec.shutdown();
            }

        } finally {
            mu.unlock();
        }
    }

    void processConnectInit() throws IOException {

        // Set our status to connecting.
        status = CONNECTING;

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
    void checkForSecure() throws IOException {
        // Check to see if we need to engage TLS
        // Check for mismatch in setups
        if (opts.isSecure() && !info.isTlsRequired()) {
            throw new IOException(ERR_SECURE_CONN_WANTED);
        } else if (info.isTlsRequired() && !opts.isSecure()) {
            throw new IOException(ERR_SECURE_CONN_REQUIRED);
        }

        // Need to rewrap with bufio
        if (opts.isSecure() || TLS_SCHEME.equals(this.getUrl().getScheme())) {
            makeTlsConn();
        }
    }

    // makeSecureConn will wrap an existing Conn using TLS
    void makeTlsConn() throws IOException {
        conn.setTlsDebug(opts.isTlsDebug());
        conn.makeTls(opts.getSslContext());
        bw = conn.getOutputStream(DEFAULT_STREAM_BUF_SIZE);
        br = conn.getInputStream(DEFAULT_STREAM_BUF_SIZE);
    }

    void processExpectedInfo() throws IOException {
        Control control;

        try {
            // Read the protocol
            control = readOp();
        } catch (IOException e) {
            processOpError(e);
            return;
        }

        // The nats protocol should send INFO first always.
        if (!control.op.equals(_INFO_OP_)) {
            throw new IOException(ERR_NO_INFO_RECEIVED);
        }

        // Parse the protocol
        processInfo(control.args);

        checkForSecure();
    }

    // processPing will send an immediate pong protocol response to the
    // server. The server uses this mechanism to detect dead clients.
    void processPing() {
        try {
            sendProto(pongProtoBytes, pongProtoBytesLen);
        } catch (IOException e) {
            setLastError(e);
            // e.printStackTrace();
        }
    }

    // processPong is used to process responses to the client's ping
    // messages. We use pings for the flush mechanism as well.
    void processPong() {
        BlockingQueue<Boolean> ch = null;
        mu.lock();
        try {
            if (pongs != null && pongs.size() > 0) {
                ch = pongs.get(0);
                pongs.remove(0);
            }
            setActualPingsOutstanding(0);
        } finally {
            mu.unlock();
        }
        if (ch != null) {
            ch.add(true);
        }
    }

    // processOK is a placeholder for processing OK messages.
    void processOk() {
        // NOOP;
    }

    // processInfo is used to parse the info messages sent
    // from the server.
    void processInfo(String infoString) {
        if ((infoString == null) || infoString.isEmpty()) {
            return;
        }
        setConnectedServerInfo(ServerInfo.createFromWire(infoString));
        boolean updated = false;

        if (info.getConnectUrls() != null) {
            for (String s : info.getConnectUrls()) {
                if (!urls.containsKey(s)) {
                    this.addUrlToPool(String.format("nats://%s", s), true);
                    updated = true;
                }
            }

            if (updated && !opts.isNoRandomize()) {
                Collections.shuffle(srvPool);
            }
        }
    }

    // processAsyncInfo does the same as processInfo, but is called
    // from the parser. Calls processInfo under connection's lock
    // protection.
    void processAsyncInfo(byte[] asyncInfo, int offset, int length) {
        mu.lock();
        try {
            String theInfo = new String(asyncInfo, offset, length);
            // Ignore errors, we will simply not update the server pool...
            processInfo(theInfo);
        } finally {
            mu.unlock();
        }
    }

    // processOpError handles errors from reading or parsing the protocol.
    // This is where disconnect/reconnect is initially handled.
    // The lock should not be held entering this function.
    void processOpError(Exception err) {
        mu.lock();
        try {
            if (connecting() || closed() || reconnecting()) {
                return;
            }

            logger.debug("Connection terminated: {}", err.getMessage());

            if (opts.isReconnectAllowed() && status == CONNECTED) {
                // Set our new status
                status = RECONNECTING;

                if (ptmr != null) {
                    ptmr.cancel(true);
                    tasks.remove(ptmr);
                }

                if (this.conn != null) {
                    try {
                        bw.flush();
                    } catch (IOException e1) {
                        logger.error("I/O error during flush", e1);
                    }
                    conn.close();
                }

                if (fch != null && !fch.offer(false)) {
                    logger.debug("Coudn't kick flush channel following connection error");
                }

                // Create a new pending buffer to underpin the buffered output
                // stream while we are reconnecting.

                setPending(new ByteArrayOutputStream(opts.getReconnectBufSize()));
                setOutputStream(getPending());

                if (exec.isShutdown()) {
                    exec = createScheduler();
                }
                exec.submit(new Runnable() {
                    public void run() {
                        Thread.currentThread().setName("reconnect");
                        doReconnect();
                    }
                });
                if (cbexec.isShutdown()) {
                    cbexec = Executors
                            .newSingleThreadExecutor(new NatsThreadFactory("callback-thread"));
                }
            } else {
                processDisconnect();
                setLastError(err);
                close();
            }
        } finally {
            mu.unlock();
        }
    }

    protected void processDisconnect() {
        logger.debug("processDisconnect()");
        status = DISCONNECTED;
    }

    @Override
    public boolean isReconnecting() {
        mu.lock();
        try {
            return reconnecting();
        } finally {
            mu.unlock();
        }
    }

    boolean reconnecting() {
        return (status == RECONNECTING);
    }

    @Override
    public boolean isConnected() {
        mu.lock();
        try {
            return connected();
        } finally {
            mu.unlock();
        }
    }

    boolean connected() {
        return (status == CONNECTED);
    }

    @Override
    public boolean isClosed() {
        mu.lock();
        try {
            return closed();
        } finally {
            mu.unlock();
        }
    }

    boolean closed() {
        return (status == CLOSED);
    }

    // flushReconnectPending will push the pending items that were
    // gathered while we were in a RECONNECTING state to the socket.
    void flushReconnectPendingItems() {
        if (pending == null) {
            return;
        }

        if (pending.size() > 0) {
            try {
                bw.write(pending.toByteArray(), 0, pending.size());
                bw.flush();
            } catch (IOException e) {
                logger.error("Error flushing pending items", e);
            }
        }

        pending = null;
    }

    // Try to reconnect using the option parameters.
    // This function assumes we are allowed to reconnect.
    void doReconnect() {
        logger.trace("doReconnect()");
        // We want to make sure we have the other watchers shutdown properly
        // here before we proceed past this point
        waitForExits();
        logger.trace("Old threads have exited, proceeding with reconnect");

        // FIXME(dlc) - We have an issue here if we have
        // outstanding flush points (pongs) and they were not
        // sent out, but are still in the pipe.

        // Hold the lock manually and release where needed below.
        mu.lock();
        try {
            // Clear any queued pongs, e.g. pending flush calls.
            nc.clearPendingFlushCalls();

            // Clear any errors.
            setLastError(null);

            // Perform appropriate callback if needed for a disconnect
            if (opts.getDisconnectedCallback() != null) {
                logger.trace("Spawning disconnectCB from doReconnect()");
                cbexec.submit(new Runnable() {
                    public void run() {
                        opts.getDisconnectedCallback().onDisconnect(new ConnectionEvent(nc));
                    }
                });
                logger.trace("Spawned disconnectCB from doReconnect()");
            }

            while (!srvPool.isEmpty()) {
                Srv cur;
                try {
                    cur = selectNextServer();
                    this.setUrl(cur.url);
                } catch (IOException nse) {
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
                    try {
                        sleepInterval((int) sleepTime, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        logger.debug("Interrupted while in doReconnect()");
                        break;
                    } finally {
                        mu.lock();
                    }
                }

                // Check if we have been closed first.
                if (isClosed()) {
                    logger.debug("Connection has been closed while in doReconnect()");
                    break;
                }

                // Mark that we tried a reconnect
                cur.reconnects++;

                // try to create a new connection
                try {
                    conn.teardown();
                    createConn();
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
                    logger.warn("couldn't connect to {} ({})", cur.url, e.getMessage());
                    setLastError(e);
                    status = RECONNECTING;
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

                // Flush the buffer
                try {
                    getOutputStream().flush();
                } catch (IOException e) {
                    logger.debug("Error flushing output stream");
                    setLastError(e);
                    status = RECONNECTING;
                    continue;
                }

                // Done with the pending buffer
                setPending(null);

                // This is where we are truly connected.
                status = CONNECTED;

                // Queue up the reconnect callback.
                if (opts.getReconnectedCallback() != null) {
                    logger.trace("Scheduling reconnectedCb from doReconnect()");
                    cbexec.submit(new Runnable() {

                        public void run() {
                            opts.getReconnectedCallback().onReconnect(new ConnectionEvent(nc));
                        }
                    });
                    logger.trace("Scheduled reconnectedCb from doReconnect()");
                }

                // Release the lock here, we will return below
                mu.unlock();
                try {
                    // Make sure to flush everything
                    flush();
                } catch (Exception e) {
                    if (status == CONNECTED) {
                        logger.warn("Error flushing connection", e);
                    }
                } finally {
                    mu.lock();
                }
                return;
            } // while

            logger.trace("Reconnect FAILED");

            // Call into close.. We have no servers left.
            if (getLastException() == null) {
                setLastError(new IOException(ERR_NO_SERVERS));
            }
        } finally {
            mu.unlock();
        }

        close();
    }

    boolean connecting() {
        return (status == CONNECTING);
    }

    ConnState status() {
        return status;
    }

    static String normalizeErr(String error) {
        String str = error;
        if (str != null) {
            str = str.replaceFirst(_ERR_OP_ + "\\s+", "").toLowerCase();
            str = str.replaceAll("^\'|\'$", "");
        }
        return str;
    }

    static String normalizeErr(ByteBuffer error) {
        String str = Parser.bufToString(error);
        if (str != null) {
            str = str.trim();
        }
        return normalizeErr(str);
    }

    // processErr processes any error messages from the server and
    // sets the connection's lastError.
    void processErr(ByteBuffer error) {
        // boolean doCBs = false;
        NATSException ex;
        String err = normalizeErr(error);

        if (STALE_CONNECTION.equalsIgnoreCase(err)) {
            processOpError(new IOException(ERR_STALE_CONNECTION));
        } else if (err.startsWith(PERMISSIONS_ERR)) {
            processPermissionsViolation(err);
        } else {
            ex = new NATSException("nats: " + err);
            ex.setConnection(this);
            mu.lock();
            try {
                setLastError(ex);
            } finally {
                mu.unlock();
            }
            close();
        }
    }

    // caller must lock
    protected void sendConnect() throws IOException {
        String line;

        // Send CONNECT
        bw.write(connectProto().getBytes());
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
        bw.flush();

        // Now read the response from the server.
        try {
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
        status = CONNECTED;
    }

    // This function is only used during the initial connection process
    String readLine() throws IOException {
        BufferedReader breader = conn.getBufferedReader();
        String line;
        line = breader.readLine();
        if (line == null) {
            throw new EOFException(ERR_CONNECTION_CLOSED);
        }
        return line;
    }

    /*
     * This method is only used by processPing. It is also used in the gnatsd tests.
     */
    void sendProto(byte[] value, int length) throws IOException {
        mu.lock();
        try {
            bw.write(value, 0, length);
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

        return String.format(CONN_PROTO, info);
    }

    Control readOp() throws IOException {
        // This is only used when creating a connection, so simplify
        // life and just create a BufferedReader to read the incoming
        // info string.
        //
        // Do not close the BufferedReader; let TcpConnection manage it.
        String str = readLine();
        return new Control(str);
    }

    // waitForExits will wait for all socket watcher threads to
    // complete before proceeding.
    private void waitForExits() {
        // Kick old flusher forcefully.
        kickFlusher();

        if (socketWatchersDoneLatch != null) {
            try {
                logger.debug("nats: waiting for watcher threads to exit");
                socketWatchersDoneLatch.await();
            } catch (InterruptedException e) {
                logger.warn("nats: interrupted waiting for threads to exit");
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void spinUpSocketWatchers() {
        // Make sure everything has exited.

        waitForExits();

        socketWatchersDoneLatch = new CountDownLatch(NUM_WATCHER_THREADS);
        socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);

        Future<?> task = exec.submit(new Runnable() {
            public void run() {
                logger.debug("readloop starting...");
                Thread.currentThread().setName("readloop");
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                    readLoop();
                } catch (InterruptedException e) {
                    logger.debug("Interrupted", e);
                } catch (Exception e) {
                    logger.error("Unexpected exception in readloop", e);
                } finally {
                    socketWatchersDoneLatch.countDown();
                }
                logger.debug("readloop exiting");
            }
        });
        tasks.add(task);

        task = exec.submit(new Runnable() {
            public void run() {
                logger.debug("flusher starting...");
                Thread.currentThread().setName("flusher");
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                    flusher();
                } catch (InterruptedException e) {
                    logger.debug("Interrupted", e);
                } catch (Exception e) {
                    logger.error("Unexpected exception in flusher", e);
                } finally {
                    socketWatchersDoneLatch.countDown();
                }
                logger.debug("flusher exiting");
            }
        });
        tasks.add(task);
        // resetFlushTimer();
        // socketWatchersDoneLatch.countDown();

        socketWatchersStartLatch.countDown();

        resetPingTimer();
    }

    void readLoop() {
        Parser parser;
        int len;
        boolean sb;
        TcpConnection conn = null;

        mu.lock();
        try {
            parser = this.parser;
            if (parser.ps == null) {
                parser.ps = new Parser.ParseState();
            }
        } finally {
            mu.unlock();
        }

        // Stack based buffer.
        byte[] buffer = new byte[DEFAULT_BUF_SIZE];

        while (true) {
            mu.lock();
            try {
                sb = (closed() || reconnecting());
                if (sb) {
                    parser.ps = new Parser.ParseState();
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
                if (len == -1) {
                    throw new IOException(ERR_STALE_CONNECTION);
                }
                parser.parse(buffer, len);
            } catch (IOException | ParseException e) {
                logger.debug("Exception in readloop(): '{}' (state: {})", e.getMessage(), status);
                if (status != CLOSED) {
                    processOpError(e);
                }
                break;
            }
        }

        mu.lock();
        try {
            parser.ps = null;
        } finally {
            mu.unlock();
        }
    }

    /**
     * waitForMsgs waits on the conditional shared with readLoop and processMsg. It is used to
     * deliver messages to asynchronous subscribers.
     *
     * @param sub the asynchronous subscriber
     * @throws InterruptedException if the thread is interrupted
     */
    void waitForMsgs(AsyncSubscriptionImpl sub) throws InterruptedException {
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

                mcb = sub.getMessageHandler();
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

    /**
     * processMsg is called by parse and will place the msg on the appropriate channel/pending queue
     * for processing. If the channel is full, or the pending queue is over the pending limits, the
     * connection is considered a slow consumer.
     *
     * @param data   the buffer containing the message body
     * @param offset the offset within this buffer of the beginning of the message body
     * @param length the length of the message body
     */
    void processMsg(byte[] data, int offset, int length) {
        SubscriptionImpl sub;

        mu.lock();
        try {
            stats.incrementInMsgs();
            stats.incrementInBytes(length);

            sub = subs.get(parser.ps.ma.sid);
            if (sub == null) {
                return;
            }

            // Doing message create outside of the sub's lock to reduce contention.
            // It's possible that we end up not using the message, but that's ok.
            Message msg = new Message(parser.ps.ma, sub, data, offset, length);

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
                } else {
                    // We use mch for everything, unlike Go client
                    if (sub.getChannel() != null) {
                        if (sub.getChannel().add(msg)) {
                            sub.pCond.signal();
                            // Clear Slow Consumer status
                            sub.setSlowConsumer(false);
                        } else {
                            handleSlowConsumer(sub, msg);
                        }
                    }
                }
            } finally {
                sub.unlock();
            }
        } finally {
            mu.unlock();
        }
    }

    // Assumes you already have the lock
    void handleSlowConsumer(SubscriptionImpl sub, Message msg) {
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
            cbexec.submit(new Runnable() {
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
            cbexec.submit(new Runnable() {
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
    boolean removeFlushEntry(BlockingQueue<Boolean> ch) {
        mu.lock();
        try {
            if (pongs == null) {
                return false;
            }

            for (BlockingQueue<Boolean> c : pongs) {
                if (c.equals(ch)) {
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
    void sendPing(BlockingQueue<Boolean> ch) {
        if (pongs == null) {
            pongs = createPongs();
        }

        if (ch != null) {
            pongs.add(ch);
        }

        try {
            bw.write(pingProtoBytes, 0, pingProtoBytesLen);
            bw.flush();
        } catch (IOException e) {
            setLastError(e);
        }
    }

    List<BlockingQueue<Boolean>> createPongs() {
        return new ArrayList<BlockingQueue<Boolean>>();
    }

    ScheduledFuture<?> createPingTimer() {
        PingTimerTask pinger = new PingTimerTask();
        return exec.scheduleWithFixedDelay(pinger, opts.getPingInterval(),
                opts.getPingInterval(), TimeUnit.MILLISECONDS);
    }

    void resetPingTimer() {
        mu.lock();
        try {
            if (ptmr != null) {
                ptmr.cancel(true);
                tasks.remove(ptmr);
            }

            if (opts.getPingInterval() > 0) {
                ptmr = createPingTimer();
                tasks.add(ptmr);
            }
        } finally {
            mu.unlock();
        }
    }

    void writeUnsubProto(SubscriptionImpl sub, long max) throws IOException {
        String str = String.format(UNSUB_PROTO, sub.getSid(), max > 0 ? Long.toString(max) : "");
        str = str.replaceAll(" +\r\n", "\r\n");
        byte[] unsub = str.getBytes();
        bw.write(unsub);
    }

    void unsubscribe(SubscriptionImpl sub, int max) throws IOException {
        unsubscribe(sub, (long) max);
    }

    // unsubscribe performs the low level unsubscribe to the server.
    // Use SubscriptionImpl.unsubscribe()
    protected void unsubscribe(SubscriptionImpl sub, long max) throws IOException {
        mu.lock();
        try {
            if (isClosed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            SubscriptionImpl subscription = subs.get(sub.getSid());
            // already unsubscribed
            if (subscription == null) {
                return;
            }

            // If the autounsubscribe max is > 0, set that on the subscription
            if (max > 0) {
                subscription.setMax(max);
            } else {
                removeSub(subscription);
            }

            // We will send all subscriptions when reconnecting
            // so that we can suppress here.
            if (!reconnecting()) {
                writeUnsubProto(subscription, max);
            }

            kickFlusher();
        } finally {
            mu.unlock();
        }
    }

    protected void kickFlusher() {
        if (bw != null && fch != null) {
            fch.offer(true);
        }
    }

    // This is the loop of the flusher thread
    protected void flusher() throws InterruptedException {
        // snapshot the bw and conn since they can change from underneath of us.
        mu.lock();
        final OutputStream bw = this.bw;
        final TcpConnection conn = this.conn;
        final BlockingQueue<Boolean> fch = this.fch;
        mu.unlock();

        if (conn == null || bw == null) {
            return;
        }

        while (fch.take()) {
            mu.lock();
            try {
                // Check to see if we should bail out.
                if (!connected() || connecting() || bw != this.bw || conn != this.conn) {
                    return;
                }
                bw.flush();
                stats.incrementFlushes();
            } catch (IOException e) {
                logger.debug("I/O exception encountered during flush");
                this.setLastError(e);
            } finally {
                mu.unlock();
            }
            sleepInterval(flushTimerInterval, flushTimerUnit);
        }
        logger.debug("flusher id:{} exiting", Thread.currentThread().getId());
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.nats.client.AbstractConnection#flush(int)
     */
    @Override
    public void flush(int timeout) throws IOException {
        if (timeout <= 0) {
            throw new IllegalArgumentException(ERR_BAD_TIMEOUT);
        }

        BlockingQueue<Boolean> ch = null;
        mu.lock();
        try {
            if (closed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            ch = createBooleanChannel(1);
            sendPing(ch);
        } finally {
            mu.unlock();
        }

        Boolean rv = null;
        try {
            rv = ch.poll(timeout, TimeUnit.MILLISECONDS);
            if (rv == null) {
                throw new IOException(ERR_TIMEOUT);
            } else if (rv) {
                ch.clear();
            } else {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }
        } catch (InterruptedException e) {
            // Set interrupted flag.
            logger.debug("flush was interrupted while waiting for PONG", e);
            Thread.currentThread().interrupt();
        } finally {
            if (rv == null) {
                this.removeFlushEntry(ch);
            }
        }
    }


    /// Flush will perform a round trip to the server and return when it
    /// receives the internal reply.
    @Override
    public void flush() throws IOException {
        // 60 second default.
        flush(60000);
    }

    // resendSubscriptions will send our subscription state back to the
    // server. Used in reconnects
    void resendSubscriptions() {
        long adjustedMax = 0L;
        for (Map.Entry<Long, SubscriptionImpl> entry : subs.entrySet()) {
            SubscriptionImpl sub = entry.getValue();

            sub.lock();
            try {
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
                    logger.debug("nats: exception while writing UNSUB proto");
                }
            }
        }
    }

    /**
     * subscribe is the internal subscribe function that indicates interest in a subject.
     *
     * @param subject the subject
     * @param queue   an optional subscription queue
     * @param cb      async callback
     * @param ch      channel
     * @return the Subscription object
     */
    SubscriptionImpl subscribe(String subject, String queue, MessageHandler cb,
                               BlockingQueue<Message> ch) {
        final SubscriptionImpl sub;
        mu.lock();
        try {
            // Check for some error conditions.
            if (closed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            if (cb == null && ch == null) {
                throw new IllegalArgumentException(ERR_BAD_SUBSCRIPTION);
            }

            if (cb != null) {
                sub = new AsyncSubscriptionImpl(this, subject, queue, cb);
                // If we have an async callback, start up a sub specific Runnable to deliver the
                // messages
                logger.debug("Starting subscription for subject '{}'", subject);
                subexec.submit(new Runnable() {
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
            if (!reconnecting()) {
                sendSubscriptionMessage(sub);
            }

            kickFlusher();

            return sub;
        } finally {
            mu.unlock();
        }
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
        return subscribe(subject, null, cb);
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
        return subscribe(subj, null, cb);
    }

    private void addSubscription(SubscriptionImpl sub) {
        sub.setSid(sidCounter.incrementAndGet());
        subs.put(sub.getSid(), sub);
    }

    @Override
    public SyncSubscription subscribeSync(String subject, String queue) {
        return (SyncSubscription) subscribe(subject, queue, null,
                createMsgChannel());
    }

    @Override
    public SyncSubscription subscribeSync(String subject) {
        return (SyncSubscription) subscribe(subject, null, null,
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
    static final byte[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    // The internal publish operation sends a protocol data message by queueing into the buffered
    // OutputStream and kicking the flush go routine. These writes should be protected.
    void publish(byte[] subject, byte[] reply, byte[] data, boolean forceFlush) throws IOException {
        int msgSize = (data != null) ? data.length : 0;
        mu.lock();
        try {
            // Proactively reject payloads over the threshold set by server.
            if (msgSize > info.getMaxPayload()) {
                throw new IllegalArgumentException(ERR_MAX_PAYLOAD);
            }

            // Since we have the lock, examine directly for a tiny performance
            // boost in fastpath
            if (closed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            // Check if we are reconnecting, and if so check if
            // we have exceeded our reconnect outbound buffer limits.
            if (reconnecting()) {
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

            stats.incrementOutMsgs();
            stats.incrementOutBytes(msgSize);

            if (forceFlush) {
                bw.flush();
                stats.incrementFlushes();
            } else {
                // Opportunistic flush
                if (fch.isEmpty()) {
                    kickFlusher();
                }
            }
        } finally {
            mu.unlock();
        }
    }

    // publish can throw a few different unchecked exceptions:
    // IllegalStateException, IllegalArgumentException, NullPointerException
    @Override
    public void publish(String subject, String reply, byte[] data, boolean flush)
            throws IOException {
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
        publish(subjBytes, replyBytes, data, flush);
    }

    @Override
    public void publish(String subject, String reply, byte[] data) throws IOException {
        publish(subject, reply, data, false);
    }

    @Override
    public void publish(String subject, byte[] data) throws IOException {
        publish(subject, null, data);
    }

    @Override
    public void publish(Message msg) throws IOException {
        publish(msg.getSubjectBytes(), msg.getReplyToBytes(), msg.getData(), false);
    }

    @Override
    public Message request(String subject, byte[] data, long timeout, TimeUnit unit)
            throws IOException {
        String inbox = newInbox();
        BlockingQueue<Message> ch = createMsgChannel(8);

        Message msg = null;
        if (Thread.currentThread().isInterrupted()) {
            Thread.interrupted();
        }
        try (SyncSubscription sub = (SyncSubscription) subscribe(inbox, null, null, ch)) {
            sub.autoUnsubscribe(1);
            publish(subject, inbox, data);
            try {
                msg = sub.nextMessage(timeout, unit);
            } catch (InterruptedException e) {
                // There is nothing a caller can do with this, so swallow it.
                logger.debug("request() interrupted (and cleared)", e);
                Thread.interrupted();
            }
            return msg;
        }
    }

    @Override
    public Message request(String subject, byte[] data, long timeout)
            throws IOException {
        return request(subject, data, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message request(String subject, byte[] data) throws IOException {
        return request(subject, data, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public String newInbox() {
        return String.format("%s%s", INBOX_PREFIX, NUID.nextGlobal());
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

    // Assumes already have the lock
    void sendSubscriptionMessage(SubscriptionImpl sub) {
        // We will send these for all subs when we reconnect
        // so that we can suppress here.
        String queue = sub.getQueue();
        String subLine = String.format(SUB_PROTO, sub.getSubject(),
                (queue != null && !queue.isEmpty()) ? " " + queue : "", sub.getSid());
        try {
            bw.write(subLine.getBytes());
        } catch (IOException e) {
            logger.warn("nats: I/O exception while sending subscription message");
        }
    }

    @Override
    public ClosedCallback getClosedCallback() {
        mu.lock();
        try {
            return opts.getClosedCallback();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public void setClosedCallback(ClosedCallback cb) {
        mu.lock();
        try {
            opts.closedCb = cb;
        } finally {
            mu.unlock();
        }
    }

    @Override
    public DisconnectedCallback getDisconnectedCallback() {
        mu.lock();
        try {
            return opts.getDisconnectedCallback();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public void setDisconnectedCallback(DisconnectedCallback cb) {
        mu.lock();
        try {
            opts.disconnectedCb = cb;
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ReconnectedCallback getReconnectedCallback() {
        mu.lock();
        try {
            return opts.getReconnectedCallback();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public void setReconnectedCallback(ReconnectedCallback cb) {
        mu.lock();
        try {
            opts.reconnectedCb = cb;
        } finally {
            mu.unlock();
        }
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        mu.lock();
        try {
            return opts.getExceptionHandler();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        mu.lock();
        try {
            opts.asyncErrorCb = exceptionHandler;
        } finally {
            mu.unlock();
        }
    }

    @Override
    public String getConnectedUrl() {
        mu.lock();
        try {
            if (status != CONNECTED) {
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
            if (status != CONNECTED) {
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

    @Override
    public Exception getLastException() {
        return lastEx;
    }

    void setLastError(Exception err) {
        this.lastEx = err;
    }

    Options getOptions() {
        return this.opts;
    }

    void setOptions(Options options) {
        this.opts = options;
    }

    void setPending(ByteArrayOutputStream pending) {
        this.pending = pending;
    }

    ByteArrayOutputStream getPending() {
        return this.pending;
    }

    protected void sleepInterval(long timeout, TimeUnit unit) throws InterruptedException {
        unit.sleep(timeout);
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


    List<BlockingQueue<Boolean>> getPongs() {
        return pongs;
    }

    void setPongs(List<BlockingQueue<Boolean>> pongs) {
        this.pongs = pongs;
    }

    Map<Long, SubscriptionImpl> getSubs() {
        return subs;
    }

    void setSubs(Map<Long, SubscriptionImpl> subs) {
        this.subs = subs;
    }

    // for testing purposes
    List<Srv> getServerPool() {
        return this.srvPool;
    }

    // for testing purposes
    void setServerPool(List<Srv> pool) {
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

    void setTcpConnection(TcpConnection conn) {
        this.conn = conn;
    }

    TcpConnection getTcpConnection() {
        return this.conn;
    }

    void setTcpConnectionFactory(TcpConnectionFactory factory) {
        this.tcf = factory;
    }

    TcpConnectionFactory getTcpConnectionFactory() {
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

    ScheduledFuture<?> getPingTimer() {
        return ptmr;
    }

    void setPingTimer(ScheduledFuture<?> ptmr) {
        this.ptmr = ptmr;
    }

    void setParser(Parser parser) {
        this.parser = parser;
    }

    Parser getParser() {
        return parser;
    }

    String[] getServers(boolean implicitOnly) {
        List<String> serversList = new ArrayList<String>(srvPool.size());
        for (Srv aSrvPool : srvPool) {
            if (implicitOnly && !aSrvPool.isImplicit()) {
                continue;
            }
            URI url = aSrvPool.url;
            String schemeUrl =
                    String.format("%s://%s:%d", url.getScheme(), url.getHost(), url.getPort());
            serversList.add(schemeUrl);
        }
        String[] servers = new String[serversList.size()];
        return serversList.toArray(servers);
    }

    @Override
    public String[] getServers() {
        mu.lock();
        try {
            return getServers(false);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public String[] getDiscoveredServers() {
        mu.lock();
        try {
            return getServers(true);
        } finally {
            mu.unlock();
        }
    }

    @Override
    public boolean isAuthRequired() {
        mu.lock();
        try {
            return info.isAuthRequired();
        } finally {
            mu.unlock();
        }
    }

    @Override
    public boolean isTlsRequired() {
        mu.lock();
        try {
            return info.isTlsRequired();
        } finally {
            mu.unlock();
        }
    }

    static class Control {
        String op = null;
        String args = null;

        Control(String line) {
            if (line == null) {
                return;
            }

            String[] parts = line.split(" ", 2);

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

    static class ConnectInfo {
        @SerializedName("verbose")
        private final Boolean verbose;

        @SerializedName("pedantic")
        private final Boolean pedantic;

        @SerializedName("user")
        private final String user;

        @SerializedName("pass")
        private final String pass;

        @SerializedName("auth_token")
        private final String token;

        @SerializedName("tls_required")
        private final Boolean tlsRequired;

        @SerializedName("name")
        private final String name;

        @SerializedName("lang")
        private String lang = ConnectionImpl.LANG_STRING;

        @SerializedName("version")
        private String version;

        @SerializedName("protocol")
        private final int protocol;

        private final transient Gson gson = new GsonBuilder().create();

        public ConnectInfo(boolean verbose, boolean pedantic, String username, String password,
                           String token, boolean secure, String connectionName, String lang,
                           String version, ClientProto proto) {
            this.verbose = verbose;
            this.pedantic = pedantic;
            this.user = username;
            this.pass = password;
            this.token = token;
            this.tlsRequired = secure;
            this.name = connectionName;
            this.lang = lang;
            this.version = version;
            this.protocol = proto.getValue();
        }

        public String toString() {
            return gson.toJson(this);
        }
    }

    static class Srv {
        URI url = null;
        int reconnects = 0;
        long lastAttempt = 0L;
        long lastAttemptNanos = 0L;
        boolean implicit = false;

        Srv(URI url, boolean implicit) {
            this.url = url;
            this.implicit = implicit;
        }

        boolean isImplicit() {
            return implicit;
        }

        // Mark the last attempt to connect to this Srv
        void updateLastAttempt() {
            lastAttemptNanos = System.nanoTime();
            lastAttempt = System.currentTimeMillis();
        }

        // Returns time since last attempt, in msec
        long timeSinceLastAttempt() {
            return (lastAttemptNanos == 0L ? 0L :
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAttemptNanos));
        }

        public String toString() {
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
            String dateToStr = format.format(new Date(lastAttempt));

            return String.format(
                    "{url=%s, reconnects=%d, lastAttempt=%s, timeSinceLastAttempt=%dms}",
                    url.toString(), reconnects, dateToStr, timeSinceLastAttempt());
        }
    }

    // This will fire periodically and send a client origin
    // ping to the server. Will also check that we have received
    // responses from the server.
    class PingTimerTask extends TimerTask {
        public void run() {
            boolean stale = false;
            mu.lock();
            try {
                if (!connected()) {
                    return;
                }

                // Check for violation
                setActualPingsOutstanding(getActualPingsOutstanding() + 1);
                if (getActualPingsOutstanding() > opts.getMaxPingsOut()) {
                    stale = true;
                    return;
                }

                sendPing(null);
            } finally {
                mu.unlock();
                if (stale) {
                    processOpError(new IOException(ERR_STALE_CONNECTION));
                }
            }
        }
    }
}
