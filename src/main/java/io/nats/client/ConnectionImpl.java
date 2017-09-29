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
import static io.nats.client.Nats.ERR_TIMEOUT;
import static io.nats.client.Nats.PERMISSIONS_ERR;
import static io.nats.client.Nats.TLS_SCHEME;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import io.nats.client.AsyncDeliveryQueue.MessageBatch;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ConnectionImpl implements Connection {

    private String version = null;

    private static final String INBOX_PREFIX = "_INBOX.";
    private static final int NUID_SIZE = 22;
    private static final int RESP_INBOX_PREFIX_LEN = INBOX_PREFIX.length() + NUID_SIZE + 1;

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

    // The interval the flusher will wait after each flush of the buffered output stream
    private long flushTimerInterval = 1;
    private TimeUnit flushTimerUnit = TimeUnit.MILLISECONDS;

    // New style response handler
    private String respSub;                                            // The wildcard subject
    private Subscription respMux;                                      // A single response subscription
    private ConcurrentHashMap<String, BlockingQueue<Message>> respMap; // Request map for the response msg queues


    protected static final String CRLF = "\r\n";
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
    protected static final String CONN_PROTO = "CONNECT %s" + CRLF;
    protected static final String PING_PROTO = "PING" + CRLF;
    protected static final String PONG_PROTO = "PONG" + CRLF;
    protected static final String PUB_PROTO = "PUB %s %s %d" + CRLF;
    protected static final String SUB_PROTO = "SUB %s%s %d" + CRLF;
    protected static final String UNSUB_PROTO = "UNSUB %d %s" + CRLF;
    protected static final String OK_PROTO = _OK_OP_ + CRLF;


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

    private final AtomicLong sidCounter = new AtomicLong(0L);
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

    private static final byte[] pingProtoBytes = PING_PROTO.getBytes();
    private static final int pingProtoBytesLen = pingProtoBytes.length;
    private static final byte[] pongProtoBytes = PONG_PROTO.getBytes();
    private static final int pongProtoBytesLen = pongProtoBytes.length;
    private static final byte[] pubPrimBytes = _PUB_P_.getBytes();
    private static final int pubPrimBytesLen = pubPrimBytes.length;
    private static final byte[] crlfProtoBytes = CRLF.getBytes();
    private static final int crlfProtoBytesLen = crlfProtoBytes.length;

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

    static final String DISPATCH_EXEC_NAME = "jnats-disptach";

    // The ping timer task
    private ScheduledFuture<?> ptmr = null;
    static final String PINGTIMER = "pingtimer";

    static final String READLOOP = "readloop";

    static final String FLUSHER = "flusher";

    private final Map<String, Future<?>> tasks = new HashMap<>();
    private static final int NUM_WATCHER_THREADS = 2;
    private CountDownLatch socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);
    private CountDownLatch socketWatchersDoneLatch = null;

    // The flusher signalling channel
    private BlockingQueue<Boolean> fch;

    private final ExecutorService subscriptionDispatchPool;
    private final AsyncDeliveryQueue asyncQueue;
    private final int subscriptionConcurrency;
    private final boolean dispatchPoolCreatedLocally;
//    ConnectionImpl() {
//    }

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
        ExecutorService subscriptionPool = opts.subscriptionDispatchPool;
        int concurrency = -1;
        boolean createdPool = false;
        if (props.contains(Nats.PROP_SUBSCRIPTION_CONCURRENCY)) {
            try {
                concurrency = Integer.parseInt(props.getProperty(Nats.PROP_SUBSCRIPTION_CONCURRENCY));
                if (concurrency > 0 && subscriptionPool == null) {
                    createdPool = true;
                    subscriptionPool = Executors.newFixedThreadPool(concurrency);
                }
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Value for subscription concurrency is not a number: "
                    + props.getProperty(Nats.PROP_SUBSCRIPTION_CONCURRENCY));
            }
        } else {
            subscriptionPool = opts.subscriptionDispatchPool;
            if (subscriptionPool instanceof ThreadPoolExecutor) {
                concurrency = ((ThreadPoolExecutor) subscriptionPool).getCorePoolSize();
            } else {
                concurrency = Runtime.getRuntime().availableProcessors();
            }
        }
        dispatchPoolCreatedLocally = createdPool;
        subscriptionConcurrency = Math.max(concurrency, 1);
        this.subscriptionDispatchPool = subscriptionPool;
        asyncQueue = subscriptionPool == null ? null : new AsyncDeliveryQueue();
        if (subscriptionPool != null) {
            startDispatchPool();
        }
    }

    ScheduledExecutorService createScheduler() {
        ScheduledThreadPoolExecutor sexec = (ScheduledThreadPoolExecutor)
                Executors.newScheduledThreadPool(NUM_CORE_THREADS,
                        new NatsThreadFactory(EXEC_NAME));
        sexec.setRemoveOnCancelPolicy(true);
        return sexec;
    }

    ExecutorService createSubscriptionScheduler() {
        return Executors.newCachedThreadPool(new NatsThreadFactory(SUB_EXEC_NAME));
    }

    ExecutorService createCallbackScheduler() {
        return Executors.newSingleThreadExecutor(new NatsThreadFactory(CB_EXEC_NAME));
    }

    void setup() {
        exec = createScheduler();
        cbexec = createCallbackScheduler();
        subexec = createSubscriptionScheduler();
        fch = createFlushChannel();
        pongs = createPongs();
        subs.clear();

        // predefine the start of the publish protocol message.
        buildPublishProtocolBuffer(Parser.MAX_CONTROL_LINE_SIZE);
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

        setupServerPool();

        mu.lock();
        try {
            for (Srv srv : srvPool) {
                this.setUrl(srv.url);

                try {
                    createConn();
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
                    } catch (InterruptedException e) {
                        returnedErr = new IOException(e);
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
            throw new IOException(ERR_BAD_TIMEOUT);
        }
        Srv srv = currentServer();
        if (srv == null) {
            throw new IOException(ERR_NO_SERVERS);
        } else {
            srv.updateLastAttempt();
        }

        try {
            conn = tcf.createConnection();
            conn.open(srv.url.toString(), opts.getConnectionTimeout());
        } catch (IOException e) {
            throw (e);
        }

        if ((pending != null) && (bw != null)) {
            try {
                bw.flush();
            } catch (IOException e) {
                // ignore
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

    // Clear any pending request calls.
    private synchronized void clearPendingRequestCalls() {
        if (respMap == null) {
            return;
        }
        Iterator<Map.Entry<String, BlockingQueue<Message>>> iter = respMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, BlockingQueue<Message>> entry = iter.next();
            try {
                entry.getValue().put(null);
            } catch (InterruptedException ignored) {
            }
            iter.remove();
        }
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

            // Clear any queued and blocking requests.
            clearPendingRequestCalls();

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
                        }
                    });
                }
                if (opts.getClosedCallback() != null) {
                    cbexec.submit(new Runnable() {
                        @Override
                        public void run() {
                            opts.getClosedCallback().onClose(new ConnectionEvent(nc));
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
                shutdownAndAwaitTermination(exec, EXEC_NAME);
            }

            if (subexec != null) {
                shutdownAndAwaitTermination(subexec, SUB_EXEC_NAME);
            }

            if (dispatchPoolCreatedLocally) {
                shutdownAndAwaitTermination(subscriptionDispatchPool, DISPATCH_EXEC_NAME);
            }

        } finally {
            mu.unlock();
        }
    }

    void shutdownAndAwaitTermination(ExecutorService pool, String name) {
        try {
            pool.shutdownNow();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    void processConnectInit() throws IOException, InterruptedException {

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
        conn.makeTls(opts.getSslContext());
        bw = conn.getOutputStream(DEFAULT_STREAM_BUF_SIZE);
        br = conn.getInputStream(DEFAULT_STREAM_BUF_SIZE);
    }

    void processExpectedInfo() throws IOException, InterruptedException {
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
    void processPong() throws InterruptedException {
        BlockingQueue<Boolean> ch = null;
        mu.lockInterruptibly();
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

        if (info.getConnectUrls() != null) {
            ArrayList<String> connectUrls = new ArrayList<>(Arrays.asList(info.getConnectUrls()));
            if (connectUrls.size() > 0) {
                // If randomization is allowed, shuffle the received array, not the
                // entire pool. We want to preserve the pool's order up to this point
                // (this would otherwise be problematic for the (re)connect loop).
                if (!opts.isNoRandomize()) {
                    Collections.shuffle(connectUrls);
                }
            }
            for (String s : connectUrls) {
                if (!urls.containsKey(s)) {
                    this.addUrlToPool(String.format("nats://%s", s), true);
                }
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
    void processOpError(Exception err) throws InterruptedException {
        mu.lockInterruptibly();
        try {
            if (connecting() || closed() || reconnecting()) {
                return;
            }

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
                        // NOOP
                    }
                    conn.close();
                }

                if (fch != null) {
                   fch.offer(false);
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
                        try {
                            doReconnect();
                        } catch (InterruptedException e) {
                            // NOOP
                        }
                    }
                });
                if (cbexec.isShutdown()) {
                    cbexec = createCallbackScheduler();
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
                // NOOP
            }
        }

        pending = null;
    }

    // Try to reconnect using the option parameters.
    // This function assumes we are allowed to reconnect.
    void doReconnect() throws InterruptedException {
        // We want to make sure we have the other watchers shutdown properly
        // here before we proceed past this point
        waitForExits();

        // FIXME(dlc) - We have an issue here if we have
        // outstanding flush points (pongs) and they were not
        // sent out, but are still in the pipe.

        // Hold the lock manually and release where needed below.
        mu.lockInterruptibly();
        try {
            // Clear any queued pongs, e.g. pending flush calls.
            nc.clearPendingFlushCalls();

            // Clear any errors.
            setLastError(null);

            // Perform appropriate callback if needed for a disconnect
            if (opts.getDisconnectedCallback() != null) {
                cbexec.submit(new Runnable() {
                    public void run() {
                        opts.getDisconnectedCallback().onDisconnect(new ConnectionEvent(nc));
                    }
                });
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

                long sleepTime = 0L;

                // Sleep appropriate amount of time before the
                // connection attempt if connecting to same server
                // we just got disconnected from.

                long timeSinceLastAttempt = cur.timeSinceLastAttempt();
                if (timeSinceLastAttempt < opts.getReconnectWait()) {
                    sleepTime = opts.getReconnectWait() - timeSinceLastAttempt;
                }

                if (sleepTime > 0) {
                    mu.unlock();
                    Thread.sleep(sleepTime);
                    mu.lockInterruptibly();
                }

                // Check if we have been closed first.
                if (isClosed()) {
                    break;
                }

                // Mark that we tried a reconnect
                cur.reconnects++;

                // try to create a new connection
                try {
//                    conn.teardown();
                    createConn();
                } catch (Exception e) {
//                    conn.teardown();
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
//                    conn.teardown();
                    setLastError(e);
                    status = RECONNECTING;
                    continue;
                }

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
                    cbexec.submit(new Runnable() {

                        public void run() {
                            opts.getReconnectedCallback().onReconnect(new ConnectionEvent(nc));
                        }
                    });
                }

                // Release the lock here, we will return below
                mu.unlock();
                try {
                    // Make sure to flush everything
                    flush();
                } catch (IOException e) {
                    // Ignore
                }
                return;
            } // while

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
    void processErr(ByteBuffer error) throws InterruptedException {
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
    private void waitForExits() throws InterruptedException {
        // Kick old flusher forcefully.
        kickFlusher();

        if (socketWatchersDoneLatch != null) {
            socketWatchersDoneLatch.await();
        }
    }

    protected void spinUpSocketWatchers() throws InterruptedException {
        // Make sure everything has exited.

        waitForExits();

        socketWatchersDoneLatch = new CountDownLatch(NUM_WATCHER_THREADS);
        socketWatchersStartLatch = new CountDownLatch(NUM_WATCHER_THREADS);

        Future<?> task = exec.submit(new Runnable() {
            public void run() {
                Thread.currentThread().setName(READLOOP);
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                    readLoop();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // Ignore other exceptions
                } finally {
                    socketWatchersDoneLatch.countDown();
                }
            }
        });
        tasks.put(READLOOP, task);

        task = exec.submit(new Runnable() {
            public void run() {
                Thread.currentThread().setName(FLUSHER);
                socketWatchersStartLatch.countDown();
                try {
                    socketWatchersStartLatch.await();
                    flusher();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // Ignore other exceptions
                } finally {
                    socketWatchersDoneLatch.countDown();
                }
            }
        });
        tasks.put(FLUSHER, task);
        // resetFlushTimer();
        // socketWatchersDoneLatch.countDown();

        socketWatchersStartLatch.countDown();

        resetPingTimer();
    }

    void readLoop() throws InterruptedException {
        Parser parser;
        int len;
        boolean sb;
        TcpConnection conn = null;

        mu.lockInterruptibly();
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

        while (!Thread.currentThread().isInterrupted()) {
            mu.lockInterruptibly();
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
                if (status != CLOSED) {
                    processOpError(e);
                }
                break;
            }
        }

        mu.lockInterruptibly();
        try {
            parser.ps = null;
        } finally {
            mu.unlock();
        }
    }

    void fetchAndDispatchAsyncMessages(int index) throws InterruptedException {
        Thread.currentThread().setName(DISPATCH_EXEC_NAME + " " + index + " for " + opts.connectionName);
        List<MessageBatch> batches = new LinkedList<>();
        for (;;) {
            asyncQueue.get(batches);
            for (MessageBatch batch : batches) {
                try {
                    AsyncSubscriptionImpl sub = (AsyncSubscriptionImpl) subs.get(batch.subscriberId());
                    if (sub != null) {
                        MessageHandler handler = sub.getMessageHandler();
                        List<Message> messages = batch.toList();
                        boolean closed;
                        long max;
                        long delivered = 0L;
                        long totalBytes = batch.totalBytes();
                        sub.lock();
                        try {
                            max = sub.max;
                            closed = sub.isClosed();
                            if (!closed) {
                                if (sub.delivered + messages.size() >= sub.max) {
                                    messages = messages.subList(0, (int) (sub.max - sub.delivered));
                                    totalBytes = 0;
                                    for (Message msg : messages) {
                                        if (msg.getData() != null) {
                                            totalBytes += msg.getData().length;
                                        }
                                    }
                                }
                                delivered = sub.delivered += messages.size();
                            }
                            // The next two lines should probably be inside if (!closed)
                            // but in the original code this is updated regardless, so
                            // emulating that
                            sub.pMsgs -= batch.size();
                            sub.pBytes -= totalBytes;
                        } finally {
                            sub.unlock();
                        }
                        if (closed) {
                            continue;
                        }
                        for (Message message : batch) {
                            try {
                                handler.onMessage(message);
                            } catch (RuntimeException ex) {
                                if (opts.asyncErrorCb != null) {
                                    opts.asyncErrorCb.onException(new NATSException(ex, ConnectionImpl.this, sub));
                                } else {
                                    // This seems like a supremely bad idea, since it will abort message
                                    // delivery, and eventually exit all the threads in the thread pool.
                                    // Nonetheless, that is what the original code does.
                                    throw ex;
                                }
                            }
                        }
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
                } finally {
                    // This must be called to enable delivery of further messages
                    // on threads other than this one - to ensure one batch of
                    // messages doesn't get ahead of another one, AsyncDeliveryQueue
                    // tracks the thread most recently given a batch of messages
                    // and will not give further messages for the same subscriber 
                    // to any other thread until close() is called on the
                    // resulting batch
                    batch.close();
                }
            }
            batches.clear();
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
                    if (sub instanceof AsyncSubscriptionImpl && !isThreadPerSubscriber()) {
                        asyncQueue.add(parser.ps.ma.sid, msg);
                    } else if (sub.getChannel() != null) {
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
    boolean removeFlushEntry(BlockingQueue<Boolean> ch) throws InterruptedException {
        mu.lockInterruptibly();
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
                tasks.put("pingtimer", ptmr);
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
        mu.lockInterruptibly();
        final OutputStream bw = this.bw;
        final TcpConnection conn = this.conn;
        final BlockingQueue<Boolean> fch = this.fch;
        mu.unlock();

        if (conn == null || bw == null) {
            return;
        }

        while (fch.take()) {
            mu.lockInterruptibly();
            try {
                // Check to see if we should bail out.
                if (!connected() || connecting() || bw != this.bw || conn != this.conn) {
                    return;
                }
                bw.flush();
                stats.incrementFlushes();
            } catch (IOException e) {
                this.setLastError(e);
            } finally {
                mu.unlock();
            }
            flushTimerUnit.sleep(flushTimerInterval);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see io.nats.client.AbstractConnection#flush(int)
     */
    @Override
    public void flush(int timeout) throws IOException, InterruptedException {
        if (timeout <= 0) {
            throw new IllegalArgumentException(ERR_BAD_TIMEOUT);
        }

        BlockingQueue<Boolean> ch = null;
        mu.lockInterruptibly();
        try {
            if (closed()) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }

            ch = createBooleanChannel(1);
            sendPing(ch);
        } finally {
            mu.unlock();
        }

        Boolean rv = ch.poll(timeout, TimeUnit.MILLISECONDS);
        if (rv == null) {
            this.removeFlushEntry(ch);
            throw new IOException(ERR_TIMEOUT);
        } else if (rv) {
            ch.clear();
        } else {
            throw new IllegalStateException(ERR_CONNECTION_CLOSED);
        }
    }


    /// Flush will perform a round trip to the server and return when it
    /// receives the internal reply.
    @Override
    public void flush() throws IOException, InterruptedException {
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
                    // Ignore
                }
            }
        }
    }

    boolean isThreadPerSubscriber() {
        return subscriptionDispatchPool == null;
    }

    void startDispatchPool() {
        for (int i = 0; i < subscriptionConcurrency; i++) {
            this.subscriptionDispatchPool.submit(new AsyncMessageDispatch(i));
        }
    }

    final class AsyncMessageDispatch implements Runnable {

        private final int index;
        AsyncMessageDispatch(int index) {
            this.index = index;
        }
        public void run() {
            try {
                fetchAndDispatchAsyncMessages(index);
            } catch (InterruptedException ex) {
                // This is not actually a good idea, but is what the original
                // code did.  In fact, setting the interrupted flag will do
                // nothing, since all that happens from here is the thread
                // exits and terminates.  But, for compatibility...
                Thread.currentThread().interrupt();
                // The point is, there is no way to guarantee that the *only*
                // way this thread can become interrupted is because it
                // *should* exit (which could break all asynchronous listening).
                // It calls out to client code, so anything can happen.
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
        if (!isThreadPerSubscriber() && cb != null && opts.asyncErrorCb == null
                && !Boolean.getBoolean("unit.test")) {
            throw new IllegalStateException("An async error callback must be set to "
                    + "use async message delivery with a shared thread pool");
        }
        if (ch != null && cb != null) {
            throw new IllegalArgumentException("Passed a queue for an asynchronous "
                    + "subscription.  It will not be used and should be null if "
                    + "a MessageHandler is passed.");
        }
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
                if (isThreadPerSubscriber()) {
                    // If we have an async callback, start up a sub specific Runnable to deliver the
                    // messages
                    subexec.submit(new Runnable() {
                        public void run() {
                            try {
                                waitForMsgs((AsyncSubscriptionImpl) sub);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
                }
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
                    // Ignore
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
                try {
                    bw.flush();
                    stats.incrementFlushes();
                } catch (IOException e) {
                    // Ignore
                }
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
            throws IOException, InterruptedException {
        if (opts.useOldRequestStyle) {
            return oldRequest(subject, data, timeout, unit);
        }

        // Make sure scoped subscription is setup at least once on first call to request().
        // Will handle duplicates in createRespMux.
        createRespMux();

        // Create literal Inbox and map to a queue.
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1);
        String respInbox = newRespInbox();
        String token = respToken(respInbox);
        respMap.put(token, queue);

        publish(subject, respInbox, data);
        Message response;
        if (timeout < 0) {
            response = queue.take();
        } else {
            response = queue.poll(timeout, unit);
            if (response == null) {
                // Timed out, cleanup queue.
                respMap.remove(token);
            }
        }
        return response;
    }

    @Override
    public Message request(String subject, byte[] data, long timeout)
            throws IOException, InterruptedException {
        return request(subject, data, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message request(String subject, byte[] data) throws IOException, InterruptedException {
        return request(subject, data, -1, TimeUnit.MILLISECONDS);
    }

    // Creates the response subscription we will use for all new style responses. This will be on an _INBOX with an
    // additional terminal token. The subscription will be on a wildcard.
    private synchronized void createRespMux() {
        if (respMap != null) {
            // Already setup for responses.
            return;
        }

        // _INBOX wildcard
        respSub = String.format("%s.*", newInbox());
        respMux = subscribe(respSub, new RespHandler());
        respMap = new ConcurrentHashMap<>();
    }

    // Creates a new literal response subject that will trigger the global subscription handler.
    private String newRespInbox() {
        byte[] b = new byte[RESP_INBOX_PREFIX_LEN + NUID_SIZE];
        byte[] respSubBytes = respSub.getBytes();
        System.arraycopy(respSubBytes, 0, b, 0, RESP_INBOX_PREFIX_LEN);
        byte[] nuid = NUID.nextGlobal().getBytes();
        System.arraycopy(nuid, 0, b, RESP_INBOX_PREFIX_LEN, nuid.length);
        return new String(b);
    }

    private String respToken(String respInbox) {
        return respInbox.substring(RESP_INBOX_PREFIX_LEN);
    }

    /**
     * RespHandler is the global response handler. It will look up the appropriate queue based on the last token and
     * place the message on the queue if possible.
     */
    private final class RespHandler implements MessageHandler {
        @Override
        public void onMessage(Message msg) {
            String token = respToken(msg.getSubject());

            // Just return if closed.
            if (isClosed()) {
                return;
            }

            BlockingQueue<Message> queue = respMap.get(token);
            if (queue == null) {
                // No response queue, drop the message.
                return;
            }

            // Delete the key regardless, one response only.
            respMap.remove(token);
            queue.offer(msg);
        }
    }

    private Message oldRequest(String subject, byte[] data, long timeout, TimeUnit unit)
            throws IOException, InterruptedException {
        String inbox = newInbox();
        BlockingQueue<Message> ch = createMsgChannel(8);

        try (SyncSubscription sub = (SyncSubscription) subscribe(inbox, null, null, ch)) {
            sub.autoUnsubscribe(1);
            publish(subject, inbox, data);
            return sub.nextMessage(timeout, unit);
        }
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
            // Ignore - FIXME:  This should be thrown
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

    @Override
    public String getName() {
        return opts.connectionName;
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
        }

        // Returns time since last attempt, in msec
        long timeSinceLastAttempt() {
            return (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAttemptNanos));
        }

        public String toString() {
            return String.format(
                    "{url=%s, reconnects=%d, timeSinceLastAttempt=%dms}",
                    url.toString(), reconnects, timeSinceLastAttempt());
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
                    try {
                        processOpError(new IOException(ERR_STALE_CONNECTION));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
