// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.nats.client.AuthenticationException;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.Consumer;
import io.nats.client.Dispatcher;
import io.nats.client.ErrorListener;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;
import io.nats.client.Options;
import io.nats.client.Statistics;
import io.nats.client.Subscription;

class NatsConnection implements Connection {
    static final byte[] EMPTY_BODY = new byte[0];

    static final byte CR = 0x0D;
    static final byte LF = 0x0A;
    static final byte[] CRLF = { CR, LF };

    static final ByteBuffer OP_CONNECT = ByteBuffer.wrap(new byte[] { 'C', 'O', 'N', 'N', 'E', 'C', 'T' });
    static final ByteBuffer OP_INFO = ByteBuffer.wrap(new byte[] { 'I', 'N', 'F', 'O' });
    static final ByteBuffer OP_SUB = ByteBuffer.wrap(new byte[] { 'S', 'U', 'B' });
    static final ByteBuffer OP_PUB = ByteBuffer.wrap(new byte[] { 'P', 'U', 'B' });
    static final ByteBuffer OP_UNSUB = ByteBuffer.wrap(new byte[] { 'U', 'N', 'S', 'U', 'B' });
    static final ByteBuffer OP_MSG = ByteBuffer.wrap(new byte[] { 'M', 'S', 'G' });
    static final ByteBuffer OP_PING = ByteBuffer.wrap(new byte[] { 'P', 'I', 'N', 'G' });
    static final ByteBuffer OP_PING_LINE = ByteBuffer.wrap(new byte[] { 'P', 'I', 'N', 'G', '\r', '\n' });
    static final ByteBuffer OP_PONG = ByteBuffer.wrap(new byte[] { 'P', 'O', 'N', 'G'});
    static final ByteBuffer OP_PONG_LINE = ByteBuffer.wrap(new byte[] { 'P', 'O', 'N', 'G', '\r', '\n' });
    static final ByteBuffer OP_OK = ByteBuffer.wrap(new byte[] { '+', 'O', 'K' });
    static final ByteBuffer OP_ERR = ByteBuffer.wrap(new byte[] { '-', 'E', 'R', 'R' });

    private Options options;

    private NatsStatistics statistics;

    private boolean connecting; // you can only connect in one thread
    private boolean disconnecting; // you can only disconnect in one thread
    private boolean closing; // respect a close call regardless
    private Exception exceptionDuringConnectChange; // an exception occurred in another thread while disconnecting or
                                                    // connecting

    private Status status;
    private ReentrantLock statusLock;
    private Condition statusChanged;

    private CompletableFuture<DataPort> dataPortFuture;
    private DataPort dataPort;
    private String currentServerURI;
    private CompletableFuture<Boolean> reconnectWaiter;
    private HashMap<String, String> serverAuthErrors;

    private NatsConnectionReader reader;
    private NatsConnectionWriter writer;

    private AtomicReference<NatsServerInfo> serverInfo;

    private Map<ByteBuffer, NatsSubscription> subscribers;
    private Map<ByteBuffer, NatsDispatcher> dispatchers; // use a concurrent map so we get more consistent iteration
                                                     // behavior
    private Map<ByteBuffer, CompletableFuture<Message>> responses;
    private ConcurrentLinkedDeque<CompletableFuture<Boolean>> pongQueue;

    private ByteBuffer mainInbox;
    private AtomicReference<NatsDispatcher> inboxDispatcher;
    private Timer timer;

    private AtomicBoolean needPing;

    private AtomicLong nextSid;
    private NUID nuid;

    private AtomicReference<String> connectError;
    private AtomicReference<String> lastError;
    private AtomicReference<CompletableFuture<Boolean>> draining;
    private AtomicBoolean blockPublishForDrain;

    private ExecutorService callbackRunner;

    private ExecutorService executor;
    private ExecutorService connectExecutor;

    private String currentServer = null;

    NatsConnection(Options options) {
        boolean trace = options.isTraceConnection();
        timeTrace(trace, "creating connection object");

        this.options = options;

        this.statistics = new NatsStatistics(this.options.isTrackAdvancedStats());

        this.statusLock = new ReentrantLock();
        this.statusChanged = this.statusLock.newCondition();
        this.status = Status.DISCONNECTED;
        this.reconnectWaiter = new CompletableFuture<>();
        this.reconnectWaiter.complete(Boolean.TRUE);

        this.dispatchers = new ConcurrentHashMap<>();
        this.subscribers = new ConcurrentHashMap<>();
        this.responses = new ConcurrentHashMap<>();

        this.serverAuthErrors = new HashMap<>();

        this.nextSid = new AtomicLong(1);
        long start = System.nanoTime();
        this.nuid = new NUID();
        if (trace) {
            long seconds = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            if (seconds > 1L) {
                // If you see this trace check: https://github.com/nats-io/nats.java#linux-platform-note
                timeTrace(trace, "NUID initialization took long: %d (s)", seconds);
            }
        }
        ByteBuffer inbox = createInboxBuffer();
        this.mainInbox = ByteBuffer.allocate(inbox.limit() + 2);
        this.mainInbox.put(inbox.asReadOnlyBuffer());
        this.mainInbox.put((byte)'.');
        this.mainInbox.put((byte)'*');
        this.mainInbox.flip();

        this.lastError = new AtomicReference<>();
        this.connectError = new AtomicReference<>();

        this.serverInfo = new AtomicReference<>();
        this.inboxDispatcher = new AtomicReference<>();
        this.pongQueue = new ConcurrentLinkedDeque<>();
        this.draining = new AtomicReference<>();
        this.blockPublishForDrain = new AtomicBoolean();

        timeTrace(trace, "creating executors");
        this.callbackRunner = Executors.newSingleThreadExecutor();
        this.executor = options.getExecutor();
        this.connectExecutor = Executors.newSingleThreadExecutor();

        timeTrace(trace, "creating reader and writer");
        this.reader = new NatsConnectionReader(this);
        this.writer = new NatsConnectionWriter(this);

        this.needPing = new AtomicBoolean(true);
        timeTrace(trace, "connection object created");
    }

    // Connect is only called after creation
    void connect(boolean reconnectOnConnect) throws InterruptedException, IOException {
        if (options.getServers().size() == 0) {
            throw new IllegalArgumentException("No servers provided in options");
        }

        boolean trace = options.isTraceConnection();
        long start = System.nanoTime();

        this.lastError.set("");

        timeTrace(trace, "starting connect loop");

        Collection<String> serversToTry = buildServerList();
        for (String serverURI : serversToTry) {
            if (isClosed()) {
                break;
            }
            this.connectError.set(""); // new on each attempt

            timeTrace(trace, "setting status to connecting");
            updateStatus(Status.CONNECTING);

            timeTrace(trace, "trying to connect to %s", serverURI);
            tryToConnect(serverURI, System.nanoTime());

            if (isConnected()) {
                this.currentServer = serverURI;
                break;
            } else {
                timeTrace(trace, "setting status to disconnected");
                updateStatus(Status.DISCONNECTED);

                String err = connectError.get();

                if (this.isAuthenticationError(err)) {
                    this.serverAuthErrors.put(serverURI, err);
                }
            }
        }

        if (!isConnected() && !isClosed()) {
            if (reconnectOnConnect) {
                timeTrace(trace, "trying to reconnect on connect");
                reconnect();
            } else {
                timeTrace(trace, "connection failed, closing to cleanup");
                close();

                String err = connectError.get();
                if (this.isAuthenticationError(err)) {
                    String msg = String.format("Authentication error connecting to NATS server: %s.", err);
                    throw new AuthenticationException(msg);
                } else {
                    String msg = String.format("Unable to connect to NATS servers: %s.", String.join(", ", getServers()));
                    throw new IOException(msg);
                }
            }
        } else if (trace) {
            long end = System.nanoTime();
            double seconds = ((double) (end - start)) / 1_000_000_000.0;
            timeTrace(trace, "connect complete in %.3f seconds", seconds);
        }
    }

    // Reconnect can only be called when the connection is disconnected
    void reconnect() throws InterruptedException {
        long maxTries = options.getMaxReconnect();
        long tries = 0;
        String lastServer = null;

        if (isClosed()) {
            return;
        }

        if (maxTries == 0) {
            this.close();
            return;
        }

        this.writer.setReconnectMode(true);

        boolean doubleAuthError = false;

        while (!isConnected() && !isClosed() && !this.isClosing()) {
            List<String> serversToTry = buildServerList();
            lastServer = serversToTry.get(serversToTry.size()-1);

            for (String server : serversToTry) {
                if (isClosed()) {
                    break;
                }

                connectError.set(""); // reset on each loop

                if (isDisconnectingOrClosed() || this.isClosing()) {
                    break;
                }

                updateStatus(Status.RECONNECTING);

                tryToConnect(server, System.nanoTime());
                tries++;

                if (maxTries > 0 && tries >= maxTries) {
                    break;
                } else if (isConnected()) {
                    this.statistics.incrementReconnects();
                    this.currentServer = server;
                    break;
                } else {
                    String err = connectError.get();

                    if (this.isAuthenticationError(err)) {
                        if (err.equals(this.serverAuthErrors.get(server))) {
                            doubleAuthError = true;
                            break; // will close below
                        }

                        this.serverAuthErrors.put(server, err);
                    }
                }

                if (server.equals(lastServer)) {
                    this.reconnectWaiter = new CompletableFuture<>();
                    waitForReconnectTimeout();
                }
            }

            if (doubleAuthError) {
                break;
            }

            if (maxTries > 0 && tries >= maxTries) {
                break;
            }
        }

        if (!isConnected()) {
            this.close();
            return;
        }

        this.subscribers.forEach((sid, sub) -> {
            if (sub.getDispatcher() == null && !sub.isDraining()) {
                sendSubscriptionMessage(sub.getSIDBuffer(), sub.getSubjectBuffer(), sub.getQueueNameBuffer(), true);
            }
        });

        this.dispatchers.forEach((nuid, d) -> {
            if (!d.isDraining()) {
                d.resendSubscriptions();
            }
        });

        try {
            this.flush(this.options.getConnectionTimeout());
        } catch (Exception exp) {
            this.processException(exp);
        }
        
        // When the flush returns we are done sending internal messages, so we can switch to the
        // non-reconnect queue
        this.writer.setReconnectMode(false);

        processConnectionEvent(Events.RESUBSCRIBED);
    }

    void timeTrace(boolean trace, String format, Object... args) {
        if (trace) {
            String timeStr = DateTimeFormatter.ISO_TIME.format(LocalDateTime.now());
            System.out.printf("[%s] connect trace: ", timeStr);
            System.out.printf(format, args);
            System.out.println();

        }
    }

    long timeCheck(boolean trace, long endNanos, String format, Object... args) throws TimeoutException {
        long now = System.nanoTime();
        long remaining = endNanos - now;

        if (trace) {
            String timeStr = DateTimeFormatter.ISO_TIME.format(LocalDateTime.now());
            double seconds = ((double) remaining) / 1_000_000_000.0;
            System.out.printf("[%s] connect trace: ", timeStr);
            System.out.printf(format, args);
            System.out.printf(", %.3f (s) remaining", seconds);
            System.out.println();

        }

        if (remaining < 0) {
            throw new TimeoutException("connection timed out");
        }

        return remaining;
    }

    // is called from reconnect and connect
    // will wait for any previous attempt to complete, using the reader.stop and
    // writer.stop
    void tryToConnect(String serverURI, long now) {
        try {
            Duration connectTimeout = options.getConnectionTimeout();
            boolean trace = options.isTraceConnection();
            long end = now + connectTimeout.toNanos();
            long timeoutNanos = timeCheck(trace, end, "starting connection attempt");

            statusLock.lock();
            try {
                if (this.connecting) {
                    return;
                }
                this.connecting = true;
                statusChanged.signalAll();
            } finally {
                statusLock.unlock();
            }

            // Create a new future for the dataport, the reader/writer will use this
            // to wait for the connect/failure.
            this.dataPortFuture = new CompletableFuture<>();

            // Make sure the reader and writer are stopped
            timeoutNanos = timeCheck(trace, end, "waiting for reader");
            this.reader.stop().get(timeoutNanos, TimeUnit.NANOSECONDS);
            timeoutNanos = timeCheck(trace, end, "waiting for writer");
            this.writer.stop().get(timeoutNanos, TimeUnit.NANOSECONDS);

            timeoutNanos = timeCheck(trace, end, "cleaning pong queue");
            cleanUpPongQueue();

            timeoutNanos = timeCheck(trace, end, "connecting data port");
            DataPort newDataPort = this.options.buildDataPort();
            newDataPort.connect(serverURI, this, timeoutNanos).get();

            // Notify the any threads waiting on the sockets
            this.dataPort = newDataPort;
            this.dataPortFuture.complete(this.dataPort);

            // Wait for the INFO message manually
            // all other traffic will use the reader and writer
            Callable<Object> connectTask = new Callable<Object>() {
                public Object call() throws IOException, ExecutionException, InterruptedException {
                    readInitialInfo();
                    checkVersionRequirements();
                    long start = System.nanoTime();
                    upgradeToSecureIfNeeded();
                    if (trace && options.isTLSRequired()) {
                        // If the time appears too long it might be related to
                        // https://github.com/nats-io/nats.java#linux-platform-note
                        timeTrace(trace, "TLS upgrade took: %.3f (s)",
                                ((double)(System.nanoTime() - start)) / 1_000_000_000.0);
                    }
                    return null;
                }
            };

            timeoutNanos = timeCheck(trace, end, "reading info, version and upgrading to secure if necessary");
            Future<Object> future = this.connectExecutor.submit(connectTask);
            try {
                future.get(timeoutNanos, TimeUnit.NANOSECONDS);
            } finally {
                future.cancel(true);
            }

            // start the reader and writer after we secured the connection, if necessary
            timeoutNanos = timeCheck(trace, end, "starting reader");
            this.reader.start(this.dataPortFuture);
            timeoutNanos = timeCheck(trace, end, "starting writer");
            this.writer.start(this.dataPortFuture);

            timeoutNanos = timeCheck(trace, end, "sending connect message");
            this.sendConnect(serverURI);

            timeoutNanos = timeCheck(trace, end, "sending initial ping");
            Future<Boolean> pongFuture = sendPing();

            if (pongFuture != null) {
                pongFuture.get(timeoutNanos, TimeUnit.NANOSECONDS);
            }

            if (this.timer == null) {
                timeoutNanos = timeCheck(trace, end, "starting ping and cleanup timers");
                this.timer = new Timer("Nats Connection Timer");

                long pingMillis = this.options.getPingInterval().toMillis();

                if (pingMillis > 0) {
                    this.timer.schedule(new TimerTask() {
                        public void run() {
                            if (isConnected()) {
                                softPing(); // The timer always uses the standard queue
                            }
                        }
                    }, pingMillis, pingMillis);
                }

                long cleanMillis = this.options.getRequestCleanupInterval().toMillis();

                if (cleanMillis > 0) {
                    this.timer.schedule(new TimerTask() {
                        public void run() {
                            cleanResponses(false);
                        }
                    }, cleanMillis, cleanMillis);
                }
            }

            // Set connected status
            timeoutNanos = timeCheck(trace, end, "updating status to connected");
            statusLock.lock();
            try {
                this.connecting = false;

                if (this.exceptionDuringConnectChange != null) {
                    throw this.exceptionDuringConnectChange;
                }

                this.currentServerURI = serverURI;
                this.serverAuthErrors.remove(serverURI); // reset on successful connection
                updateStatus(Status.CONNECTED); // will signal status change, we also signal in finally
            } finally {
                statusLock.unlock();
            }
            timeTrace(trace, "status updated");
        } catch (RuntimeException exp) { // runtime exceptions, like illegalArgs
            processException(exp);
            throw exp;
        } catch (Exception exp) { // every thing else
            processException(exp);
            try {
                this.closeSocket(false);
            } catch (InterruptedException e) {
                processException(e);
            }
        } finally {
            statusLock.lock();
            try {
                this.connecting = false;
                statusChanged.signalAll();
            } finally {
                statusLock.unlock();
            }
        }
    }

    void checkVersionRequirements() throws IOException {
        Options opts = getOptions();
        NatsServerInfo info = getInfo();

        if (opts.isNoEcho() && info.getProtocolVersion() < 1) {
            throw new IOException("Server does not support no echo.");
        }
    }

    void upgradeToSecureIfNeeded() throws IOException, ExecutionException, InterruptedException {
        Options opts = getOptions();
        NatsServerInfo info = getInfo();

        if (opts.isTLSRequired() && !info.isTLSRequired()) {
            throw new IOException("SSL connection wanted by client.");
        } else if (!opts.isTLSRequired() && info.isTLSRequired()) {
            throw new IOException("SSL required by server.");
        }

        if (opts.isTLSRequired()) {
            this.dataPort.upgradeToSecure();
        }
    }

    // Called from reader/writer thread
    void handleCommunicationIssue(Exception io) {
        // If we are connecting or disconnecting, note exception and leave
        statusLock.lock();
        try {
            if (this.connecting || this.disconnecting || this.status == Status.CLOSED || this.isDraining()) {
                this.exceptionDuringConnectChange = io;
                return;
            }
        } finally {
            statusLock.unlock();
        }

        processException(io);

        // Spawn a thread so we don't have timing issues with
        // waiting on read/write threads
        executor.submit(() -> {
            try {
                this.closeSocket(true);
            } catch (InterruptedException e) {
                processException(e);
            }
        });
    }

    // Close socket is called when another connect attempt is possible
    // Close is called when the connection should shutdown, period
    void closeSocket(boolean tryReconnectIfConnected) throws InterruptedException {
        boolean wasConnected = false;

        statusLock.lock();
        try {
            if (isDisconnectingOrClosed()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            } else {
                this.disconnecting = true;
                this.exceptionDuringConnectChange = null;
                wasConnected = (this.status == Status.CONNECTED);
                statusChanged.signalAll();
            }
        } finally {
            statusLock.unlock();
        }

        closeSocketImpl();

        statusLock.lock();
        try {
            updateStatus(Status.DISCONNECTED);
            this.exceptionDuringConnectChange = null; // Ignore IOExceptions during closeSocketImpl()
            this.disconnecting = false;
            statusChanged.signalAll();
        } finally {
            statusLock.unlock();
        }

        if (isClosing()) { // Bit of a misname, but closing means we are in the close method or were asked
                           // to be
            close();
        } else if (wasConnected && tryReconnectIfConnected) {
            reconnect();
        }
    }

    // Close socket is called when another connect attempt is possible
    // Close is called when the connection should shutdown, period
    public void close() throws InterruptedException {
        this.close(true);
    }

    void close(boolean checkDrainStatus) throws InterruptedException {
        statusLock.lock();
        try {
            if (checkDrainStatus && this.isDraining()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            }

            this.closing = true;// We were asked to close, so do it
            if (isDisconnectingOrClosed()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            } else {
                this.disconnecting = true;
                this.exceptionDuringConnectChange = null;
                statusChanged.signalAll();
            }
        } finally {
            statusLock.unlock();
        }

        // Stop the reconnect wait timer after we stop the writer/reader (only if we are
        // really closing, not on errors)
        if (this.reconnectWaiter != null) {
            this.reconnectWaiter.cancel(true);
        }

        closeSocketImpl();

        this.dispatchers.forEach((nuid, d) -> {
            d.stop(false);
        });

        this.subscribers.forEach((sid, sub) -> {
            sub.invalidate();
        });

        this.dispatchers.clear();
        this.subscribers.clear();

        if (timer != null) {
            timer.cancel();
            timer = null;
        }

        cleanResponses(true);

        cleanUpPongQueue();

        statusLock.lock();
        try {
            updateStatus(Status.CLOSED); // will signal, we also signal when we stop disconnecting

            /*
            if (exceptionDuringConnectChange != null) {
                processException(exceptionDuringConnectChange);
                exceptionDuringConnectChange = null;
            }*/
        } finally {
            statusLock.unlock();
        }

        // Stop the error handling and connect executors
        callbackRunner.shutdown();
        try {
            callbackRunner.awaitTermination(this.options.getConnectionTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } finally {
            callbackRunner.shutdownNow();
        }

        // There's no need to wait for running tasks since we're told to close
        connectExecutor.shutdownNow();

        statusLock.lock();
        try {
            this.disconnecting = false;
            statusChanged.signalAll();
        } finally {
            statusLock.unlock();
        }
    }

    // Should only be called from closeSocket or close
    void closeSocketImpl() {
        this.currentServerURI = null;

        //Signal both to stop. 
        final Future<Boolean> readStop = this.reader.stop();
        final Future<Boolean> writeStop = this.writer.stop();
        
        // Now wait until they both stop before closing the socket. 
        try {
            readStop.get(1, TimeUnit.SECONDS);
        } catch (Exception ex) {
            //
        }
        try {
            writeStop.get(1, TimeUnit.SECONDS);
        } catch (Exception ex) {
            //
        }

        this.dataPortFuture.cancel(true);
        

        // Close the current socket and cancel anyone waiting for it
        try {
            if (this.dataPort != null) {
                this.dataPort.close();
            }

        } catch (IOException ex) {
            processException(ex);
        }
        cleanUpPongQueue();
        

        try {
            this.reader.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            processException(ex);
        }
        try {
            this.writer.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            processException(ex);
        }

    }

    void cleanUpPongQueue() {
        Future<Boolean> b;
        while ((b = pongQueue.poll()) != null) {
            try {
                b.cancel(true);
            } catch (CancellationException e) {
                if (!b.isDone() && !b.isCancelled()) {
                    processException(e);
                }
            }
        }
    }

    public void publish(String subject, byte[] body) {
        this.publish((subject != null) ? NatsEncoder.encodeSubject(subject) : null, null, body);
    }

    public void publish(String subject, String replyTo, byte[] body) {
        this.publish((subject != null) ? NatsEncoder.encodeSubject(subject) : null, (replyTo != null) ? NatsEncoder.encodeReplyTo(replyTo) : null, body);
    }

    public void publish(ByteBuffer subject, ByteBuffer replyTo, byte[] body) {

        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (blockPublishForDrain.get()) {
            throw new IllegalStateException("Connection is Draining"); // Ok to publish while waiting on subs
        }

        if (subject == null || subject.remaining() == 0) {
            throw new IllegalArgumentException("Subject is required in publish");
        }

        if (replyTo != null && replyTo.remaining() == 0) {
            throw new IllegalArgumentException("ReplyTo cannot be the empty string");
        }

        if (body == null) {
            body = EMPTY_BODY;
        } else if (body.length > this.getMaxPayload() && this.getMaxPayload() > 0) {
            throw new IllegalArgumentException(
                    "Message payload size exceed server configuration " + body.length + " vs " + this.getMaxPayload());
        }

        NatsMessage msg = new NatsMessage(subject, replyTo, ByteBuffer.wrap(body), options.supportUTF8Subjects());

        if ((this.status == Status.RECONNECTING || this.status == Status.DISCONNECTED)
                && !this.writer.canQueue(msg, options.getReconnectBufferSize())) {
            throw new IllegalStateException(
                    "Unable to queue any more messages during reconnect, max buffer is " + options.getReconnectBufferSize());
        }
        queueOutgoing(msg);
    }

    public Subscription subscribe(String subject) {

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        Pattern pattern = Pattern.compile("\\s");
        Matcher matcher = pattern.matcher(subject);

        if (matcher.find()) {
            throw new IllegalArgumentException("Subject cannot contain whitespace");
        }

        return createSubscription(NatsEncoder.encodeSubject(subject), null, null);
    }

    public Subscription subscribe(String subject, String queueName) {

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }
        
        Pattern pattern = Pattern.compile("\\s");
        Matcher smatcher = pattern.matcher(subject);

        if (smatcher.find()) {
            throw new IllegalArgumentException("Subject cannot contain whitespace");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }

        Matcher qmatcher = pattern.matcher(queueName);

        if (qmatcher.find()) {
            throw new IllegalArgumentException("Queue names cannot contain whitespace");
        }

        return createSubscription(NatsEncoder.encodeSubject(subject), NatsEncoder.encodeQueue(queueName), null);
    }

    void invalidate(NatsSubscription sub) {
        ByteBuffer sid = sub.getSIDBuffer();

        subscribers.remove(sid);

        if (sub.getNatsDispatcher() != null) {
            sub.getNatsDispatcher().remove(sub);
        }

        sub.invalidate();
    }

    void unsubscribe(NatsSubscription sub, int after) {
        if (isClosed()) { // last chance, usually sub will catch this
            throw new IllegalStateException("Connection is Closed");
        }

        if (after <= 0) {
            this.invalidate(sub); // Will clean it up
        } else {
            sub.setUnsubLimit(after);

            if (sub.reachedUnsubLimit()) {
                sub.invalidate();
            }
        }

        if (!isConnected()) {
            return;// We will setup sub on reconnect or ignore
        }

        sendUnsub(sub, after);
    }

    void sendUnsub(NatsSubscription sub, int after) {
        ByteBuffer sid = sub.getSIDBuffer();
        byte[] afterBytes = null;
        int afterSize = 0;
        if (after > 0) {
            afterBytes = String.valueOf(after).getBytes();
            afterSize = afterBytes.length + 1;
        }
        ByteBuffer protocolBuilder = ByteBuffer.allocate(5 + 1 + sid.limit() + afterSize + 2);
        protocolBuilder.put(OP_UNSUB.asReadOnlyBuffer());
        protocolBuilder.put((byte)' ');
        protocolBuilder.put(sid);

        if (afterBytes != null) {
            protocolBuilder.put((byte)' ');
            protocolBuilder.put(afterBytes);
        }
        protocolBuilder.put((byte)'\r');
        protocolBuilder.put((byte)'\n');
        protocolBuilder.flip();
        NatsMessage unsubMsg = new NatsMessage(protocolBuilder);
        queueInternalOutgoing(unsubMsg);
    }

    // Assumes the null/empty checks were handled elsewhere
    NatsSubscription createSubscription(ByteBuffer subject, ByteBuffer queueName, NatsDispatcher dispatcher) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining() && (dispatcher == null || dispatcher != this.inboxDispatcher.get())) {
            throw new IllegalStateException("Connection is Draining");
        }

        NatsSubscription sub = null;
        long sidL = nextSid.getAndIncrement();
        ByteBuffer sid = NatsEncoder.encodeSID(String.valueOf(sidL));

        sub = new NatsSubscription(sid, subject, queueName, this, dispatcher);
        subscribers.put(sid, sub);

        sendSubscriptionMessage(sid, subject, queueName, false);
        return sub;
    }

    void sendSubscriptionMessage(ByteBuffer sid, ByteBuffer subject, ByteBuffer queueName, boolean treatAsInternal) {
        if (!isConnected()) {
            return;// We will setup sub on reconnect or ignore
        }

        // use a big buffer, may fail at server, but we don't want to fail here
        int subLength = (subject != null) ? subject.limit() : 0;
        int qLength = (queueName != null) ? queueName.limit() + 1 : 0;
        ByteBuffer protocolBuilder = ByteBuffer.allocate(3 + 1 + subLength + qLength + 1 + sid.limit() + 2);
        protocolBuilder.put(OP_SUB.asReadOnlyBuffer());
        protocolBuilder.put((byte)' ');
        protocolBuilder.put(subject.asReadOnlyBuffer());

        if (queueName != null) {
            protocolBuilder.put((byte)' ');
            protocolBuilder.put(queueName.asReadOnlyBuffer());
        }

        protocolBuilder.put((byte)' ');
        protocolBuilder.put(sid.asReadOnlyBuffer());
        protocolBuilder.put((byte)'\r');
        protocolBuilder.put((byte)'\n');
        protocolBuilder.flip();
        NatsMessage subMsg = new NatsMessage(protocolBuilder);

        if (treatAsInternal) {
            queueInternalOutgoing(subMsg);
        } else {
            queueOutgoing(subMsg);
        }
    }

    public String createInbox() {
        return NatsEncoder.decodeInbox(this.createInboxBuffer());
    }

    public ByteBuffer createInboxBuffer() {
        ByteBuffer prefix = options.getInboxPrefixBuffer();
        ByteBuffer next = this.nuid.nextBuffer();
        ByteBuffer builder = ByteBuffer.allocate(prefix.remaining() + next.limit());
        builder.put(prefix.asReadOnlyBuffer());
        builder.put(next);
        builder.flip();
        return builder;
    }

    int getRespInboxLength() {
        ByteBuffer prefix = options.getInboxPrefixBuffer();
        return prefix.remaining() + 22 + 1; // 22 for nuid, 1 for .
    }

    ByteBuffer createResponseInbox(ByteBuffer inbox) {
        ByteBuffer next = this.nuid.nextBuffer();
        ByteBuffer builder = ByteBuffer.allocate(getRespInboxLength() + next.limit());
        ByteBuffer inboxBuf = inbox.duplicate();
        inboxBuf.limit(getRespInboxLength()); // Get rid of the *

        builder.put(inboxBuf);
        builder.put(next);
        builder.flip();
        return builder;
    }

    ByteBuffer getResponseToken(ByteBuffer responseInbox) {
        int len = getRespInboxLength();
        if (responseInbox.limit() <= len) {
            return responseInbox.duplicate();
        }
        ByteBuffer tokenBuffer = ByteBuffer.allocate(responseInbox.limit() - len);
        ByteBuffer responseInboxBuf = responseInbox.duplicate();
        responseInboxBuf.position(len);
        tokenBuffer.put(responseInboxBuf);
        tokenBuffer.flip();
        return tokenBuffer;
    }

    // If the inbox is long enough, pull out the end part, otherwise, just use the
    // full thing
    ByteBuffer getResponseToken(String responseInbox) {
        return getResponseToken(NatsEncoder.encodeInbox(responseInbox));
    }

    void cleanResponses(boolean cancelIfRunning) {
        ArrayList<ByteBuffer> toRemove = new ArrayList<>();

        responses.forEach((token, f) -> {
            if (f.isDone() || cancelIfRunning) {
                try {
                    f.cancel(true); // does nothing if already done
                } catch (CancellationException e) {
                    // Expected
                }
                toRemove.add(token);
                statistics.decrementOutstandingRequests();
            }
        });

        for (ByteBuffer token : toRemove) {
            responses.remove(token);
        }
    }

    public Message request(String subject, byte[] body, Duration timeout) throws InterruptedException {
        Message reply = null;
        Future<Message> incoming = this.request(subject, body);
        try {
            reply = incoming.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            incoming.cancel(true);
        } catch (Throwable e) {
            throw new AssertionError(e);
        }

        return reply;
    }

    public CompletableFuture<Message> request(String subject, byte[] body) {
        return this.request((subject != null) ? NatsEncoder.encodeSubject(subject) : null, body);
    }

    public CompletableFuture<Message> request(ByteBuffer subject, byte[] body) {
        ByteBuffer responseInbox = null;
        boolean oldStyle = options.isOldRequestStyle();

        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining()) {
            throw new IllegalStateException("Connection is Draining");
        }

        if (subject == null || subject.limit() == 0) {
            throw new IllegalArgumentException("Subject is required in publish");
        }

        if (body == null) {
            body = EMPTY_BODY;
        } else if (body.length > this.getMaxPayload() && this.getMaxPayload() > 0) {
            throw new IllegalArgumentException(
                    "Message payload size exceed server configuration " + body.length + " vs " + this.getMaxPayload());
        }

        if (inboxDispatcher.get() == null) {
            NatsDispatcher d = new NatsDispatcher(this, (msg) -> {
                deliverReply(msg);
            });

            if (inboxDispatcher.compareAndSet(null, d)) {
                ByteBuffer id = this.nuid.nextBuffer();
                this.dispatchers.put(id, d);
                d.start(id);
                d.subscribe(this.mainInbox);
            }
        }

        if (oldStyle) {
            responseInbox = createInboxBuffer();
        } else {
            responseInbox = createResponseInbox(this.mainInbox);
        }

        ByteBuffer responseToken = getResponseToken(responseInbox);
        CompletableFuture<Message> future = new CompletableFuture<>();

        if (!oldStyle) {
            responses.put(responseToken, future);
        }
        statistics.incrementOutstandingRequests();

        if (oldStyle) {
            NatsDispatcher dispatcher = this.inboxDispatcher.get();
            NatsSubscription sub = dispatcher.subscribeReturningSubscription(responseInbox);
            dispatcher.unsubscribe(responseInbox, 1);
            // Unsubscribe when future is cancelled:
            ByteBuffer finalResponseInbox = responseInbox;
            future.whenComplete((msg, exception) -> {
                if ( null != exception && exception instanceof CancellationException ) {
                    dispatcher.unsubscribe(finalResponseInbox);
                }
            });
            responses.put(sub.getSIDBuffer(), future);
        }

        this.publish(subject, responseInbox, body);
        statistics.incrementRequestsSent();

        return future;
    }

    void deliverReply(Message msg) {
        boolean oldStyle = options.isOldRequestStyle();
        ByteBuffer subject = msg.getSubjectBuffer();
        ByteBuffer token = getResponseToken(subject);
        CompletableFuture<Message> f = null;

        if (oldStyle) {
            f = responses.remove(msg.getSIDBuffer());
        } else {
            f = responses.remove(token);
        }

        if (f != null) {
            statistics.decrementOutstandingRequests();
            f.complete(msg);
            statistics.incrementRepliesReceived();
        } else if (!oldStyle && !subject.duplicate().limit(mainInbox.limit()).equals(mainInbox)) {
            System.out.println("ERROR: Subject remapping requires Options.oldRequestStyle() to be set on the Connection");
        }
    }

    public Dispatcher createDispatcher(MessageHandler handler) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining()) {
            throw new IllegalStateException("Connection is Draining");
        }

        NatsDispatcher dispatcher = new NatsDispatcher(this, handler);
        ByteBuffer id = this.nuid.nextBuffer();
        this.dispatchers.put(id, dispatcher);
        dispatcher.start(id);
        return dispatcher;
    }

    public void closeDispatcher(Dispatcher d) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (!(d instanceof NatsDispatcher)) {
            throw new IllegalArgumentException("Connection can only manage its own dispatchers");
        }

        NatsDispatcher nd = ((NatsDispatcher) d);

        if (nd.isDraining()) {
            return; // No op while draining
        }

        if (!this.dispatchers.containsKey(nd.getId())) {
            throw new IllegalArgumentException("Dispatcher is already closed.");
        }

        cleanupDispatcher(nd);
    }

    void cleanupDispatcher(NatsDispatcher nd) {
        nd.stop(true);
        this.dispatchers.remove(nd.getId());
    }

    public void flush(Duration timeout) throws TimeoutException, InterruptedException {

        Instant start = Instant.now();
        waitForConnectOrClose(timeout);

        if (isClosed()) {
            throw new TimeoutException("Attempted to flush while closed");
        }

        if (timeout == null) {
            timeout = Duration.ZERO;
        }

        Instant now = Instant.now();
        Duration waitTime = Duration.between(start, now);

        if (!timeout.equals(Duration.ZERO) && waitTime.compareTo(timeout) >= 0) {
            throw new TimeoutException("Timeout out waiting for connection before flush.");
        }

        try {
            Future<Boolean> waitForIt = sendPing();

            if (waitForIt == null) { // error in the send ping code
                return;
            }

            long nanos = timeout.toNanos();

            if (nanos > 0) {

                nanos -= waitTime.toNanos();

                if (nanos <= 0) {
                    nanos = 1; // let the future timeout if it isn't resolved
                }

                waitForIt.get(nanos, TimeUnit.NANOSECONDS);
            } else {
                waitForIt.get();
            }

            this.statistics.incrementFlushCounter();
        } catch (ExecutionException | CancellationException e) {
            throw new TimeoutException(e.getMessage());
        }
    }

    void sendConnect(String serverURI) throws IOException {
        try {
            NatsServerInfo info = this.serverInfo.get();
            ByteBuffer connectOptions = this.options.buildProtocolConnectOptionsString(serverURI, info.isAuthRequired(), info.getNonce());
            ByteBuffer connectString = ByteBuffer.allocate(7 + 1 + connectOptions.limit() + 2);
            connectString.put(NatsConnection.OP_CONNECT.asReadOnlyBuffer());
            connectString.put((byte)' ');
            connectString.put(connectOptions.asReadOnlyBuffer());
            connectString.put((byte)'\r');
            connectString.put((byte)'\n');
            connectString.flip();
            NatsMessage msg = new NatsMessage(connectString);
            
            queueInternalOutgoing(msg);
        } catch (Exception exp) {
            throw new IOException("Error sending connect string", exp);
        }
    }
    
    CompletableFuture<Boolean> sendPing() {
        return this.sendPing(true);
    }

    CompletableFuture<Boolean> softPing() {
        return this.sendPing(false);
    }

    // Send a ping request and push a pong future on the queue.
    // futures are completed in order, keep this one if a thread wants to wait
    // for a specific pong. Note, if no pong returns the wait will not return
    // without setting a timeout.
    CompletableFuture<Boolean> sendPing(boolean treatAsInternal) {
        int max = this.options.getMaxPingsOut();

        if (!isConnectedOrConnecting()) {
            CompletableFuture<Boolean> retVal = new CompletableFuture<Boolean>();
            retVal.complete(Boolean.FALSE);
            return retVal;
        }

        if (!treatAsInternal && !this.needPing.get()) {
            CompletableFuture<Boolean> retVal = new CompletableFuture<Boolean>();
            retVal.complete(Boolean.TRUE);
            this.needPing.set(true);
            return retVal;
        }

        if (max > 0 && pongQueue.size() + 1 > max) {
            handleCommunicationIssue(new IllegalStateException("Max outgoing Ping count exceeded."));
            return null;
        }

        CompletableFuture<Boolean> pongFuture = new CompletableFuture<>();
        NatsMessage msg = new NatsMessage(NatsConnection.OP_PING_LINE.duplicate());
        pongQueue.add(pongFuture);

        if (treatAsInternal) {
            queueInternalOutgoing(msg);
        } else {
            queueOutgoing(msg);
        }

        this.needPing.set(true);
        this.statistics.incrementPingCount();
        return pongFuture;
    }

    void sendPong() {
        NatsMessage msg = new NatsMessage(NatsConnection.OP_PONG_LINE.duplicate());
        queueInternalOutgoing(msg);
    }

    // Called by the reader
    void handlePong() {
        CompletableFuture<Boolean> pongFuture = pongQueue.pollFirst();
        if (pongFuture != null) {
            pongFuture.complete(Boolean.TRUE);
        }
    }

    void readInitialInfo() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(options.getBufferSize());
        ByteBuffer protocolBuffer = ByteBuffer.allocate(options.getBufferSize());
        boolean gotCRLF = false;
        boolean gotCR = false;

        while (!gotCRLF) {
            try {
                if (this.dataPort.read(readBuffer).get() < 0) {
                    break;
                }
                readBuffer.flip();
            } catch (ExecutionException | InterruptedException ex) {
                final Throwable cause = ex.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
            }

            while (readBuffer.hasRemaining()) {
                byte b = readBuffer.get();

                if (gotCR) {
                    if (b != LF) {
                        throw new IOException("Missed LF after CR waiting for INFO.");
                    } else if (readBuffer.hasRemaining()) {
                        throw new IOException("Read past initial info message.");
                    }

                    gotCRLF = true;
                    break;
                }

                if (b == CR) {
                    gotCR = true;
                } else {
                    if (!protocolBuffer.hasRemaining()) {
                        if (protocolBuffer.limit() == protocolBuffer.capacity()) {
                            protocolBuffer = enlargeBuffer(protocolBuffer, 0); // just double it
                        } else {
                            protocolBuffer.limit(protocolBuffer.limit() + 1);
                        }
                    }
                    protocolBuffer.put(b);
                }
            }
            readBuffer.compact();

            if (gotCRLF) {
                break;
            }
        }

        if (!gotCRLF) {
            throw new IOException("Failed to read initial info message.");
        }

        protocolBuffer.flip();

        ByteBuffer op = ByteBuffer.allocate(OP_INFO.limit());
        while (protocolBuffer.hasRemaining()) {
            byte b = protocolBuffer.get();
            if (b == (byte)' ' || b == (byte)'\t') {
                break;
            } else if (op.hasRemaining()) {
                op.put(b);
            }
        }
        op.flip();

        if (!OP_INFO.equals(op)) {
            throw new IOException("Received non-info initial message.");
        }

        String infoJson = StandardCharsets.UTF_8.decode(protocolBuffer).toString();

        handleInfo(infoJson);
    }

    void handleInfo(String infoJson) {
        NatsServerInfo serverInfo = new NatsServerInfo(infoJson);
        this.serverInfo.set(serverInfo);

        String[] urls = this.serverInfo.get().getConnectURLs();
        if (urls != null && urls.length > 0) {
            processConnectionEvent(Events.DISCOVERED_SERVERS);
        }

        if (serverInfo.isLameDuckMode()) {
            processConnectionEvent(Events.LAME_DUCK);
        }
    }

    void queueOutgoing(NatsMessage msg) {
        if (msg.getControlLineLength() > this.options.getMaxControlLine()) {
            throw new IllegalArgumentException("Control line is too long");
        }
        if (!this.writer.queue(msg)) {
            ErrorListener errorListener = this.options.getErrorListener();
            if (errorListener != null) {
                errorListener.messageDiscarded(this, msg);
            }
        }
    }

    void queueInternalOutgoing(NatsMessage msg) {
        if (msg.getControlLineLength() > this.options.getMaxControlLine()) {
            throw new IllegalArgumentException("Control line is too long");
        }
        this.writer.queueInternalMessage(msg);
    }

    void deliverMessage(NatsMessage msg) {
        this.needPing.set(false);
        this.statistics.incrementInMsgs();
        this.statistics.incrementInBytes(msg.getSizeInBytes());

        NatsSubscription sub = subscribers.get(msg.getSIDBuffer());

        if (sub != null) {
            msg.setSubscription(sub);

            NatsDispatcher d = sub.getNatsDispatcher();
            NatsConsumer c = (d == null) ? sub : d;
            MessageQueue q = ((d == null) ? sub.getMessageQueue() : d.getMessageQueue());

            if (c.hasReachedPendingLimits()) {
                // Drop the message and count it
                this.statistics.incrementDroppedCount();
                c.incrementDroppedCount();

                // Notify the first time
                if (!c.isMarkedSlow()) {
                    c.markSlow();
                    processSlowConsumer(c);
                }
            } else if (q != null) {
                c.markNotSlow();
                q.push(msg);
            }

        } else {
            // Drop messages we don't have a subscriber for (could be extras on an
            // auto-unsub for example)
        }
    }

    void processOK() {
        this.statistics.incrementOkCount();
    }

    void processSlowConsumer(Consumer consumer) {
        ErrorListener handler = this.options.getErrorListener();

        if (handler != null && !this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        handler.slowConsumerDetected(this, consumer);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processException(Exception exp) {
        ErrorListener handler = this.options.getErrorListener();

        this.statistics.incrementExceptionCount();

        if (handler != null && !this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        handler.exceptionOccurred(this, exp);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processError(String errorText) {
        ErrorListener handler = this.options.getErrorListener();

        this.statistics.incrementErrCount();

        this.lastError.set(errorText);
        this.connectError.set(errorText); // even if this isn't during connection, save it just in case

        // If we are connected && we get an authentication error, save it
        String url = this.getConnectedUrl();
        if (this.isConnected() && this.isAuthenticationError(errorText) && url != null) {
            this.serverAuthErrors.put(url, errorText);
        }

        if (handler != null && !this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        handler.errorOccurred(this, errorText);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processConnectionEvent(Events type) {
        ConnectionListener handler = this.options.getConnectionListener();

        if (handler != null && !this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        handler.connectionEvent(this, type);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    NatsServerInfo getInfo() {
        return this.serverInfo.get();
    }

    public Options getOptions() {
        return this.options;
    }

    public Statistics getStatistics() {
        return this.statistics;
    }

    NatsStatistics getNatsStatistics() {
        return this.statistics;
    }

    DataPort getDataPort() {
        return this.dataPort;
    }

    // Used for testing
    int getConsumerCount() {
        return this.subscribers.size() + this.dispatchers.size();
    }

    public long getMaxPayload() {
        NatsServerInfo info = this.serverInfo.get();

        if (info == null) {
            return -1;
        }

        return info.getMaxPayload();
    }

    public Collection<String> getServers() {
        NatsServerInfo info = this.serverInfo.get();
        HashSet<String> check = new HashSet<String>();
        ArrayList<String> servers = new ArrayList<>();

        options.getServers().stream().forEach(x -> {
            String uri = x.toString();
            if (!check.contains(uri)) {
                servers.add(uri);
                check.add(uri);
            }
        });

        if (info != null && info.getConnectURLs() != null) {
            for (String uri : info.getConnectURLs()) {
                if (!check.contains(uri)) {
                    servers.add(uri);
                    check.add(uri);
                }
            }
        }

        return servers;
    }

    public String getConnectedUrl() {
        return this.currentServerURI;
    }

    public Status getStatus() {
        return this.status;
    }

    public String getLastError() {
        return this.lastError.get();
    }

    ExecutorService getExecutor() {
        return executor;
    }

    void updateStatus(Status newStatus) {
        Status oldStatus = this.status;

        statusLock.lock();
        try {
            if (oldStatus == Status.CLOSED || newStatus == oldStatus) {
                return;
            }
            this.status = newStatus;
        } finally {
            statusChanged.signalAll();
            statusLock.unlock();
        }

        if (this.status == Status.DISCONNECTED) {
            processConnectionEvent(Events.DISCONNECTED);
        } else if (this.status == Status.CLOSED) {
            processConnectionEvent(Events.CLOSED);
        } else if (oldStatus == Status.RECONNECTING && this.status == Status.CONNECTED) {
            processConnectionEvent(Events.RECONNECTED);
        } else if (this.status == Status.CONNECTED) {
            processConnectionEvent(Events.CONNECTED);
        }
    }

    boolean isClosing() {
        return this.closing;
    }

    boolean isClosed() {
        return this.status == Status.CLOSED;
    }

    boolean isConnected() {
        return this.status == Status.CONNECTED;
    }

    boolean isConnectedOrConnecting() {
        statusLock.lock();
        try {
            return this.status == Status.CONNECTED || this.connecting;
        } finally {
            statusLock.unlock();
        }
    }

    boolean isDisconnectingOrClosed() {
        statusLock.lock();
        try {
            return this.status == Status.CLOSED || this.disconnecting;
        } finally {
            statusLock.unlock();
        }
    }

    boolean isDisconnecting() {
        statusLock.lock();
        try {
            return this.disconnecting;
        } finally {
            statusLock.unlock();
        }
    }

    void waitForDisconnectOrClose(Duration timeout) throws InterruptedException {
        waitFor(timeout, (Void) -> {
            return this.isDisconnecting() && !this.isClosed();
        });
    }

    void waitForConnectOrClose(Duration timeout) throws InterruptedException {
        waitFor(timeout, (Void) -> {
            return !this.isConnected() && !this.isClosed();
        });
    }

    void waitFor(Duration timeout, Predicate<Void> test) throws InterruptedException {
        statusLock.lock();
        try {
            long currentWaitNanos = (timeout != null) ? timeout.toNanos() : -1;
            long start = System.nanoTime();
            while (currentWaitNanos >= 0 && test.test(null)) {
                if (currentWaitNanos > 0) {
                    statusChanged.await(currentWaitNanos, TimeUnit.NANOSECONDS);
                    long now = System.nanoTime();
                    currentWaitNanos = currentWaitNanos - (now - start);
                    start = now;

                    if (currentWaitNanos <= 0) {
                        break;
                    }
                } else {
                    statusChanged.await();
                }
            }
        } finally {
            statusLock.unlock();
        }
    }

    void waitForReconnectTimeout() {
        Duration waitTime = options.getReconnectWait();
        long currentWaitNanos = (waitTime != null) ? waitTime.toNanos() : -1;
        long start = System.nanoTime();

        while (currentWaitNanos > 0 && !isDisconnectingOrClosed() && !isConnected() && !this.reconnectWaiter.isDone()) {
            try {
                this.reconnectWaiter.get(currentWaitNanos, TimeUnit.NANOSECONDS);
            } catch (Exception exp) {
                // ignore, try to loop again
            }
            long now = System.nanoTime();
            currentWaitNanos = currentWaitNanos - (now - start);
            start = now;
        }

        this.reconnectWaiter.complete(Boolean.TRUE);
    }

    List<String> buildServerList() {
        ArrayList<String> reconnectList = new ArrayList<>();

        reconnectList.addAll(getServers());

        if (options.isNoRandomize()) {
            return reconnectList;
        }

        if (currentServer == null) {
            Collections.shuffle(reconnectList);
        } else {
            // Remove the current server from the list, shuffle if it makes sense,
            // and then add it to the end of the list.  This prevents the client
            // from immediately reconnecting to a server it just lost connection with.
            reconnectList.remove(this.currentServer);
            if (reconnectList.size() > 1) {
                Collections.shuffle(reconnectList);
            }
            reconnectList.add(this.currentServer);
        }
        return reconnectList;
    }

    ByteBuffer enlargeBuffer(ByteBuffer buffer, int atLeast) {
        int current = buffer.capacity();
        int newSize = Math.max(current * 2, atLeast);
        ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
        buffer.flip();
        newBuffer.put(buffer);
        return newBuffer;
    }

    // For testing
    NatsConnectionReader getReader() {
        return this.reader;
    }

    // For testing
    NatsConnectionWriter getWriter() {
        return this.writer;
    }

    // For testing
    Future<DataPort> getDataPortFuture() {
        return this.dataPortFuture;
    }

    boolean isDraining() {
        return this.draining.get() != null;
    }

    boolean isDrained() {
        CompletableFuture<Boolean> tracker = this.draining.get();

        try {
            if (tracker != null && tracker.getNow(false)) {
                return true;
            }
        } catch (Exception e) {
            // These indicate the tracker was cancelled/timed out
        }

        return false;
    }

    public CompletableFuture<Boolean> drain(Duration timeout) throws TimeoutException, InterruptedException {

        if (isClosing() || isClosed()) {
            throw new IllegalStateException("A connection can't be drained during close.");
        }

        this.statusLock.lock();
        try {
            if (isDraining()) {
                return this.draining.get();
            }
            this.draining.set(new CompletableFuture<>());
        } finally {
            this.statusLock.unlock();
        }
        
        final CompletableFuture<Boolean> tracker = this.draining.get();
        Instant start = Instant.now();

        // Don't include subscribers with dispatchers
        HashSet<NatsSubscription> pureSubscribers = new HashSet<>();
        pureSubscribers.addAll(this.subscribers.values());
        pureSubscribers.removeIf((s) -> {
            return s.getDispatcher() != null;
        });

        final HashSet<NatsConsumer> consumers = new HashSet<>();
        consumers.addAll(pureSubscribers);
        consumers.addAll(this.dispatchers.values());

        NatsDispatcher inboxer = this.inboxDispatcher.get();

        if(inboxer != null) {
            consumers.add(inboxer);
        }

        // Stop the consumers NOW so that when this method returns they are blocked
        consumers.forEach((cons) -> {
            cons.markDraining(tracker);
            cons.sendUnsubForDrain();
        });

        try {
            this.flush(timeout); // Flush and wait up to the timeout, if this fails, let the caller know
        } catch (Exception e) {
            this.close(false);
            throw e;
        }

        consumers.forEach((cons) -> {
            cons.markUnsubedForDrain();
        });

        // Wait for the timeout or the pending count to go to 0
        executor.submit(() -> {
            try {
                Instant now = Instant.now();

                while (timeout == null || timeout.equals(Duration.ZERO)
                        || Duration.between(start, now).compareTo(timeout) < 0) {
                    for (Iterator<NatsConsumer> i = consumers.iterator(); i.hasNext();) {
                        NatsConsumer cons = i.next();
                        if (cons.isDrained()) {
                            i.remove();
                        }
                    }
                    
                    if (consumers.size() == 0) {
                        break;
                    }

                    Thread.sleep(1); // Sleep 1 milli

                    now = Instant.now();
                }

                // Stop publishing
                this.blockPublishForDrain.set(true);

                // One last flush
                if (timeout == null || timeout.equals(Duration.ZERO)) {
                    this.flush(Duration.ZERO);
                } else {
                    now = Instant.now();

                    Duration passed = Duration.between(start, now);
                    Duration newTimeout = timeout.minus(passed);

                    if (newTimeout.toNanos() > 0) {
                        this.flush(newTimeout);
                    }
                }

                this.close(false); // close the connection after the last flush
                tracker.complete(consumers.size() == 0);
            } catch (TimeoutException | InterruptedException e) {
                this.processException(e);
            } finally {
                try {
                    this.close(false);// close the connection after the last flush
                } catch (InterruptedException e) {
                    this.processException(e);
                }
                tracker.complete(false);
            }
        });

        return tracker;
    }

    boolean isAuthenticationError(String err) {
        if (err == null) {
            return false;
        }
        err = err.toLowerCase();
        return err.startsWith("user authentication") || err.contains("authorization violation");
    }
}
