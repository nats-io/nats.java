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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import io.nats.client.Connection;
import io.nats.client.ConnectionHandler;
import io.nats.client.Dispatcher;
import io.nats.client.ErrorHandler;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;
import io.nats.client.Options;
import io.nats.client.Statistics;
import io.nats.client.Subscription;

// TODO(sasbury): connection and reconnect and error notifcations
// TODO(sasbury): Throw exception on publish, etc. if not connected
// TODO(sasbury): Ping timer w/tests maybe via stats
class NatsConnection implements Connection {

    public enum State {
        UNCONNECTED,
        CONNECTING,
        CONNECTED,
        DISCONNECTING,
        DISCONNECTED,
        CLOSING_SOCKET,
        CLOSING,
        CLOSED;
    }

    static final int MAX_PROTOCOL_LINE = 1024;
    static final int BUFFER_SIZE = 32 * 1024;
    static final byte[] EMPTY_BODY = new byte[0];

    static final String INBOX_PREFIX = "_INBOX.";

    static final byte CR = 0x0D;
    static final byte LF = 0x0A;
    static final byte[] CRLF = { CR, LF };

    static final String OP_CONNECT = "CONNECT";
    static final String OP_INFO = "INFO";
    static final String OP_SUB = "SUB";
    static final String OP_UNSUB = "UNSUB";
    static final String OP_MSG = "MSG";
    static final String OP_PING = "PING";
    static final String OP_PONG = "PONG";
    static final String OP_OK = "+OK";
    static final String OP_ERR = "-ERR";

    private Options options;

    private NatsStatistics statistics;

    // State is used internally to create a sort of state engine
    // and insure the connection moves through reasonable transitions
    // despite threading.
    private State state;

    // Status is an external, simpler value than state, that is matches
    // other NATS APIs.
    private Status status;
    private ReentrantLock statusLock;

    private CompletableFuture<DataPort> dataPortFuture;
    private DataPort dataPort;
    private String currentServerURI;
    private CompletableFuture<Boolean> reconnectWaiter;

    private NatsConnectionReader reader;
    private NatsConnectionWriter writer;

    private AtomicReference<NatsServerInfo> serverInfo;

    private ReentrantLock subscriberLock;
    private HashMap<String, NatsSubscription> subscribers;
    private ArrayList<NatsDispatcher> dispatchers;

    private ReentrantLock responseLock;
    private HashMap<String, CompletableFuture<Message>> responses;
    private String mainInbox;
    private Dispatcher inboxDispatcher;
    private Timer responseCleanupTimer;

    private long nextSid;
    private NUID nuid;

    private ConcurrentLinkedDeque<CompletableFuture<Boolean>> pongQueue;

    private ConnectionHandler connectionHandler;
    private ErrorHandler errorHandler;

    NatsConnection(Options options) {
        this.options = options;

        this.statistics = new NatsStatistics();

        this.statusLock = new ReentrantLock();
        this.state = State.UNCONNECTED;
        this.status = Status.DISCONNECTED;
        this.reconnectWaiter = new CompletableFuture<>();
        this.reconnectWaiter.complete(Boolean.TRUE);

        this.subscriberLock = new ReentrantLock();
        this.subscribers = new HashMap<>();
        this.dispatchers = new ArrayList<>();
        this.responses = new HashMap<>();
        this.responseLock = new ReentrantLock();
        this.nextSid = 1;
        this.nuid = new NUID();
        this.mainInbox = createInbox() + ".*";
        this.responseCleanupTimer = null;

        this.serverInfo = new AtomicReference<>();
        this.pongQueue = new ConcurrentLinkedDeque<>();

        this.reader = new NatsConnectionReader(this);
        this.writer = new NatsConnectionWriter(this);
    }

    // Connect is only called after creation
    void connect() throws InterruptedException {
        Duration connectTimeout = options.getConnectionTimeout();
        
        if (options.getServers().size() == 0) {
            throw new IllegalArgumentException("No servers provided in options");
        }

        // Ensure we are only called when unconnected (the initial state)
        if (!transitionState(State.UNCONNECTED, State.DISCONNECTED)) {
            return;
        }

        for (String serverURI : getServers()) {
            
            this.waitForState(State.DISCONNECTED, this.options.getConnectionTimeout());
            
            // Throughout the loop, we can only try to connect, if we are disconnected
            if (!transitionState(State.DISCONNECTED, State.CONNECTING)) {
                break;
            }

            updateStatus(Status.CONNECTING);

            tryToConnect(serverURI);

            if (isConnected()) {
                break;
            } else {
                transitionState(State.CONNECTING, State.DISCONNECTED);
                updateStatus(Status.DISCONNECTED);
            }
        }
    }

    // Reconnect can only be called when the connection is disconnected
    void reconnect() {
        // TODO(sasbury): LOTS OF TESTS during reconnect scenarios
        long maxTries = options.getMaxReconnect();
        long tries = 0;
        String lastServer = null;

        if (maxTries == 0) {
            this.close();
            return;
        }
        
        updateStatus(Status.RECONNECTING);

        while (this.state == State.DISCONNECTED) {
            Collection<String> serversToTry = buildReconnectList();

            for (String server : serversToTry) {
                if (server.equals(lastServer)) {
                    this.reconnectWaiter = new CompletableFuture<>();
                    waitForReconnectTimeout();
                } else {
                    this.waitForState(State.DISCONNECTED, this.options.getConnectionTimeout());
                }

                if (!transitionState(State.DISCONNECTED, State.CONNECTING)) {
                    break;
                }
        
                tryToConnect(server);
                lastServer = server;
                tries++;
                
                if (tries > maxTries && maxTries > 0) {
                    break;
                } else if (isConnected()) {
                    break;
                } else {
                    transitionState(State.CONNECTING, State.DISCONNECTED);
                    updateStatus(Status.RECONNECTING);
                }
            }
        }

        if (!isConnected()) {
            this.close();
            return;
        }

        // TODO(sasbury): NEED TO HANDLE FAILURE DURING resending subscriptions (may have issues like pub exception)

        subscriberLock.lock();
        try {
            for (NatsSubscription sub : this.subscribers.values()) {
                sendSubscriptionMessage(sub.getSID(), sub.getSubject(), sub.getQueueName());
            }

            for (NatsDispatcher d : this.dispatchers) {
                d.resendSubscriptions();
            }
        } finally {
            subscriberLock.unlock();
        }
    }

    // is called from reconnect and connect
    // will wait for any previous attempt to complete, using the reader.stop and writer.stop
    void tryToConnect(String serverURI) {
        try {
            Duration connectTimeout = options.getConnectionTimeout();

            // Create a new future for the SocketChannel, the reader/writer will use this
            // to wait for the connect/failure.
            this.dataPortFuture = new CompletableFuture<>();
 
            // Make sure the reader and writer are stopped
            this.reader.stop().get();
            this.writer.stop().get();

            this.cleanUpPongQueue();

            DataPort newDataPort = this.options.buildDataPort();
            newDataPort.connect(serverURI, this);

            // Notify the any threads waiting on the sockets
            this.dataPort = newDataPort;
            this.dataPortFuture.complete(this.dataPort);

            // Wait for the INFO message manually
            // all other traffic will use the reader and writer
            readInitialInfo();

            upgradeToSecureIfNeeded();
            
            // Start the reader and writer after we secured the connection, if necessary
            this.reader.start(this.dataPortFuture);
            this.writer.start(this.dataPortFuture);

            this.sendConnect();
            Future<Boolean> pongFuture = sendPing();
            pongFuture.get(connectTimeout.toMillis(), TimeUnit.MILLISECONDS);

            // Set connected status
            transitionState(State.CONNECTING, State.CONNECTED);
            updateStatus(Status.CONNECTED);
            this.currentServerURI = serverURI;
        } catch (IOException | InterruptedException | CancellationException | TimeoutException | ExecutionException ex) {
            handleConnectIssue(ex);
        }
    }

    void upgradeToSecureIfNeeded() throws IOException {
        Options opts = getOptions();
        NatsServerInfo info = getInfo();

        if (opts.isSSLRequired() && !info.isTLSRequired()) {
            throw new IOException("SSL connection wanted by client.");
        } else if (!opts.isSSLRequired() && info.isTLSRequired()) {
            throw new IOException("SSL required by server.");
        }

        if (opts.isSSLRequired()) {
            this.dataPort.upgradeToSecure();
        }
    }

    // Called from connect threads
    void handleConnectIssue(Exception io) {
        // TODO(sasbury): Something with exceptions if they are not null
        // io.printStackTrace();
            
        // We can handle this in the connect thread
        this.closeSocket(State.DISCONNECTED);
        // Connect thread will deal with reconnect if necessary
    }

    // Called from reader/writer thread
    void handleCommunicationIssue(Exception io) {
        // TODO(sasbury): Something with exceptions if they are not null
        // io.printStackTrace();

        // Spawn a thread so we don't have timing issues with
        // waiting on read/write threads
        Thread t = new Thread(() -> {
            if(this.closeSocket(State.DISCONNECTED)) {
                this.reconnect();
            }
        });
        t.start();
    }

    // Close socket can perform three state transitions:
    // * Connected -> Disconnecting
    // * Connecting -> Disconnecting
    // * ClosingSocket -> Closing
    // The first of these leads to a reconnect attempt. The others do not.
    // All other entry states (Closed, Disconnecting, Disconnected) result in a no-op.
    boolean closeSocket(State exitState) {
        boolean wasConnected = transitionState(State.CONNECTED, State.DISCONNECTING);
        boolean wasConnectingOrDisconnecting = transitionState(State.CONNECTING, State.DISCONNECTING) || 
                                                    transitionState(State.CLOSING_SOCKET, State.DISCONNECTING);
 
        if (!wasConnected && !wasConnectingOrDisconnecting) {
            return false;
        }

        this.currentServerURI = null;

        // Signal the reader and writer
        this.reader.stop();
        this.writer.stop();

        // Close the current socket and cancel anyone waiting for it
        this.dataPortFuture.cancel(true);

        try {
            if (this.dataPort != null) {
                this.dataPort.close();
            }
        } catch (IOException ex) {
            // Issue closing the socket, but we will just move on
        }

        cleanUpPongQueue();

        // We signalled now wait for them to stop
        try {
            this.reader.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            // Ignore these, issue with future so move on
        }
        try {
            this.writer.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            // Ignore these, issue with future so move on
        }

        transitionState(State.DISCONNECTING, exitState);
        updateStatus(Status.DISCONNECTED);
        
        return wasConnected;
    }

    // Close performs 3 state transitions:
    // * Connecting -> ClosingSocket
    // * Connected -> ClosingSocket
    // * Disconnected -> ClosingSocket
    // Once complete, the socket close will put the state to Closing
    // Once close completes the state will move to closed, and the status is updated
    public void close() {
        boolean notClosingOrClosed = transitionState(State.CONNECTING, State.CLOSING_SOCKET) || 
                                                transitionState(State.CONNECTED, State.CLOSING_SOCKET) ||
                                                transitionState(State.DISCONNECTING, State.CLOSING_SOCKET) ||
                                                transitionState(State.DISCONNECTED, State.CLOSING_SOCKET);

        if(!notClosingOrClosed) {
            return;
        }

        // Stop the reconnect wait timer after we stop the writer/reader (only if we are really closing, not on errors)
        if (this.reconnectWaiter != null) {
            this.reconnectWaiter.cancel(true);
        }

        closeSocket(State.CLOSING);

        subscriberLock.lock();
        try {
            for (NatsDispatcher d : this.dispatchers) {
                d.stop();
            }

            this.dispatchers.clear();

            for (NatsSubscription s : this.subscribers.values()) {
                s.invalidate();
            }

            this.subscribers.clear();
            

            if (responseCleanupTimer != null) {
                responseCleanupTimer.cancel();
            }

            cleanResponses(true);
        } finally {
            subscriberLock.unlock();
        }

        cleanUpPongQueue();

        statusLock.lock();
        try {
            this.state = State.CLOSED;
            updateStatus(Status.CLOSED);
        } finally {
            statusLock.unlock();
        }
    }

    void cleanUpPongQueue() {
        Future<Boolean> b;
        while ((b = pongQueue.poll()) != null) {
            try {
                b.cancel(true);
            } catch (CancellationException e) {
                // Ignore these
            }
        }
    }

    public void publish(String subject, byte[] body) {
        this.publish(subject, null, body);
    }

    public void publish(String subject, String replyTo, byte[] body) {

        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        }

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in publish");
        }

        if (replyTo != null && replyTo.length() == 0) {
            throw new IllegalArgumentException("ReplyTo cannot be the empty string");
        }

        if (body == null) {
            body = EMPTY_BODY;
        } else if (body.length > this.getMaxPayload() && this.getMaxPayload() > 0) {
            throw new IllegalArgumentException("Message payload size exceed server configuraiton "+body.length+" vs "+this.getMaxPayload());
        }
        
        NatsMessage msg = new NatsMessage(subject, replyTo, body);

        if (this.status == Status.RECONNECTING && !this.writer.canQueue(msg, options.getReconnectBufferSize())) {
            throw new IllegalStateException("Unable to queue any more messages during reconnect, max buffer is "+getMaxPayload());
        }

        this.statistics.incrementOutMsgs();
        this.statistics.incrementOutBytes(msg.getSize());
        this.writer.queue(msg);
    }

    public Subscription subscribe(String subject) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        return createSubscription(subject, null, null);
    }

    public Subscription subscribe(String subject, String queueName) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }

        return createSubscription(subject, queueName, null);
    }

    void invalidate(NatsSubscription sub) {
        String sid = sub.getSID();

        subscriberLock.lock();
        try {
            subscribers.remove(sid);
        } finally {
            subscriberLock.unlock();
        }

        if (sub.getDispatcher() != null) {
            sub.getDispatcher().remove(sub);
        }

        sub.invalidate();
    }

    void unsubscribe(NatsSubscription sub, int after) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        }

        if (after <= 0) {
            this.invalidate(sub); // Will clean it up
        } else {
            sub.setMax(after);

            if (sub.reachedMax()) {
                sub.invalidate();
            }
        }

        if (this.state != State.CONNECTED) { 
            return;// We will setup sub on reconnect or ignore
        }

        String sid = sub.getSID();
        StringBuilder protocolBuilder = new StringBuilder();
        protocolBuilder.append(OP_UNSUB);
        protocolBuilder.append(" ");
        protocolBuilder.append(sid);

        if (after > 0) {
            protocolBuilder.append(" ");
            protocolBuilder.append(String.valueOf(after));
        }
        NatsMessage unsubMsg = new NatsMessage(protocolBuilder.toString());
        this.writer.queue(unsubMsg);
    }

    // Assumes the null/empty checks were handled elsewhere
    NatsSubscription createSubscription(String subject, String queueName, NatsDispatcher dispatcher) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        }

        NatsSubscription sub = null;
        subscriberLock.lock();
        try {
            String sid = String.valueOf(this.nextSid);
            sub = new NatsSubscription(sid, subject, queueName, this, dispatcher);
            nextSid++;
            subscribers.put(sid, sub);
            sendSubscriptionMessage(sid, subject, queueName);
        } finally {
            subscriberLock.unlock();
        }
        return sub;
    }

    void sendSubscriptionMessage(String sid, String subject, String queueName) {
        if (this.state != State.CONNECTED) { 
            return;// We will setup sub on reconnect or ignore
        }

        StringBuilder protocolBuilder = new StringBuilder();
        protocolBuilder.append(OP_SUB);
        protocolBuilder.append(" ");
        protocolBuilder.append(subject);

        if (queueName != null) {
            protocolBuilder.append(" ");
            protocolBuilder.append(queueName);
        }

        protocolBuilder.append(" ");
        protocolBuilder.append(sid);
        NatsMessage subMsg = new NatsMessage(protocolBuilder.toString());
        this.writer.queue(subMsg);
    }

    String createInbox() {
        StringBuilder builder = new StringBuilder();
        builder.append(INBOX_PREFIX);
        builder.append(this.nuid.next());
        return builder.toString();
    }

    static final int RESP_INBOX_PREFIX_LEN = INBOX_PREFIX.length() + 22 + 1; //22 for nuid, 1 for .

    String createResponseInbox(String inbox) {
        StringBuilder builder = new StringBuilder();
        builder.append(inbox.substring(0, RESP_INBOX_PREFIX_LEN)); // Get rid of the *
        builder.append(this.nuid.next());
        return builder.toString();
    }

    // If the inbox is long enough, pull out the end part, otherwise, just use the full thing
    String getResponseToken(String responseInbox) {
        if (responseInbox.length() <= RESP_INBOX_PREFIX_LEN) {
            return responseInbox;
        }
        return responseInbox.substring(RESP_INBOX_PREFIX_LEN);
    }

    void cleanResponses(boolean cancelIfRunning) {
        responseLock.lock();
        try {
            Iterator<Map.Entry<String,CompletableFuture<Message>>> it = responses.entrySet().iterator();
            
            while (it.hasNext()) {
                Map.Entry<String, CompletableFuture<Message>> pair = it.next();
                CompletableFuture<Message> f = pair.getValue();
                if (f.isDone() || cancelIfRunning) {
                    f.cancel(true); // does nothing if already done
                    it.remove();
                    statistics.decrementOutstandingRequests();
                }
            }
        } finally {
            responseLock.unlock();
        }
    }

    public Future<Message> request(String subject, byte[] body) {
        String responseInbox = null;
        boolean oldStyle = options.isOldRequestStyle();
        
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        }

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in publish");
        }

        if (body == null) {
            body = EMPTY_BODY;
        } else if (body.length > this.getMaxPayload() && this.getMaxPayload() > 0) {
            throw new IllegalArgumentException("Message payload size exceed server configuraiton "+body.length+" vs "+this.getMaxPayload());
        }

        if (oldStyle) {
            responseInbox = createInbox();
        } else {
            responseInbox = createResponseInbox(this.mainInbox);
        }
        
        String responseToken = getResponseToken(responseInbox);
        CompletableFuture<Message> future = new CompletableFuture<>();

        this.subscriberLock.lock();
        try {
            if (this.inboxDispatcher == null) {

                this.inboxDispatcher = this.createDispatcher((msg) -> {
                    deliverRequest(msg);
                });

                this.inboxDispatcher.subscribe(this.mainInbox);

                this.responseCleanupTimer = new Timer();
                this.responseCleanupTimer.schedule(new TimerTask() {
                    public void run() {
                        cleanResponses(false);
                    }
                }, this.options.getRequestCleanupInterval().toMillis());
            }
        } finally {
            this.subscriberLock.unlock();
        }

        responseLock.lock();
        try {
            responses.put(responseToken, future);
        } finally {
            responseLock.unlock();
        }
        statistics.incrementOutstandingRequests();

        if (oldStyle) {
            this.inboxDispatcher.subscribe(responseInbox).unsubscribe(responseInbox,1);
        }
        
        this.publish(subject, responseInbox, body);
        statistics.incrementRequestsSent();

        return future;
    }

    void deliverRequest(Message msg) {
        String subject = msg.getSubject();
        String token = getResponseToken(subject);
        CompletableFuture<Message> f = null;

        responseLock.lock();
        try {
            f = responses.remove(token);
        } finally {
            responseLock.unlock();
        }

        if (f != null) {
            statistics.decrementOutstandingRequests();
            f.complete(msg);
            statistics.incrementRepliesReceived();
        }
    }

    public Dispatcher createDispatcher(MessageHandler handler) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        }

        NatsDispatcher dispatcher = null;
        subscriberLock.lock();
        try {
            dispatcher = new NatsDispatcher(this, handler);
            dispatchers.add(dispatcher);
        } finally {
            subscriberLock.unlock();
        }
        dispatcher.start();
        return dispatcher;
    }

    public void flush(Duration timeout) throws TimeoutException, InterruptedException {

        if (!isConnected()) {
            throw new IllegalStateException("Not Connected");
        }

        Future<Boolean> waitForIt = sendPing();

        if (timeout == null) {
            timeout = Duration.ZERO;
        }

        try {
            long millis = timeout.toMillis();

            if (millis > 0) {
                waitForIt.get(millis, TimeUnit.MILLISECONDS);
            } else {
                waitForIt.get();
            }

            this.statistics.incrementFlushCounter();
        } catch (ExecutionException e) {
            throw new TimeoutException(e.getMessage());
        }

        if (!waitForIt.isDone()) {
            waitForIt.cancel(true);
            throw new TimeoutException("Flush did not return in time");
        }
    }

    void sendConnect() {
        NatsServerInfo info = this.serverInfo.get();
        StringBuilder connectString = new StringBuilder();
        connectString.append(NatsConnection.OP_CONNECT);
        connectString.append(" ");
        String connectOptions = this.options.buildProtocolConnectOptionsString(info.isAuthRequired());
        connectString.append(connectOptions);
        NatsMessage msg = new NatsMessage(connectString.toString());
        this.writer.queue(msg);
    }

    // Send a ping request and push a pong future on the queue.
    // futures are completed in order, keep this one if a thread wants to wait
    // for a specific pong. Note, if no pong returns the wait will not return
    // without setting a timeout.
    CompletableFuture<Boolean> sendPing() {
        CompletableFuture<Boolean> pongFuture = new CompletableFuture<>();
        NatsMessage msg = new NatsMessage(NatsConnection.OP_PING);
        pongQueue.add(pongFuture);
        this.writer.queue(msg);
        return pongFuture;
    }

    void sendPong() {
        NatsMessage msg = new NatsMessage(NatsConnection.OP_PONG);
        this.writer.queue(msg);
    }

    // Called by the reader
    void handlePong() {
        CompletableFuture<Boolean> pongFuture = pongQueue.pollFirst();
        if (pongFuture != null) {
            pongFuture.complete(Boolean.TRUE);
        }
    }

    void readInitialInfo() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(NatsConnection.MAX_PROTOCOL_LINE);
        ByteBuffer protocolBuffer = ByteBuffer.allocate(NatsConnection.MAX_PROTOCOL_LINE);
        boolean gotCRLF = false;
        boolean gotCR = false;
        int read = 0;

        while (!gotCRLF) {
            read = this.dataPort.read(readBuffer);

            if (read < 0) {
                break;
            }

            readBuffer.flip();

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
                    protocolBuffer.put(b);
                }
            }

            readBuffer.clear();

            if (gotCRLF) {
                break;
            }
        }

        if (!gotCRLF) {
            throw new IOException("Failed to read initial info message.");
        }

        protocolBuffer.flip();

        String infoJson = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
        infoJson = infoJson.trim();
        String msg[] = this.reader.space.split(infoJson);
        String op = msg[0].toUpperCase();

        if (!OP_INFO.equals(op)) {
            throw new IOException("Received non-info initial message.");
        }

        NatsServerInfo newServerInfo = new NatsServerInfo(infoJson);
        this.serverInfo.set(newServerInfo);
    }

    void handleInfo(String infoJson) {
        NatsServerInfo serverInfo = new NatsServerInfo(infoJson);
        this.serverInfo.set(serverInfo);
    }

    void deliverMessage(NatsMessage msg) {
        NatsSubscription sub = null;

        this.statistics.incrementInMsgs();
        this.statistics.incrementInBytes(msg.getSize());

        subscriberLock.lock();
        try {
            sub = subscribers.get(msg.getSID());
        } finally {
            subscriberLock.unlock();
        }

        if (sub != null) {
            msg.setSubscription(sub);

            NatsDispatcher d = sub.getDispatcher();
            MessageQueue q = ((d == null) ? sub.getMessageQueue() : d.getMessageQueue());

            if (q != null) {
                q.push(msg);
            }
        } else {
            // Drop messages we don't have a subscriber for (could be extras on an
            // auto-unsub for example)
        }
    }

    NatsServerInfo getInfo() {
        return this.serverInfo.get();
    }

    Options getOptions() {
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

    public Status getStatus() {
        return this.status;
    }

    void updateStatus(Status newStatus) {
        statusLock.lock();
        try {
            this.status = newStatus;
        } finally {
            statusLock.unlock();
        }
    }

    boolean transitionState(State from, State to) {
        boolean retVal = false;
        statusLock.lock();
        try {
            retVal = this.state == from;

            if (retVal) {
                this.state = to;
            }
        } finally {
            statusLock.unlock();
        }
        return retVal;
    }

    boolean isClosingOrClosed() {
        return (this.state == State.CLOSING) || (this.state == State.CLOSED) || (this.state == State.CLOSING_SOCKET);
    }

    boolean isClosed() {
        return this.state == State.CLOSED;
    }

    boolean isConnected() {
        return (this.state == State.CONNECTED);
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
        ArrayList<String> servers = new ArrayList<String>();

        options.getServers().stream().forEach(x -> servers.add(x.toString()));

        if (info != null && info.getConnectURLs()!=null) {
            servers.addAll(Arrays.asList(info.getConnectURLs()));
        }

        return servers;
    }

    public void setErrorHandler(ErrorHandler eh) {
        this.errorHandler = eh;
    }

    public void setConnectionHandler(ConnectionHandler ch) {
        this.connectionHandler = ch;
    }

    public String getCurrentServerURI() {
        return this.currentServerURI;
    }

    boolean waitForState(State state, Duration timeout) {
        long currentWaitNanos = (timeout != null) ? timeout.toNanos() : -1;
        long start = System.nanoTime();

        while (currentWaitNanos > 0 && this.state != state) {
            try {
                Thread.sleep(0, 100); // Short sleep
            } catch (Exception exp) {
                // ignore, try to loop again
            }
            long now = System.nanoTime();
            currentWaitNanos = currentWaitNanos - (now-start);
            start = now;
        }

        return this.state == state;
    }

    void waitForReconnectTimeout() {
        Duration waitTime = options.getReconnectWait();
        long currentWaitNanos = (waitTime != null) ? waitTime.toNanos() : -1;
        long start = System.nanoTime();

        while (currentWaitNanos > 0 && !isClosingOrClosed() &&
                    !isConnected() && !this.reconnectWaiter.isDone()) {
            try {
                this.reconnectWaiter.get(currentWaitNanos, TimeUnit.NANOSECONDS);
            } catch (Exception exp) {
                // ignore, try to loop again
            }
            long now = System.nanoTime();
            currentWaitNanos = currentWaitNanos - (now-start);
            start = now;
        }

        this.reconnectWaiter.complete(Boolean.TRUE);
    }

    Collection<String> buildReconnectList() {
        ArrayList<String> reconnectList = new ArrayList<>();

        reconnectList.addAll(getServers());

        if (options.isNoRandomize()) {
            return reconnectList;
        }

        Collections.shuffle(reconnectList);

        return reconnectList;
    }
}