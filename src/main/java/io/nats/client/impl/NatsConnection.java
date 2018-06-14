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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;
import io.nats.client.Options;
import io.nats.client.Statistics;
import io.nats.client.Subscription;

// TODO(sasbury): connection and reconnect notifcations
// TODO(sasbury): Throw exception on publish, etc. if not connected
class NatsConnection implements Connection {
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

    private Connection.Status status;
    private ReentrantLock statusLock;

    private CompletableFuture<SocketChannel> channelFuture;
    private SocketChannel socketChannel;

    private NatsConnectionReader reader;
    private NatsConnectionWriter writer;

    private AtomicReference<NatsServerInfo> serverInfo;
    private CompletableFuture<NatsServerInfo> serverInfoFuture;

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

    NatsConnection(Options options) {
        this.options = options;

        this.statistics = new NatsStatistics();

        this.status = Status.DISCONNECTED;
        this.statusLock = new ReentrantLock();

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

    void connect() throws InterruptedException {
        if (options.getServers().size() == 0) {
            throw new IllegalArgumentException("No servers provided in options");
        }

        for (String serverURI : getServers()) {
            tryConnect(serverURI, true);
            if (status == Status.CONNECTED) {
                break;
            }
        }
    }

    void reconnect() {
        // TODO(sasbury): resend any subscriptions
        // TODO(sasbury): What do we do with the reader while we try to reconnect?
        // TODO(sasbury): Limit on publish buffer during reconnect
        // TOOD(sasbury): Deal with dispatchers (esp if we can't reconnect, then they
        // need to be closed)
        // TODO(sasbury): LOTS OF TESTS during reconnect scenarios
        this.close(); // for now to clean up
        throw new UnsupportedOperationException("Not implemented yet");
    }

    void tryConnect(String serverURI, boolean firstTime) throws InterruptedException {
        try {
            Duration connectTimeout = options.getConnectionTimeout();

            statusLock.lock();
            if (firstTime) {
                this.status = Status.CONNECTING;
            } else {
                this.status = Status.RECONNECTING;
            }
            statusLock.unlock();

            // Wait for the reader to be ready, it is ready at creation and after stopping
            this.reader.stop().get();
            this.writer.stop().get();

            // Create a new future for the SocketChannel, the reader/writer will use this
            // to wait for the connect/failure.
            this.channelFuture = new CompletableFuture<>();
            this.serverInfoFuture = new CompletableFuture<>();

            this.cleanUpPongQueue();

            // Start the reader, after we know it is stopped
            this.reader.start(this.channelFuture);
            this.writer.start(this.channelFuture);

            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(true);

            // TODO(sasbury): Bind to local address, set other options

            URI uri = new URI(serverURI);
            channel.socket().connect(new InetSocketAddress(uri.getHost(), uri.getPort()),
                    (int) options.getConnectionTimeout().toMillis());
            channel.finishConnect();

            // Notify the any threads waiting on the sockets
            this.socketChannel = channel;
            this.channelFuture.complete(this.socketChannel);

            // Wait for the INFO message
            // Reader will cancel this if told to reconnect due to an io exception
            NatsServerInfo info = this.serverInfoFuture.get();
            this.serverInfo.set(info);

            this.sendConnect();
            Future<Boolean> pongFuture = sendPing();
            pongFuture.get(connectTimeout.toMillis(), TimeUnit.MILLISECONDS);

            // Set connected status
            statusLock.lock();
            this.status = Status.CONNECTED;
            statusLock.unlock();

        } catch (IOException | CancellationException | TimeoutException | ExecutionException | URISyntaxException ex) {
            handleCommunicationIssue(ex);
        }
    }

    // Can be called from reader/writer thread, or inside the connect code
    void handleCommunicationIssue(Exception io) {
        boolean wasConnected = this.closeSocket(false);

        // Try to reconnect in a new thread
        // This should only be true if the read/write thread call this method
        // The connect thread will call this method but only before the status is
        // set to connected, so connect won't force a reconnect
        if (wasConnected) {
            Thread t = new Thread(() -> {
                this.reconnect();
            });
            t.start();
        }
    }

    public void close() {
        closeSocket(true);

        // Reader and writer are stopped or stopping, wait for them to finish
        try {
            this.reader.stop().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            // Ignore these, issue with future so move on
        }
        try {
            this.writer.stop().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            // Ignore these, issue with future so move on
        }

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

        statusLock.lock();
        this.status = Status.CLOSED;
        statusLock.unlock();
    }

    boolean closeSocket(boolean closing) {
        boolean wasConnected = false;
        boolean wasDisconnected = false;

        statusLock.lock();
        wasConnected = !closing && (this.status == Status.CONNECTED);
        wasDisconnected = (this.status == Status.DISCONNECTED);
        this.status = Status.DISCONNECTED;
        statusLock.unlock();

        if (wasDisconnected) {
            return wasConnected;
        }

        this.serverInfoFuture.cancel(true); // Stop the connect thread from waiting for this

        this.reader.stop();
        this.writer.stop();

        // Close the current socket and cancel anyone waiting for it
        this.channelFuture.cancel(true);

        cleanUpPongQueue();

        try {
            if (this.socketChannel != null) {
                this.socketChannel.close();
            }
        } catch (IOException ex) {
            // Issue closing the socket, but we will just move on
        }

        return wasConnected;
    }

    void cleanUpPongQueue() {
        Future<Boolean> b;
        while ((b = pongQueue.poll()) != null) {
            b.cancel(true);
        }
        pongQueue.clear();
    }

    public void publish(String subject, byte[] body) {
        this.publish(subject, null, body);
    }

    public void publish(String subject, String replyTo, byte[] body) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in publish");
        }

        if (replyTo != null && replyTo.length() == 0) {
            throw new IllegalArgumentException("ReplyTo cannot be the empty string");
        }

        if (body == null) {
            body = EMPTY_BODY;
        }

        NatsMessage msg = new NatsMessage(subject, replyTo, body);
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
        if (after <= 0) {
            this.invalidate(sub); // Will clean it up
        } else {
            sub.setMax(after);

            if (sub.reachedMax()) {
                sub.invalidate();
            }
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
        NatsSubscription sub = null;
        subscriberLock.lock();
        try {
            String sid = String.valueOf(this.nextSid);
            sub = new NatsSubscription(sid, subject, queueName, this, dispatcher);
            nextSid++;
            subscribers.put(sid, sub);

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
        } finally {
            subscriberLock.unlock();
        }
        return sub;
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

    public Future<Message> request(String subject, byte[] data) {
        String responseInbox = null;
        boolean oldStyle = options.isOldRequestStyle();

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
        
        publish(subject, responseInbox, data);
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

    void handleInfo(String infoJson) {
        NatsServerInfo serverInfo = new NatsServerInfo(infoJson);
        // More than one from the same server
        if (this.serverInfoFuture.isDone()) {
            this.serverInfo.set(serverInfo);
        } else {
            this.serverInfoFuture.complete(serverInfo); // Will set in connect thread
        }
    }

    void deliverMessage(NatsMessage msg) {
        NatsSubscription sub = null;

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
        return statistics;
    }

    NatsStatistics getNatsStatistics() {
        return statistics;
    }

    public Status getStatus() {
        return this.status;
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

        if (info != null) {
            servers.addAll(Arrays.asList(info.getConnectURLs()));
        }

        return servers;
    }
}