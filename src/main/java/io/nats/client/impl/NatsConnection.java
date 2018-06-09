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
import io.nats.client.Options;
import io.nats.client.Subscription;

// TODO(sasbury): connection and reconnect notifcations
class NatsConnection implements Connection {
    static final int MAX_PROTOCOL_LINE = 1024;
    static final int BUFFER_SIZE = 4 * 1024;

    static final byte CR = 0x0D;
    static final byte LF = 0x0A;

    static final String OP_CONNECT = "CONNECT";
    static final String OP_INFO = "INFO";
    static final String OP_MSG = "MSG";
    static final String OP_PING = "PING";
    static final String OP_PONG = "PONG";
    static final String OP_OK = "+OK";
    static final String OP_ERR = "-ERR";

    private Options options;

    private Connection.Status status;
    private ReentrantLock statusLock;

    private CompletableFuture<SocketChannel> channelFuture;
    private SocketChannel socketChannel;

    private NatsConnectionReader reader;
    private NatsConnectionWriter writer;

    private AtomicReference<NatsServerInfo> serverInfo;
    private CompletableFuture<NatsServerInfo> serverInfoFuture;

    private ConcurrentLinkedDeque<CompletableFuture<Boolean>> pongQueue;

    NatsConnection(Options options) {
        this.options = options;
        
        this.status = Status.DISCONNECTED;
        this.statusLock = new ReentrantLock();

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

        } catch (IOException|CancellationException|TimeoutException|ExecutionException|URISyntaxException ex) {
            handleCommunicationIssue(ex);
        }
    }

    // Can be called from reader/writer thread, or inside the connect code
    void handleCommunicationIssue(Exception io) {
        boolean wasConnected = this.closeSocket();
        
        // Try to reconnect in a new thread
        // This should only be true if the read/write thread call this method
        // The connect thread will call this method but only before the status is
        // set to connected, so connect won't force a reconnect
        if (wasConnected) {
            Thread t = new Thread(() -> {this.reconnect();});
            t.start();
        }
    }

    public void close() {
        closeSocket();

        // Reader and writer are stopped or stopping, wait for them to finish
        try {
            this.reader.stop().get(30, TimeUnit.SECONDS);
        } catch(Exception ex) {
            // Ignore these, issue with future so move on
        }
        try {
            this.writer.stop().get(30, TimeUnit.SECONDS);
        } catch(Exception ex) {
            // Ignore these, issue with future so move on
        }

        statusLock.lock();
        this.status = Status.CLOSED;
        statusLock.unlock();
    }

    boolean closeSocket() {
        boolean wasConnected = false;
        boolean wasDisconnected = false;

        statusLock.lock();
        wasConnected = (this.status == Status.CONNECTED);
        wasDisconnected = (this.status == Status.DISCONNECTED);
        this.status = Status.DISCONNECTED;
        statusLock.unlock();

        if (wasDisconnected) {
            return wasConnected;
        }

        this.serverInfoFuture.cancel(true); //Stop the connect thread from waiting for this

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
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public void publish(String subject, String replyTo, byte[] body) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Subscription subscribe(String subject) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Subscription subscribe(String subject, String queueName) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Message request(String subject, byte[] data, Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Dispatcher createDispatcher(MessageHandler handler) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public void flush(Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet");
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
        if (pongFuture!=null) {
            pongFuture.complete(Boolean.TRUE);
        }
    }

    void handleInfo(String infoJson) {
        NatsServerInfo serverInfo = new NatsServerInfo(infoJson);
        // More than one from the same server
        if (this.serverInfoFuture.isDone()) {
            this.serverInfo.set(serverInfo);
        } else {
            this.serverInfoFuture.complete(serverInfo);
        }
    }

    Options getOptions() {
        return this.options;
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