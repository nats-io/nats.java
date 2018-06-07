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
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Options;
import io.nats.client.Subscription;

class NatsConnection implements Connection {
    private Options options;
    private Connection.Status status = Status.DISCONNECTED;

    private Socket socket;
    private NatsConnectionReader reader;

    private NatsServerInfo serverInfo;

    NatsConnection(Options options) {
        this.options = options;
        this.reader = new NatsConnectionReader(this);
    }

    void connect() throws IOException {
        if (options.getServers().size() == 0) {
            throw new IllegalArgumentException("No servers provided in options");
        }

        // TODO(sasbury): Use getServers on the connection, it may have more choices
        for (URI serverURI : options.getServers()) {
            tryToConnect(serverURI, true);

            if (status == Status.CONNECTED) {
                break;
            }
        }
    }

    void tryToConnect(URI serverURI, boolean firstTime) throws IOException {
       
        this.reader.getLock().lock();
        try {
            this.socket = new Socket();

            if (firstTime) {
                this.status = Status.CONNECTING;
            } else {
                this.status = Status.RECONNECTING;
            }

            // TODO(sasbury): Bind to local address

            this.socket.connect(new InetSocketAddress(serverURI.getHost(), serverURI.getPort()),
                                                         (int) options.getConnectionTimeout().toMillis());
        } finally {
            this.reader.getCondition().signalAll();
            this.reader.getLock().unlock();
        }
            
        
        // TODO(sasbury): Need to wait for info/connect messages and set connected status when we connect successfully
        throw new UnsupportedOperationException("Not implemented yet");

    }

    void readWriteIOException(IOException io) {
        this.status = Status.DISCONNECTED;

        try {
            this.socket.close();
        } catch (IOException exp) {
            this.socket = null;
            // Don't reset server info, we use it for the server list
            // It will be replaced when we connect to a new server
        }

        // TODO(sasbury): reconnect
    }

    Socket getSocket() {
        return this.socket;
    }

    Options getOptions() {
        return this.options;
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

    public void close() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Status getStatus() {
        return this.status;
    }

    public long getMaxPayload() {
        if (this.serverInfo == null) {
            return -1;
        }

        return this.serverInfo.getMaxPayload();
    }

    public Collection<String> getServers() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    void setServerInfo(NatsServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }
}