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

import java.time.Duration;
import java.util.Collection;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Options;
import io.nats.client.Subscription;

class TCPConnection implements Connection {
    private Options options;

    TCPConnection(Options options) {
        this.options = options;
    }

    void connect() {
        throw new UnsupportedOperationException("Not implemented yet");
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
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public long getMaxPayload() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Collection<String> getServers() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}