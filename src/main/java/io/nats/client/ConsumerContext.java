// Copyright 2022 The NATS Authors
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

package io.nats.client;

import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.SimpleConsumerConfiguration;
import io.nats.client.impl.NatsJetStreamManagement;

import java.io.IOException;

/**
 * THIS IS PART OF AN EXPERIMENTAL API AND IS CONSIDERED EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumerContext {
    private String name;
    private String stream;
    private ConsumerInfo consumerInfo;
    private NatsJetStreamManagement jsm;

    public ConsumerContext(String stream, String name, NatsJetStreamManagement jsm) {
        this.stream = stream;
        this.name = name;
        this.jsm = jsm;
    }

    public ConsumerContext(StreamContext streamContext, String name, NatsJetStreamManagement jsm) {
        this.stream = streamContext.getName();
        this.name = name;
        this.jsm = jsm;
    }

    public String getName() {
        return name;
    }

    public ConsumerInfo getConsumerInfo() throws JetStreamApiException, IOException {
        if (consumerInfo == null) {
            consumerInfo = jsm.getConsumerInfo(stream, name);
        }
        return consumerInfo;
    }
    
    public ConsumerReader read(int messageLimit) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerListener listen(int messageLimit, ConsumerCallback callback) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerReader read() throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerReader read(SimpleConsumerOptions options) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerReader read(SimpleConsumerConfiguration config) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerReader read(SimpleConsumerConfiguration config, SimpleConsumerOptions options) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerListener listen(ConsumerCallback callback) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerListener listen(ConsumerCallback callback, SimpleConsumerOptions options) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerListener listen(SimpleConsumerConfiguration config, ConsumerCallback callback) throws IOException, JetStreamApiException {
        return null;
    }

    public ConsumerListener listen(SimpleConsumerConfiguration config, ConsumerCallback callback, SimpleConsumerOptions options) throws IOException, JetStreamApiException {
        return null;
    }
}
