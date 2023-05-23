// Copyright 2020-2023 The NATS Authors
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

import io.nats.client.ConsumerContext;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamOptions;
import io.nats.client.StreamContext;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamInfoOptions;

import java.io.IOException;

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
class NatsStreamContext implements StreamContext {
    final NatsJetStreamManagement jsm;
    final String streamName;

    NatsStreamContext(NatsConnection connection, JetStreamOptions jsOptions, String streamName) throws IOException, JetStreamApiException {
        jsm = new NatsJetStreamManagement(connection, jsOptions);
        this.streamName = streamName;
        jsm.getStreamInfo(streamName); // this is just verifying that the stream exists
    }

    NatsStreamContext(NatsStreamContext streamContext) {
        jsm = streamContext.jsm;
        streamName = streamContext.streamName;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public StreamInfo getStreamInfo() throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName);
    }

    @Override
    public StreamInfo getStreamInfo(StreamInfoOptions options) throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName, options);
    }

    @Override
    public ConsumerInfo createConsumer(ConsumerConfiguration config) throws IOException, JetStreamApiException {
        return jsm.addOrUpdateConsumer(streamName, config);
    }

    @Override
    public boolean deleteConsumer(String consumerName) throws IOException, JetStreamApiException {
        return jsm.deleteConsumer(streamName, consumerName);
    }

    @Override
    public ConsumerContext getConsumerContext(String consumerName) throws IOException, JetStreamApiException {
        return new NatsConsumerContext(jsm.conn, jsm.jso, streamName, consumerName);
    }
}
