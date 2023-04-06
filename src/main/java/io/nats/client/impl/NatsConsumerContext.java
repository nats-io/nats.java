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

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.Validator;

import java.io.IOException;

/**
 * TODO
 */
public class NatsConsumerContext extends NatsStreamContext implements ConsumerContext {

    private final NatsJetStream js;
    private final ConsumerConfiguration userCc;
    private String consumer;

    NatsConsumerContext(NatsStreamContext streamContext, String consumerName, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        super(streamContext);
        js = new NatsJetStream(jsm.conn, jsm.jso);
        if (consumerName != null) {
            consumer = consumerName;
            userCc = null;
            jsm.getConsumerInfo(stream, consumer);
        }
        else {
            userCc = cc;
        }
    }

    private NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String streamName,
                                String consumerName, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        this(new NatsStreamContext(connection, jsOptions, streamName), consumerName, cc);
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, String consumerName) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, Validator.required(consumerName, "Consumer Name"), null);
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, ConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, null, Validator.required(consumerConfiguration, "Consumer Configuration"));
    }

    public String getName() {
        return consumer;
    }

    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(stream, consumer);
    }

    private NatsJetStreamPullSubscription makeSubscription() throws IOException, JetStreamApiException {
        PullSubscribeOptions pso;
        if (consumer == null) {
            pso = ConsumerConfiguration.builder(userCc).buildPullSubscribeOptions(stream);
        }
        else {
            pso = PullSubscribeOptions.bind(stream, consumer);
        }
        return (NatsJetStreamPullSubscription)js.subscribe(null, pso);
    }

    /* inner */ class NatsFetchConsumer extends NatsMessageConsumer implements FetchConsumer {
        public NatsFetchConsumer(FetchConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
            super(consumeOptions);
            setSub(makeSubscription());
            sub.pull(PullRequestOptions.builder(consumeOptions.getMaxMessages())
                .maxBytes(consumeOptions.getMaxBytes())
                .expiresIn(consumeOptions.getExpires())
                .idleHeartbeat(consumeOptions.getIdleHeartbeat())
                .build()
            );
        }

        @Override
        public Message nextMessage() throws InterruptedException {
            Message m;
            if (pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1)) {
                m = sub.nextMessage(null); // null means don't wait, the queue either has something already or it doesn't
            }
            else {
                m = sub.nextMessage(consumeOptions.getExpires());
            }
            if (m == null) {
                // todo unsubscribe on a different thread so can return right away;
            }
            return m;
        }
    }

    @Override
    public FetchConsumer fetch(int maxMessages) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxMessages(maxMessages).build());
    }

    @Override
    public FetchConsumer fetch(int maxBytes, int maxMessages) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxBytes(maxBytes, maxMessages).build());
    }

    @Override
    public FetchConsumer fetch(FetchConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(consumeOptions, "Consume Options");
        return new NatsFetchConsumer(consumeOptions);
    }

    @Override
    public MessageConsumer consume() throws IOException, JetStreamApiException {
        return consume((ConsumeOptions)null);
    }

    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return null;
    }

    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return null;
    }


    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return null;
    }
}
