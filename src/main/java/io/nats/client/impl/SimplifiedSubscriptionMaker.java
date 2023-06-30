// Copyright 2023 The NATS Authors
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

import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;

import java.io.IOException;

public class SimplifiedSubscriptionMaker {
    private final NatsJetStream js;
    private final PullSubscribeOptions pso;
    private final String subscribeSubject;
    @SuppressWarnings("FieldCanBeLocal") // can't be local because I want to keep its reference as part of the state
    private Dispatcher dispatcher;

    public SimplifiedSubscriptionMaker(NatsJetStream js, PullSubscribeOptions pso) {
        this.js = js;
        this.pso = pso;
        this.subscribeSubject = null;
    }

    public SimplifiedSubscriptionMaker(NatsJetStream js, PullSubscribeOptions pso, String subscribeSubject) {
        this.js = js;
        this.pso = pso;
        this.subscribeSubject = subscribeSubject;
    }

    public NatsJetStreamPullSubscription makeSubscription(MessageHandler messageHandler) throws IOException, JetStreamApiException {
        if (messageHandler == null) {
            return (NatsJetStreamPullSubscription)js.subscribe(subscribeSubject, pso);
        }

        dispatcher = js.conn.createDispatcher();
        return (NatsJetStreamPullSubscription)js.subscribe(subscribeSubject, dispatcher, messageHandler, pso);
    }
}
