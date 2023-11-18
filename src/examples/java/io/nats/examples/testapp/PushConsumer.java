// Copyright 2020 The NATS Authors
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

package io.nats.examples.testapp;

import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PushSubscribeOptions;

import java.io.IOException;

public class PushConsumer extends ConnectableConsumer {
    static final String ID = "PushConsumer";

    final Dispatcher d;
    final JetStreamSubscription sub;

    public PushConsumer(boolean durable) throws IOException, InterruptedException, JetStreamApiException {
        super(ID, durable);

        d = nc.createDispatcher();

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(App.STREAM)
            .configuration(startCreateConsumer().idleHeartbeat(1000).build())
            .build();

        sub = js.subscribe(App.SUBJECT, d, handler, false, pso);
        Ui.controlMessage(ID, "ConsumerInfo", sub.getConsumerInfo().getJv());
    }
}
