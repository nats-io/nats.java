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

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConnectableConsumer {
    protected final String id;
    protected final Connection nc;
    protected final JetStream js;
    protected final UiConnectionListener connectionListener;
    protected final UiErrorListener errorListener;
    protected final AtomicLong lastReceivedSequence;
    protected final MessageHandler handler;
    protected final List<String> consumerNames;
    protected String durableName;

    public ConnectableConsumer(String id, boolean durable) throws IOException, InterruptedException, JetStreamApiException {
        this.id = id;
        connectionListener = new UiConnectionListener(id);
        errorListener = new UiErrorListener(id);

        Options options = new Options.Builder()
            .server(App.BOOTSTRAP)
            .connectionListener(connectionListener)
            .errorListener(errorListener)
            .maxReconnects(-1)
            .build();

        nc = Nats.connect(options);
        js = nc.jetStream();
        lastReceivedSequence = new AtomicLong(0);
        durableName = durable ? "dur-" + NUID.nextGlobalSequence() : null;
        consumerNames = new ArrayList<>();

        handler = m -> {
            m.ack();
            long seq = m.metaData().streamSequence();
            lastReceivedSequence.set(seq);
            Ui.workMessage(this.id, "Received: SEQ: " + seq);
        };
    }

    public long getLastReceivedSequence() {
        return lastReceivedSequence.get();
    }

    protected ConsumerConfiguration.Builder startCreateConsumer() {
        long last = lastReceivedSequence.get();
        return ConsumerConfiguration.builder()
            .durable(durableName)
            .deliverPolicy(last == 0 ? DeliverPolicy.All : DeliverPolicy.ByStartSequence)
            .startSequence(last == 0 ? -1 : last + 1)
            .filterSubject(App.SUBJECT);
    }
}
