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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.Watcher;

import java.io.IOException;
import java.util.List;

import static io.nats.client.api.ConsumerConfiguration.ULONG_UNSET;

public class NatsWatchSubscription<T> implements AutoCloseable {
    private final JetStream js;
    private NatsDispatcher dispatcher;
    private JetStreamSubscription sub;

    public NatsWatchSubscription(JetStream js) {
        this.js = js;
    }

    protected void finishInit(NatsFeatureBase fb,
                              List<String> subscribeSubjects,
                              DeliverPolicy deliverPolicy,
                              boolean headersOnly,
                              long fromRevision,
                              WatchMessageHandler<T> handler,
                              String consumerNamePrefix)
        throws IOException, JetStreamApiException
    {
        if (fromRevision > ULONG_UNSET) {
            deliverPolicy = DeliverPolicy.ByStartSequence;
        }
        else {
            fromRevision = ULONG_UNSET; // easier on the builder since we aren't starting at a fromRevision
            if (deliverPolicy == DeliverPolicy.New) {
                handler.sendEndOfData();
            }
        }

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(fb.getStreamName())
            .ordered(true)
            .configuration(ConsumerConfiguration.builder()
                .name(consumerNamePrefix)
                .ackPolicy(AckPolicy.None)
                .deliverPolicy(deliverPolicy)
                .startSequence(fromRevision)
                .headersOnly(headersOnly)
                .filterSubjects(subscribeSubjects)
                .build())
            .build();

        dispatcher = (NatsDispatcher) ((NatsJetStream) js).conn.createDispatcher();
        sub = js.subscribe(null, dispatcher, handler, false, pso);
        if (!handler.endOfDataSent) {
            long pending = sub.getConsumerInfo().getCalculatedPending();
            if (pending == 0) {
                handler.sendEndOfData();
            }
        }
    }

    protected static abstract class WatchMessageHandler<T> implements MessageHandler {
        private final Watcher<T> watcher;
        boolean endOfDataSent;

        protected WatchMessageHandler(Watcher<T> watcher) {
            this.watcher = watcher;
        }

        public void sendEndOfData() {
            endOfDataSent = true;
            watcher.endOfData();
        }
    }

    public void unsubscribe() {
        if (dispatcher != null) {
            dispatcher.unsubscribe(sub);
            if (dispatcher.hasNoSubs()) {
                dispatcher.connection.closeDispatcher(dispatcher);
                dispatcher = null;
            }
        }
    }

    @Override
    public void close() throws Exception {
        unsubscribe();
    }
}
