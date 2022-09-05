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

public class NatsWatchSubscription<T> implements AutoCloseable {
    private static final Object dispatcherLock = new Object();
    private static NatsDispatcher dispatcher;

    private final JetStream js;
    private JetStreamSubscription sub;

    public NatsWatchSubscription(JetStream js) {
        this.js = js;
    }

    protected void finishInit(NatsFeatureBase fb, String subscribeSubject, DeliverPolicy deliverPolicy, boolean headersOnly, WatchMessageHandler<T> handler)
        throws IOException, JetStreamApiException
    {
        if (deliverPolicy == DeliverPolicy.New
            || fb._getLast(subscribeSubject) == null)
        {
            handler.sendEndOfData();
        }

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(fb.getStreamName())
            .ordered(true)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None)
                    .deliverPolicy(deliverPolicy)
                    .headersOnly(headersOnly)
                    .filterSubject(subscribeSubject)
                    .build())
            .build();

        sub = js.subscribe(subscribeSubject, getDispatcher(js), handler, false, pso);
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

    private static Dispatcher getDispatcher(JetStream js) {
        synchronized (dispatcherLock) {
            if (dispatcher == null) {
                dispatcher = (NatsDispatcher) ((NatsJetStream) js).conn.createDispatcher();
            }
            return dispatcher;
        }
    }

    public void unsubscribe() {
        synchronized (dispatcherLock) {
            dispatcher.unsubscribe(sub);
            if (dispatcher.getSubscriptionHandlers().size() == 0) {
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
