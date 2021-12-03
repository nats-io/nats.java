// Copyright 2021 The NATS Authors
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
import io.nats.client.api.*;
import io.nats.client.support.NatsKeyValueUtil;

import java.io.IOException;

public class NatsKeyValueWatchSubscription {
    private static final Object dispatcherLock = new Object();
    private static NatsDispatcher dispatcher;

    private final JetStreamSubscription sub;

    public NatsKeyValueWatchSubscription(JetStream js, String bucketName, String keyPattern,
                                         boolean headersOnly, final KeyValueWatcher watcher, KeyValueOperation[] operations) throws IOException, JetStreamApiException {
        String stream = NatsKeyValueUtil.streamName(bucketName);
        String subject = NatsKeyValueUtil.keySubject(bucketName, keyPattern);

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(stream)
            .ordered(true)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None)
                    .deliverPolicy(DeliverPolicy.LastPerSubject)
                    .headersOnly(headersOnly)
                    .filterSubject(subject)
                    .build())
            .build();

        MessageHandler handler;
        if (operations.length == 0) {
            handler = m -> watcher.watch(new KeyValueEntry(m));
        }
        else if (operations.length == 1) {
            final KeyValueOperation op = operations[0];
            handler = m -> {
                KeyValueEntry kve = new KeyValueEntry(m);
                if (kve.getOperation() == op) {
                    watcher.watch(kve);
                }
            };
        }
        else {
            handler = m -> {
                KeyValueEntry kve = new KeyValueEntry(m);
                for (KeyValueOperation op : operations) {
                    if (kve.getOperation() == op) {
                        watcher.watch(kve);
                        return;
                    }
                }
            };
        }

        sub = js.subscribe(subject, getDispatcher(js), handler, false, pso);
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
}
