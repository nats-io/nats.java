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
import java.util.concurrent.atomic.AtomicBoolean;

public class NatsKeyValueWatchSubscription {
    private static final Object dispatcherLock = new Object();
    private static NatsDispatcher dispatcher;

    private final JetStreamSubscription sub;
    private final AtomicBoolean endOfDataSent;

    public NatsKeyValueWatchSubscription(NatsKeyValue kv, String bucketName, String keyPattern,
                                         final KeyValueWatcher watcher,
                                         KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException {
        String stream = NatsKeyValueUtil.streamName(bucketName);
        String keySubject = NatsKeyValueUtil.keySubject(kv.js.jso, bucketName, keyPattern);

        // figure out the result options
        boolean headersOnly = false;
        boolean ignoreDeletes = false;
        DeliverPolicy deliverPolicy = DeliverPolicy.LastPerSubject;
        for (KeyValueWatchOption wo : watchOptions) {
            if (wo != null) {
                switch (wo) {
                    case META_ONLY: headersOnly = true; break;
                    case IGNORE_DELETE: ignoreDeletes = true; break;
                    case UPDATES_ONLY: deliverPolicy = DeliverPolicy.New; break;
                    case INCLUDE_HISTORY: deliverPolicy = DeliverPolicy.All; break;
                }
            }
        }

        if (deliverPolicy == DeliverPolicy.New) {
            watcher.endOfData();
            endOfDataSent = new AtomicBoolean(true);
        }
        else {
            KeyValueEntry kveCheckPending = kv.getInternal(keyPattern);
            if (kveCheckPending == null) {
                watcher.endOfData();
                endOfDataSent = new AtomicBoolean(true);
            }
            else {
                endOfDataSent = new AtomicBoolean(false);
            }
        }

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(stream)
            .ordered(true)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None)
                    .deliverPolicy(deliverPolicy)
                    .headersOnly(headersOnly)
                    .filterSubject(keySubject)
                    .build())
            .build();

        final boolean includeDeletes = !ignoreDeletes;
        MessageHandler handler = m -> {
            KeyValueEntry kve = new KeyValueEntry(m);
            if (includeDeletes || kve.getOperation().equals(KeyValueOperation.PUT)) {
                watcher.watch(kve);
            }
            if (!endOfDataSent.get() && kve.getDelta() == 0) {
                watcher.endOfData();
                endOfDataSent.set(true);
            }
        };

        sub = kv.js.subscribe(keySubject, getDispatcher(kv.js), handler, false, pso);
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
