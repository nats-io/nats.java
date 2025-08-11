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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.*;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NatsKeyValueWatchSubscription extends NatsWatchSubscription<KeyValueEntry> {

    public NatsKeyValueWatchSubscription(NatsKeyValue kv, String keyPattern, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException {
        this(kv, Collections.singletonList(keyPattern), watcher, fromRevision, watchOptions);
    }

    public NatsKeyValueWatchSubscription(NatsKeyValue kv, List<String> keyPatterns, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException {
        super(kv.js);
        kvWatchInit(kv, keyPatterns, watcher, fromRevision, watchOptions);
    }

    private void kvWatchInit(NatsKeyValue kv, List<String> keyPatterns, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption[] watchOptions) throws IOException, JetStreamApiException {
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

        // convert each key to a read subject
        List<String> readSubjects = new ArrayList<>();
        for (String keyPattern : keyPatterns) {
            readSubjects.add(kv.readSubject(keyPattern.trim()));
        }

        finishInit(kv,
            readSubjects,
            deliverPolicy,
            headersOnly,
            fromRevision,
            getHandler(watcher, !ignoreDeletes),
            watcher.getConsumerNamePrefix());
    }

    private static @NonNull WatchMessageHandler<KeyValueEntry> getHandler(KeyValueWatcher watcher, boolean includeDeletes) {
        return new WatchMessageHandler<KeyValueEntry>(watcher) {
            @Override
            public void onMessage(Message m) throws InterruptedException {
                KeyValueEntry kve = new KeyValueEntry(m);
                if (includeDeletes || kve.getOperation() == KeyValueOperation.PUT) {
                    watcher.watch(kve);
                }
                if (!endOfDataSent && kve.getDelta() == 0) {
                    sendEndOfData();
                }
            }
        };
    }
}
