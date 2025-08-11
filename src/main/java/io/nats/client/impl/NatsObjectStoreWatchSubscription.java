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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ObjectInfo;
import io.nats.client.api.ObjectStoreWatchOption;
import io.nats.client.api.ObjectStoreWatcher;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.Collections;

import static io.nats.client.api.ConsumerConfiguration.ULONG_UNSET;

public class NatsObjectStoreWatchSubscription extends NatsWatchSubscription<ObjectInfo> {

    public NatsObjectStoreWatchSubscription(NatsObjectStore os, ObjectStoreWatcher watcher, ObjectStoreWatchOption... watchOptions) throws IOException, JetStreamApiException {
        super(os.js);

        // figure out the result options
        boolean headersOnly = false;
        boolean ignoreDeletes = false;
        DeliverPolicy deliverPolicy = DeliverPolicy.LastPerSubject;
        for (ObjectStoreWatchOption wo : watchOptions) {
            if (wo != null) {
                switch (wo) {
                    case IGNORE_DELETE: ignoreDeletes = true; break;
                    case UPDATES_ONLY: deliverPolicy = DeliverPolicy.New; break;
                    case INCLUDE_HISTORY: deliverPolicy = DeliverPolicy.All; break;
                }
            }
        }

        finishInit(os,
            Collections.singletonList(os.rawAllMetaSubject()),
            deliverPolicy,
            headersOnly,
            ULONG_UNSET,
            getHandler(watcher, !ignoreDeletes),
            watcher.getConsumerNamePrefix());
    }

    private static @NonNull WatchMessageHandler<ObjectInfo> getHandler(ObjectStoreWatcher watcher, boolean includeDeletes) {
        return new WatchMessageHandler<ObjectInfo>(watcher) {
            @Override
            public void onMessage(Message m) throws InterruptedException {
                ObjectInfo os = new ObjectInfo(m);
                if (includeDeletes || !os.isDeleted()) {
                    watcher.watch(os);
                }
                if (!endOfDataSent && m.metaData().pendingCount() == 0) {
                    sendEndOfData();
                }
            }
        };
    }
}
