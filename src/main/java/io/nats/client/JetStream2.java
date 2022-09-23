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
package io.nats.client;

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;

import java.util.List;

public interface JetStream2 {

    // ----------------------------------------------------------------------------------------------------
    // Consumer creation - historically part of the "management" functions.
    // ----------------------------------------------------------------------------------------------------
    // Validations: Maybe just let the server do things? Maybe.
    // * Deliver group is not allowed with an ordered consumer.
    // * Durable is not allowed with an ordered consumer.
    // * Deliver subject is not allowed with an ordered consumer.
    // * Deliver subject is not allowed with an ordered consumer.
    // * Max deliver is limited to 1 with an ordered consumer.
    // ----------------------------------------------------------------------------------------------------
    ConsumerInfo createOrUpdateConsumer(String streamName, ConsumerConfiguration config);

    // ----------------------------------------------------------------------------------------------------
    // Interfaces / classes
    // ----------------------------------------------------------------------------------------------------

    class PullRawOptions {
        int batchSize; // required
        int maxBytes;
        boolean noWait;
        long expiresInMillis;
        long idleHeartbeat;
    }

    enum FetchType {
        Instant, Wait, OneShot, Blah, AlbertoWasWorkingOnOptions
    }

    class FetchOptions {
        FetchType fetchType; // required

        // optional, but would need to know defaults for batchsize
        // or possibly just pull out the PRO fields that make sense
        PullRawOptions pullRawOptions;
    }

    class AsyncOptions {
        Dispatcher dispatcher;  // required
        MessageHandler handler; // required
        boolean autoAck;        // defaults to false
    }

    interface PushSyncSubscription {
        Message NextMessage(long timeoutMillis);
    }

    interface PushAsyncSubscription {}

    interface PullFetchSubscription {
        List<Message> Fetch(long timeoutMillis);
    }

    interface PullAsyncSubscription {}

    interface PullRawSubscription {
        Message NextMessage(long timeoutMillis);
    }

    // ----------------------------------------------------------------------------------------------------
    // Durable Subscribes
    // ----------------------------------------------------------------------------------------------------
    // * consumer must exist, must be able to be looked up, must validate as the proper type
    // * push consumer must have deliver subject
    // * push consumer must NOT have pull settings like Max [Pull] Waiting, Max Batch, Max Bytes
    // * push consumer must have deliver subject
    // * pull consumer must NOT have deliver subject, deliver group, flow control
    // ----------------------------------------------------------------------------------------------------
    JetStreamReader reader(int batchSize, int repullAt);
    PushSyncSubscription pushSyncSubscribe(String stream, String consumerName);
    PushAsyncSubscription pushAsyncSubscribe(String stream, String consumerName, AsyncOptions asyncOptions);
    PullFetchSubscription pullFetchSubscribe(String stream, String consumerName, FetchOptions fetchOptions);
    PullAsyncSubscription pullAsyncSubscribe(String stream, String consumerName, FetchOptions fetchOptions, AsyncOptions asyncOptions);
    PullRawSubscription pullRawSubscribe(String stream, String consumerName, PullRawOptions pullRawOptions);

    // ----------------------------------------------------------------------------------------------------
    // Ephemeral Subscribes
    // ----------------------------------------------------------------------------------------------------
    // * validates that a durable consumer name is not provided.
    // * validates pull vs push options as in durable subscribes. Or maybe lets the server
    // ----------------------------------------------------------------------------------------------------
    PushSyncSubscription pushSyncSubscribe(ConsumerConfiguration config);
    PushAsyncSubscription pushAsyncSubscribe(ConsumerConfiguration config, AsyncOptions asyncOptions);
    PullFetchSubscription pullFetchSubscribe(ConsumerConfiguration config, FetchOptions fetchOptions);
    PullAsyncSubscription pullAsyncSubscribe(ConsumerConfiguration config, FetchOptions fetchOptions, AsyncOptions asyncOptions);
    PullRawSubscription pullRawSubscribe(ConsumerConfiguration config, PullRawOptions pullRawOptions);
}
