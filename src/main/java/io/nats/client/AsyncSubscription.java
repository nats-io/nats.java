// Copyright 2015-2018 The NATS Authors
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

/**
 * A client uses an {@code AsyncSubscription} object to receive messages from a subject. It runs
 * asynchronously (on an {@code Executor}), receives one {@code Message} at a time, and invokes a
 * user-defined callback method to process each {@code Message}.
 */
public interface AsyncSubscription extends Subscription {

    /**
     * Starts asynchronous message delivery to this subscription
     *
     * @throws IllegalStateException if the subscription is invalid/closed. Reasons for this could
     *                               include reaching the subscription's autoUnsubscribe limit,
     *                               calling
     *                               {@link #unsubscribe()}, or closing the Connection.
     */
    @Deprecated
    void start();

    /**
     * @param cb the message handler to set for this subscription. When new messages arrive for this
     *           subscription, the {@link MessageHandler#onMessage(Message)} method will be invoked.
     * @see MessageHandler#onMessage(Message)
     */
    void setMessageHandler(MessageHandler cb);

    MessageHandler getMessageHandler();
}
