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

import java.time.Duration;

/**
 * A Subscription encapsulates an incoming queue of messages associated with a single
 * subject and optional queue name. Subscriptions can be in one of two modes. Either the
 * subscription can be used for synchronous reading of messages with {@link #nextMessage(Duration) nextMessage()}
 * or the subscription can be owned by a Dispatcher. When a subscription is owned by a dispatcher
 * it cannot be used to get messages or unsubscribe, those operations must be performed on the dispatcher.
 * 
 * <p>Subscriptions support the concept of auto-unsubscribe. This concept is a bit tricky, as it involves
 * the client library, the server and the Subscriptions history. If told to unsubscribe after 5 messages, a subscription
 * will stop receiving messages when one of the following occurs:
 * <ul>
 * <li>The subscription delivers 5 messages with next messages, <em>including any previous messages</em>.
 * <li>The server sends 5 messages to the subscription.
 * <li>The subscription already received 5 or more messages.
 * </ul>
 * <p>In the case of a reconnect, the remaining message count, as maintained by the subscription, will be sent
 * to the server. So if you unsubscribe with a max of 5, then disconnect after 2, the new server will be told to
 * unsubscribe after 3.
 * <p>The other, possibly confusing case, is that unsubscribe is based on total messages. So if you make a subscription and
 * receive 5 messages on it, then say unsubscribe with a maximum of 5, the subscription will immediately stop handling messages.
 */
public interface Subscription extends Consumer {

    /**
     * @return the subject associated with this subscription, will be non-null
     */
    String getSubject();

    /**
     * @return the queue associated with this subscription, may be null.
     */
    String getQueueName();

    /**
     * @return the Dispatcher that owns this subscription, or null
     */
    Dispatcher getDispatcher();

    /**
     * Read the next message for a subscription, or block until one is available.
     * While useful in some situations, i.e. tests and simple examples, using a
     * Dispatcher is generally easier and likely preferred for application code.
     * 
     * <p>Will return null if the calls times out.
     * 
     * 
     * <p>Use a timeout of 0 to wait indefinitely. This could still be interrupted if
     * the subscription is unsubscribed or the client connection is closed.
     * 
     * 
     * @param timeout the maximum time to wait
     * @return the next message for this subscriber or null if there is a timeout
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     * @throws InterruptedException if one occurs while waiting for the message
     */
    Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException;

    /**
     * Unsubscribe this subscription and stop listening for messages.
     * 
     * <p>Stops messages to the subscription locally and notifies the server.
     * 
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     */
    void unsubscribe();

    /**
     * Unsubscribe this subscription and stop listening for messages, after the
     * specified number of messages.
     * 
     * <p>If the subscription has already received <code>after</code> messages, it will not receive
     * more. The provided limit is a lifetime total for the subscription, with the caveat
     * that if the subscription already received more than <code>after</code> when unsubscribe is called
     * the client will not travel back in time to stop them.
     * 
     * <p>Supports chaining so that you can do things like:
     * <pre>
     * nc = Nats.connect()
     * m = nc.subscribe("hello").unsubscribe(1).nextMessage(Duration.ZERO);
     * </pre>
     * 
     * @param after the number of messages to accept before unsubscribing
     * @return the subscription so that calls can be chained
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     */
    Subscription unsubscribe(int after);
}
