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

public interface Subscription {

    /**
     * Subscriptions are active until they are unsubscribed.
     * 
     * @return true if the Subscription is still receiving messages
     */
    public boolean isActive();

    public String getSubject();

    public String getQueueName();

    public boolean isSingleReaderThreadMode();
    public void enableSingleReaderThreadMode();

    /**
     * Read the next message for a subscription, or block until one is available.
     * While useful in some situations, i.e. tests and simple examples, using a
     * Dispatcher is generally easier and likely preferred for applciation code.
     * 
     * <p>
     * Will return null if the calls times out.
     * </p>
     * 
     * <p>
     * Use a timeout of 0 to wait indefinitely. This could still be interrupted if
     * the subscription is unsubscribed or the client connection is closed.
     * </p>
     * 
     * @param timeout The maximum time to wait.
     * @return The next message for this subscriber or null if there is a timeout.
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     * @throws InterruptedException if one occurs while waiting for the message
     */
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException;

    /**
     * Unsubscribe this subscription and stop listening for messages.
     * 
     * <p>Stops messages to the subscription locally and notifies the server.</p>
     * 
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     */
    public void unsubscribe();

    /**
     * Unsubscribe this subscription and stop listening for messages, after the
     * specified number of messages. Works with the server.
     * 
     * <p>If the subscription has already received <code>after</code> messages, it will not receive
     * more. The provided limit is a lifetime total for the subscription, with the caveat
     * that if the subscription already received more than <code>after</code> when unsubscribe is called
     * the client will not travel back in time to stop them.</p>
     * 
     * <p>Supports chaining so that you can do things like:</p>
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * m = nc.subscribe("hello").unsubscribe(1).nextMessage(Duration.ZERO);
     * </pre></blockquote></p>
     * 
     * @param after The number of messages to accept before unsubscribing
     * @return The subscription so that calls can be chained
     * @throws IllegalStateException if the subscription belongs to a dispatcher, or is not active
     */
    public Subscription unsubscribe(int after);
}