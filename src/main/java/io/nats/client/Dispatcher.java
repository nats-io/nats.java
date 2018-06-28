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

public interface Dispatcher extends Consumer {

    /**
     * Create a subscription to the specified subject under the control of this
     * dispatcher.
     * 
     * <p>
     * This call is a no-op if the dispatcher already has a subscription to the
     * specified subject.
     * 
     * 
     * @param subject The subject to subscribe to.
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was closed
     */
    public Dispatcher subscribe(String subject);

    /**
     * Create a subscription to the specified subject and queue under the control of
     * this dispatcher.
     * 
     * <p>
     * This call is a no-op if the dispatcher already has a subscription to the
     * specified subject (regardless of the queue name).
     * 
     * 
     * @param subject The subject to subscribe to.
     * @param queue The queue group to join.
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was closed
     */
    public Dispatcher subscribe(String subject, String queue);

    /**
     * Unsubscribe from the specified subject, the queue is implicit.
     * 
     * <p>Stops messages to the subscription locally and notifies the server.
     * 
     * @param subject The subject to unsubscribe from.
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was closed
     */
    public Dispatcher unsubscribe(String subject);

    /**
     * Unsubscribe from the specified subject, the queue is implicit, after the
     * specified number of messages.
     * 
     * <p>If the subscription has already received <code>after</code> messages, it will not receive
     * more. The provided limit is a lifetime total for the subscription, with the caveat
     * that if the subscription already received more than <code>after</code> when unsubscribe is called
     * the client will not travel back in time to stop them.
     * 
     * <p>For example, to get a single asynchronous message, you might do:
     * <blockquote><pre>
     * nc = Nats.connect()
     * d = nc.createDispatcher(myHandler);
     * d.subscribe("hello").unsubscribe("hello", 1);
     * </pre></blockquote>
     * 
     * @param subject The subject to unsubscribe from.
     * @param after The number of messages to accept before unsubscribing
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was closed
     */
    public Dispatcher unsubscribe(String subject, int after);
}