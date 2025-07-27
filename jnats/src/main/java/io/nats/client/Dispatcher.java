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
 * This library uses the concept of a Dispatcher to organize message callbacks in a way that the
 * application can control. Each dispatcher has a single {@link MessageHandler MessageHandler} that
 * will be notified of incoming messages. The dispatcher also has 0 or more subscriptions associated with it.
 * This means that a group of subscriptions, or subjects, can be combined into a single callback thread. But,
 * multiple dispatchers can be created to handle different groups of subscriptions/subjects.
 *
 * <p>All messages to this dispatcher are delivered via a single thread. If the message handler is slow to handle
 * any one message it will delay deliver of subsequent messages. Use separate dispatchers to handle the scenario of
 * a set of messages that require a lot of work and a set of fast moving messages, or create other threads as necessary.
 * The Dispatcher will only use one.
 *
 * <p>Dispatchers are created from the connection using {@link Connection#createDispatcher(MessageHandler) createDispatcher()}
 * and can be closed using {@link Connection#closeDispatcher(Dispatcher) closeDispatcher()}. Closing a dispatcher will
 * clean up the thread it is using for message deliver.
 *
 * <p><em>See the documentation on {@link Consumer Consumer} for configuring behavior in a slow consumer situation.</em>
 */
public interface Dispatcher extends Consumer {

    /**
     * Start the dispatcher with a given id.
     * Use post-construction to start to the dispatcher,
     * which should not be started on construction.
     * @param id the assigned id of the dispatcher
     */
    void start(String id);

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
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Dispatcher subscribe(String subject);

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
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Dispatcher subscribe(String subject, String queue);

    /**
     * Create a subscription to the specified subject under the control of this
     * dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * <p>
     * Every call creates a new subscription, unlike the
     * {@link Dispatcher#subscribe(String)} method that does not take a
     * MessageHandler.
     *
     *
     * @param subject The subject to subscribe to.
     * @param handler The target for the messages
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Subscription subscribe(String subject, MessageHandler handler);

    /**
     * Create a subscription to the specified subject under the control of this
     * dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * <p>
     * Every call creates a new subscription, unlike the
     * {@link Dispatcher#subscribe(String, String)} method that does not take a
     * MessageHandler.
     *
     *
     * @param subject The subject to subscribe to.
     * @param queue The queue group to join.
     * @param handler The target for the messages
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Subscription subscribe(String subject, String queue, MessageHandler handler);

    /**
     * Unsubscribe from the specified subject, the queue is implicit.
     *
     * <p>Stops messages to the subscription locally and notifies the server.
     *
     * @param subject The subject to unsubscribe from.
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Dispatcher unsubscribe(String subject);

    /**
     * Unsubscribe from the specified Subscription.
     *
     * <p>Stops messages to the subscription locally and notifies the server.
     * This method is to be used to unsubscribe from subscriptions created by
     * the Dispatcher using {@link Dispatcher#subscribe(String, MessageHandler)}.
     *
     * @param subscription The Subscription to unsubscribe from.
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was previously closed
     * @throws IllegalStateException if the Subscription is not managed by this dispatcher
     */
    Dispatcher unsubscribe(Subscription subscription);

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
     * @throws IllegalStateException if the dispatcher was previously closed
     */
    Dispatcher unsubscribe(String subject, int after);

    /**
     * Unsubscribe from the specified subject, the queue is implicit, after the
     * specified number of messages.
     *
     * <p>If the subscription has already received <code>after</code> messages, it will not receive
     * more. The provided limit is a lifetime total for the subscription, with the caveat
     * that if the subscription already received more than <code>after</code> when unsubscribe is called
     * the client will not travel back in time to stop them.
     *
     * <p>Stops messages to the subscription locally and notifies the server.
     * This method is to be used to unsubscribe from subscriptions created by
     * the Dispatcher using {@link Dispatcher#subscribe(String, MessageHandler)}.
     *
     * @param subscription The Subscription to unsubscribe from.
     * @param after The number of messages to accept before unsubscribing
     * @return The Dispatcher, so calls can be chained.
     * @throws IllegalStateException if the dispatcher was previously closed
     * @throws IllegalStateException if the Subscription is not managed by this dispatcher
     */
    Dispatcher unsubscribe(Subscription subscription, int after);
}
