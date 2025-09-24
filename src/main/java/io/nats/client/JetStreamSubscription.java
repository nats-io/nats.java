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

import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * Subscription on a JetStream context.
 */
public interface JetStreamSubscription extends Subscription {

    /**
     * Gets the consumer name associated with the subscription.
     * @return the consumer name
     */
    String getConsumerName();

    /**
     * Gets the stream name associated with the subscription.
     * @return the stream name
     */
    String getStreamName();

    /**
     * Initiate pull with the specified batch size.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @throws IllegalStateException if not a pull subscription.
     */
    void pull(int batchSize);

    /**
     * Initiate pull with the specified request options
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param pullRequestOptions the options object
     * @throws IllegalStateException if not a pull subscription.
     */
    void pull(PullRequestOptions pullRequestOptions);

    /**
     * Initiate pull in noWait mode with the specified batch size.
     * The fetch will return immediately with as many messages as are available. Between zero and the maximum configured.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @throws IllegalStateException if not a pull subscription.
     */
    void pullNoWait(int batchSize);

    /**
     * Initiate pull in noWait mode with the specified batch size.
     * The fetch will return immediately with as many messages as are available, but at least one message. Between one and the maximum configured.
     * When no message is available it will wait for new messages to arrive till it expires.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @param expiresIn how long from now this request should be expired from the server wait list
     * @throws IllegalStateException if not a pull subscription.
     */
    void pullNoWait(int batchSize, Duration expiresIn);

    /**
     * Initiate pull in noWait mode with the specified batch size.
     * The fetch will return immediately with as many messages as are available, but at least one message. Between one and the maximum configured.
     * When no message is available it will wait for new messages to arrive till it expires.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @param expiresInMillis how long from now this request should be expired from the server wait list, in milliseconds
     * @throws IllegalStateException if not a pull subscription.
     */
    void pullNoWait(int batchSize, long expiresInMillis);

    /**
     * Initiate pull for all messages available before expiration.
     * <p>
     * <code>sub.nextMessage(timeout)</code> can return a:
     * <ul>
     * <li>regular JetStream message
     * <li>null
     * </ul>
     * <p>
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @param expiresIn how long from now this request should be expired from the server wait list
     * @throws IllegalStateException if not a pull subscription.
     */
    void pullExpiresIn(int batchSize, Duration expiresIn);

    /**
     * Initiate pull for all messages available before expiration.
     * This can only be used when the subscription is pull based.
     * <p>
     * <code>sub.nextMessage(timeout)</code> can return a:
     * <ul>
     * <li>regular JetStream message
     * <li>null
     * </ul>
     * <p>
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     * ! Primitive API for ADVANCED use only, officially not supported. Prefer fetch, iterate or reader.
     *
     * @param batchSize the size of the batch
     * @param expiresInMillis how long from now this request should be expired from the server wait list, in milliseconds
     * @throws IllegalStateException if not a pull subscription.
     */
    void pullExpiresIn(int batchSize, long expiresInMillis);

    /**
     * Fetch a list of messages up to the batch size, waiting no longer than maxWait.
     * This uses <code>pullExpiresIn</code> under the covers, and manages all responses
     * from <code>sub.nextMessage(...)</code> to only return regular JetStream messages.
     * This can only be used when the subscription is pull based.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     *
     * @param batchSize the size of the batch
     * @param maxWait the maximum time to wait to collect messages for the batch.
     *
     * @return the list of messages
     * @throws IllegalStateException if not a pull subscription.
     */
    List<Message> fetch(int batchSize, Duration maxWait);

    /**
     * Fetch a list of messages up to the batch size, waiting no longer than maxWait.
     * This uses <code>pullExpiresIn</code> under the covers, and manages all responses
     * from <code>sub.nextMessage(...)</code> to only return regular JetStream messages.
     * This can only be used when the subscription is pull based.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     *
     * @param batchSize the size of the batch
     * @param maxWaitMillis the maximum time to wait to collect messages for the batch, in milliseconds.
     *
     * @return the list of messages
     * @throws IllegalStateException if not a pull subscription.
     */
    List<Message> fetch(int batchSize, long maxWaitMillis);

    /**
     * Prepares an iterator. This uses <code>pullExpiresIn</code> under the covers,
     * and manages all responses. The iterator will have no messages if it does not
     * receive the first message within the max wait period. It will stop if the batch is
     * fulfilled or if there are fewer than batch size messages. 408 Status messages
     * are ignored and will not count toward the fulfilled batch size.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     *
     * @param batchSize the size of the batch
     * @param maxWait the maximum time to wait for the first message.
     *
     * @return the message iterator
     * @throws IllegalStateException if not a pull subscription.
     */
    Iterator<Message> iterate(int batchSize, Duration maxWait);

    /**
     * Prepares an iterator. This uses <code>pullExpiresIn</code> under the covers,
     * and manages all responses. The iterator will have no messages if it does not
     * receive the first message within the max wait period. It will stop if the batch is
     * fulfilled or if there are fewer than batch size messages. 408 Status messages
     * are ignored and will not count toward the fulfilled batch size.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     *
     * @param batchSize the size of the batch
     * @param maxWaitMillis the maximum time to wait for the first message, in milliseconds.
     *
     * @return the message iterator
     * @throws IllegalStateException if not a pull subscription.
     */
    Iterator<Message> iterate(int batchSize, long maxWaitMillis);

    /**
     * Prepares a reader. A reader looks like a push sync subscription,
     * meaning it is just an endless stream of messages to ask for by nextMessage,
     * but uses pull under the covers.
     * ! Pull subscriptions only. Push subscription will throw IllegalStateException
     *
     * @param batchSize the size of the batch
     * @param repullAt the point in the current batch to tell the server to start the next batch
     *
     * @return the message iterator
     * @throws IllegalStateException if not a pull subscription.
     */
    JetStreamReader reader(int batchSize, int repullAt);

    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;
}
