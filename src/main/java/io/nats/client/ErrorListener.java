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

import java.io.IOException;

import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.ServerInfo;
import io.nats.client.impl.NatsJetStreamSubscription;
import io.nats.client.support.Status;

/**
 * This library groups problems into four categories:
 * <dl>
 * <dt>Errors</dt>
 * <dd>The server sent an error message using the {@code -err} protocol operation.</dd>
 * <dt>Exceptions</dt>
 * <dd>A Java exception occurred, and was handled by the library.</dd>
 * <dt>Slow Consumers</dt>
 * <dd>One of the connections consumers, Subscription or Dispatcher, is slow, and starting to drop messages.</dd>
 * <dt>Fast Producers</dt>
 * <dd>One of the connections producers is too fast, and is discarding messages</dd>
 * </dl>
 * <p>All of these problems are reported to the application code using the ErrorListener. The
 * listener is configured in the {@link Options Options} at creation time.
 */
public interface ErrorListener {
    /**
     * NATs related errors that occur asynchronously in the client library are sent
     * to an ErrorListener via errorOccurred. The ErrorListener can use the error text to decide what to do about the problem.
     * <p>The text for an error is described in the protocol doc at `https://nats.io/documentation/internals/nats-protocol`.
     * <p>In some cases the server will close the clients connection after sending one of these errors. In that case, the
     * connections {@link ConnectionListener ConnectionListener} will be notified.
     * @param conn The connection associated with the error
     * @param error The text of error that has occurred, directly from the server
     */
    default void errorOccurred(Connection conn, String error) {};

    /**
     * Exceptions that occur in the "normal" course of operations are sent to the
     * ErrorListener using exceptionOccurred. Examples include, application exceptions
     * during Dispatcher callbacks, IOExceptions from the underlying socket, etc..
     * The library will try to handle these, via reconnect or catching them, but they are
     * forwarded here in case the application code needs them for debugging purposes.
     *
     * @param conn The connection associated with the error
     * @param exp The exception that has occurred, and was handled by the library
     */
    default void exceptionOccurred(Connection conn, Exception exp) {};

    /**
     * Called by the connection when a &quot;slow&quot; consumer is detected. This call is only made once
     * until the consumer stops being slow. At which point it will be called again if the consumer starts
     * being slow again.
     *
     * <p>See {@link Consumer#setPendingLimits(long, long) Consumer.setPendingLimits}
     * for information on how to configure when this method is fired.
     *
     * <p> Slow consumers will result in dropped messages each consumer provides a method
     * for retrieving the count of dropped messages, see {@link Consumer#getDroppedCount() Consumer.getDroppedCount}.
     *
     * @param conn The connection associated with the error
     * @param consumer The consumer that is being marked slow
     */
    default void slowConsumerDetected(Connection conn, Consumer consumer) {};

    /**
     * Called by the connection when a message is discarded.
     *
     * @param conn The connection that discarded the message
     * @param msg The message that is discarded
     */
    default void messageDiscarded(Connection conn, Message msg) {}

    /**
     * Called when subscription heartbeats are missed according to the configured period and threshold.
     * The consumer must be configured with an idle heartbeat time.
     *
     * @param conn The connection that had the issue
     * @param sub the JetStreamSubscription that this occurred on
     * @param lastStreamSequence the last received stream sequence
     * @param lastConsumerSequence the last received consumer sequence
     */
    default void heartbeatAlarm(Connection conn, JetStreamSubscription sub,
                                long lastStreamSequence, long lastConsumerSequence) {}

    /**
     * Called when an unhandled status is received in a push subscription.
     * @param conn The connection that had the issue
     * @param sub the JetStreamSubscription that this occurred on
     * @param status the status
     */
    default void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {}

    /**
     * Called when a pull subscription receives a status message that indicates either
     * the subscription or pull might be problematic
     *
     * @param conn   The connection that had the issue
     * @param sub    the JetStreamSubscription that this occurred on
     * @param status the status
     */
    default void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {}

    /**
     * Called when a pull subscription receives a status message that indicates either
     * the subscription cannot continue or the pull request cannot be processed.
     *
     * @param conn   The connection that had the issue
     * @param sub    the JetStreamSubscription that this occurred on
     * @param status the status
     */
    default void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {}

    enum FlowControlSource { FLOW_CONTROL, HEARTBEAT }

    /**
     * Called by the connection when a flow control is processed.
     *
     * @param conn The connection that had the issue
     * @param sub the JetStreamSubscription that this occurred on
     * @param subject the flow control subject that was handled
     * @param source enum indicating flow control handling in response to which type of message
     */
    default void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {}

    /**
     * Called by the connection when a low level socket write timeout occurs.
     *
     * @param conn The connection that had the issue
     */
    default void socketWriteTimeout(Connection conn) {}

    /**
     * General message producing function which understands the possible parameters to listener calls.
     * @param label the label for the message
     * @param conn The connection that had the issue, if provided.
     * @param consumer The consumer that is being marked slow, if applicable
     * @param sub the JetStreamSubscription that this occurred on, if applicable
     * @param pairs custom string pairs. I.E. "foo: ", fooObject, "bar-", barObject will be appended
     *              to the message like ", foo: &lt;fooValue&gt;, bar-&lt;barValue&gt;".
     * @return the message
     */
    default String supplyMessage(String label, Connection conn, Consumer consumer, Subscription sub, Object... pairs) {
        StringBuilder sb = new StringBuilder(label == null ? "" : label);
        if (conn != null) {
            ServerInfo si = conn.getServerInfo();
            if (si != null) {
                sb.append(", Connection: ").append(conn.getServerInfo().getClientId());
            }
        }
        if (consumer != null) {
            sb.append(", Consumer: ").append(consumer.hashCode());
        }
        if (sub != null) {
            sb.append(", Subscription: ").append(sub.hashCode());
            if (sub instanceof JetStreamSubscription) {
                JetStreamSubscription jssub = (JetStreamSubscription)sub;
                sb.append(", Consumer Name: ").append(jssub.getConsumerName());
            }
        }
        for (int x = 0; x < pairs.length; x++) {
            sb.append(", ").append(pairs[x]).append(pairs[++x]);
        }
        return sb.toString();
    }

}
