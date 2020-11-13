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

import java.sql.Time;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeoutException;

/**
 * The NATS library uses a Message object to encapsulate incoming messages. Applications
 * publish and send requests with raw strings and byte[] but incoming messages can have a few
 * values, so they need a wrapper.
 * 
 * <p>The byte[] returned by {@link #getData() getData()} is not shared with any library code
 * and is safe to manipulate.
 */
public interface Message {

	/**
	 * This interface defined the methods use to get Jetstream metadata about a message, 
	 * when applicable.
	 */
	public interface JetstreamMetaData {

		/**
		 * Gets the stream the message is from.
		 * @return the stream.
		 */
		public String getStream();
		
		/**
		 * Gets the consumer that generated this message.
		 * @return the consumer.
		 */
		public String getConsumer();


		/**
		 * Gets the number of times this message has been delivered.
		 * @return delivered count.
		 */
		public long deliveredCount();
		 
		/**
		 * Gets the stream sequence number of the message.
		 * @return sequence number
		 */
		public long streamSequence();

		/**
		 * Gets consumer sequence number of this message.
		 * @return sequence number
		 */
		public long consumerSequence();

		/**
		 * Gets the pending count of the consumer.
		 * @return pending count
		 */
		public long pendingCount();

		/**
		 * Gets the timestamp of the message.
		 * @return the timestamp
		 */
		public LocalDateTime timestamp();
	}

	/**
	 * @return the subject that this message was sent to
	 */
	public String getSubject();

	/**
	 * @return the subject the application is expected to send a reply message on
	 */
	public String getReplyTo();

	/**
	 * @return the data from the message
	 */
	public byte[] getData();

	/**
	 * @return the Subscription associated with this message, may be owned by a Dispatcher
	 */
	public Subscription getSubscription();

	/**
	 * @return the id associated with the subscription, used by the connection when processing an incoming
	 * message from the server
	 */
	public String getSID();

	/**
	 * @return the connection which can be used for publishing, will be null if the subscription is null
	 */
	public Connection getConnection();

	/**
	 * Gets the metadata associated with a jetstream message.
	 * @return metadata or null if the message is not a jetstream message.
	 */
	public JetstreamMetaData getJetstreamMetaData();

	/**
	 * ack acknowledges a JetStream messages received from a Consumer, indicating the message
	 * should not be received again later.  Duration.ZERO does not confirm the acknowledgement.
	 * @param timeout The duration to wait for a confirmation.
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public void ack(Duration timeout) throws TimeoutException, InterruptedException;

	/**
	 * nak acknowledges a JetStream message received from a Consumer, indicating that the message
	 * is not completely processed and should be sent again later. 
	 * A timeout of Duration.ZERO does not wait to confirm the acknowledgement.
	 * @param timeout the duration to wait for a response
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public void nak(Duration timeout) throws TimeoutException, InterruptedException;

	/**
	 * ackProgress acknowledges a Jetstream message received from a Consumer, indicating that work is
	 * ongoing and further processing time is required equal to the configured AckWait of the Consumer.
	 * A timeout of Duration.ZERO does not wait to confirm the acknowledgement.
	 * @param timeout
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public void ackProgress(Duration timeout) throws TimeoutException, InterruptedException;

	/**
	 * ackNext performs an Ack() and request the next message.  To request multiple messages use AckNextRequest()
	 */
	public void ackNext();
	/**
	 * ackNextRequest performs an acknowledgement of a message and request the next batch of messages.
	 * @param expiry the time the server will stop honoring this request
	 * @param batch the number of messages to request
	 * @param noWait if true, return when existing messages have been processed
	 */
	public void ackNextRequest(LocalDateTime expiry, long batch, boolean noWait);

	/**
	 * ackAndFetch performs an AckNext() and returns the next message from the stream.
	 * A timeout of Duration.ZERO does not wait to confirm the acknowledgement.
	 * @param timeout
	 * @return the next message from the stream, or null if the request timed out.
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 */
	public Message ackAndFetch(Duration timeout) throws InterruptedException;

	/**
	 * ackTerm acknowledges a message received from JetStream indicating the message will not be processed
	 * and should not be sent to another consumer.
	 * A timeout of Duration.ZERO does not wait to confirm the acknowledgement.
	 * @param timeout
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public void ackTerm(Duration timeout) throws TimeoutException, InterruptedException;

}
