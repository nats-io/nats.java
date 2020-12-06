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
import java.time.ZonedDateTime;
import java.util.concurrent.TimeoutException;
import io.nats.client.impl.Headers;
import io.nats.client.support.Status;

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
	public interface MetaData {

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
		public ZonedDateTime timestamp();
	}

	/**
	 * @return the subject that this message was sent to
	 */
	String getSubject();

	/**
	 * @return the subject the application is expected to send a reply message on
	 */
	String getReplyTo();

	/**
	 * @return true if there are headers
	 */
	boolean hasHeaders();

	/**
	 * @return the headers object the message
	 */
	Headers getHeaders();

	/**
	 * @return true if there is status
	 */
	boolean hasStatus();

	/**
	 * @return the status object message
	 */
	Status getStatus();

	/**
	 * @return the data from the message
	 */
	byte[] getData();

	/**
	 * @return if is utf8Mode
	 */
	boolean isUtf8mode();

	/**
	 * @return the Subscription associated with this message, may be owned by a Dispatcher
	 */
	Subscription getSubscription();

	/**
	 * @return the id associated with the subscription, used by the connection when processing an incoming
	 * message from the server
	 */
	String getSID();

	/**
	 * @return the connection which can be used for publishing, will be null if the subscription is null
	 */
	public Connection getConnection();

	/**
	 * Gets the metadata associated with a jetstream message.
	 * @return metadata or null if the message is not a jetstream message.
	 */
	public MetaData metaData();

	/**
	 * ack acknowledges a JetStream messages received from a Consumer, indicating the message
	 * should not be received again later.
	 */
	public void ack();

	/**
	 * ack acknowledges a JetStream messages received from a Consumer, indicating the message
	 * should not be received again later.  Duration.ZERO does not confirm the acknowledgement.
	 * @param timeout the duration to wait for an ack confirmation
     * @throws TimeoutException if a timeout was specified and the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
	 */
	public void ackSync(Duration timeout) throws TimeoutException, InterruptedException;	

	/**
	 * nak acknowledges a JetStream message has been received but indicates that the message
	 * is not completely processed and should be sent again later.
	 */
	public void nak();

	/**
	 * term prevents this message from every being delivered regardless of maxDeliverCount.
	 */
	public void term();

	/**
	 *  Indicates that this message is being worked on and reset redelkivery timer in the server.
	 */
	public void inProgress();

	/**
	 * Checks if a message is from Jetstream or is a standard message.
	 * @return true if the message is from JetStream.
	 */
	public boolean isJetStream();

}
