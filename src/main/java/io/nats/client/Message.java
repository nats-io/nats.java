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

import java.time.LocalDateTime;

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
	 * Gets the metadata associated with a jetstream messages.
	 * @return metadata, null if not a jetstream message.
	 */
	public JetstreamMetaData getJetstreamMetaData();

}
