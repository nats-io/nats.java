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

import java.nio.ByteBuffer;

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
	 * @return the subject that this message was sent to
	 */
	public String getSubject();

	/**
	 * @return the subject that this message was sent to
	 */
	public ByteBuffer getSubjectBuffer();

	/**
	 * @return the subject the application is expected to send a reply message on
	 */
	public String getReplyTo();

	/**
	 * @return the subject the application is expected to send a reply message on
	 */
	public ByteBuffer getReplyToBuffer();

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
	 * @return the id associated with the subscription, used by the connection when processing an incoming
	 * message from the server
	 */
	public ByteBuffer getSIDBuffer();

	/**
	 * @return the connection which can be used for publishing, will be null if the subscription is null
	 */
	public Connection getConnection();
}
