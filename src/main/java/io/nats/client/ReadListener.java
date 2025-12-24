// Copyright 2024 The NATS Authors
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
 * A listener that allows the user to track messages as soon as they come in off the wire.
 * Be aware that implementations tracking messages can slow down handing off of those messages to the subscriptions waiting for them
 * if they take a significant time to process them. This class is really intended for debugging purposes.
 */
public interface ReadListener {
    /**
     * Called when the message is specifically a protocol message
     * @param op the protocol operation
     * @param text the text associated with the protocol if there is any. May be null
     */
    default void protocol(String op, String text) {};

    /**
     * Called when the message is any non-protocol message
     * @param op the message operation
     * @param message the actual message
     */
    default void message(String op, Message message) {};
}
