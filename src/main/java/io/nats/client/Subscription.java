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
    public String getSubject();
    public String getQueueName();

    /**
     * Read the next message for a subscription, or block until one is available.
     * 
     * <p>Will return null if the calls times out.</p>
     * 
     * <p>Use a timeout of 0 to wait indefinitely.</p>
     * 
     * @param timeout The maximum time to wait.
     * @return The next message for this subscriber or null.
     */
    public Message nextMessage(Duration timeout);
    
    /**
     * Unsubscribe this subscription and stop listening for messages.
     * 
     * <p><strong>TODO(sasbury)</strong> Timing on messages in the queue ...</p>
     */
    public void unsubscribe();

    /**
     * Unsubscribe this subscription and stop listening for messages, after the specified number
     * of messages.
     * 
     * <p><strong>TODO(sasbury)</strong> Timing on messages in the queue ...</p>
     * 
     * <p>Supports chaining so that you can do things like:</p>
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * m = nc.subscribe("hello").unsubscribe(1).nextMessage(Duration.ZERO);
     * </pre></blockquote></p>
     * 
     * @param after The number of messages to accept before unsubscribing
     * @return The subscription so that calls can be chained
     */
    public Subscription unsubscribe(int after);
}