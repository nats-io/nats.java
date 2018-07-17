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
 * {@link Dispatcher Dispatchers} use the MessageHandler interface to define the listener
 * for their messages. Each Dispatcher can have a single message handler, although the 
 * handler can use the incoming message's subject to branch for the actual work.
 */
public interface MessageHandler {
    /**
     * Called to deliver a message to the handler. This call is in the dispatcher's thread
     * and can block all other messages being delivered.
     * 
     * <p>The thread used to call onMessage will be interrupted if the connection is closed, or the dispatcher is stopped.
     *
     * @param msg the received Message
     * @throws InterruptedException if the dispatcher interrupts this handler
     */
    void onMessage(Message msg) throws InterruptedException;
}
