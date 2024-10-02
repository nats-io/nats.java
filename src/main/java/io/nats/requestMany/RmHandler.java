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

package io.nats.requestMany;

/**
 * This class is EXPERIMENTAL, meaning it's api is subject to change.
 * This interface is used by the RequestMany request method to
 * <ul>
 * <li>Give messages to the user as the come in over the wire</li>
 * <li>For the user to indicate to continue or stop waiting for messages. If the user is not using the sentinel pattern, they should always return true.</li>
 * </ul>
 */
public interface RmHandler {
    /**
     * Accept a message from the request method.
     * @param message the message. It may be an end of data (EOD) marker
     * @return true if the RequestMany should continue to wait for messages. False to stop waiting.
     */
    boolean handle(RmMessage message);
}
