// Copyright 2022 The NATS Authors
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

package io.nats.client.support;

/**
 * Class representing the current state of a pull subscription
 */
public class PullStatus {
    private final long pendingMessages;
    private final long pendingBytes;
    private final boolean trackingHeartbeats;

    /**
     * Construct a pull status object. This is really an internal object,
     * and should not be constructed by users
     * @param pendingMessages the number of pending messages
     * @param pendingBytes the number of pending bytes
     * @param trackingHeartbeats whether heartbeats are currently being tracked
     */
    public PullStatus(long pendingMessages, long pendingBytes, boolean trackingHeartbeats) {
        this.pendingMessages = pendingMessages;
        this.pendingBytes = pendingBytes;
        this.trackingHeartbeats = trackingHeartbeats;
    }

    /**
     * Get the number of pending messages for the pull
     * @return the pending messages
     */
    public long getPendingMessages() {
        return pendingMessages;
    }

    /**
     * Get the number of pending bytes for the pull
     * @return the pending bytes
     */
    public long getPendingBytes() {
        return pendingBytes;
    }

    /**
     * Get whether heartbeats are currently being tracked.
     * @return true if heartbeats are currently being tracked
     */
    public boolean isTrackingHeartbeats() {
        return trackingHeartbeats;
    }
}
