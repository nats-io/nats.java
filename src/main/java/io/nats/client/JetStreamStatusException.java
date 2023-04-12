// Copyright 2021 The NATS Authors
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

import io.nats.client.support.Status;

/**
 * JetStreamStatusException is used to indicate an unknown status message was received.
 */
public class JetStreamStatusException extends IllegalStateException {
    public static final String DEFAULT_DESCRIPTION = "Unknown or unprocessed status message";

    private final JetStreamSubscription sub;
    private final String description;
    private final Status status;

    /**
     * Construct an exception with a status message
     * @param sub the subscription
     * @param status the status
     */
    public JetStreamStatusException(JetStreamSubscription sub, Status status) {
        this(sub, DEFAULT_DESCRIPTION, status);
    }

    /**
     * Construct an exception with a status message
     * @param sub the subscription
     * @param description custom description
     * @param status the status
     */
    public JetStreamStatusException(JetStreamSubscription sub, String description, Status status) {
        super(description + ": " + status.getMessage());
        this.sub = sub;
        this.description = description;
        this.status = status;
    }

    /**
     * Construct an exception with a status message
     * @param status the status
     */
    public JetStreamStatusException(Status status) {
        super(status.getMessageWithCode());
        this.sub = null;
        this.description = status.toString();
        this.status = status;
    }

    /**
     * Get the subscription this issue occurred on
     *
     * @return the subscription
     */
    public JetStreamSubscription getSubscription() {
        return sub;
    }

    /**
     * Get the description
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the full status object
     *
     * @return the status
     */
    public Status getStatus() {
        return status;
    }
}