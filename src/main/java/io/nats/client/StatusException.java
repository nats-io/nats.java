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
public class StatusException extends IllegalStateException {
    protected final Subscription sub;
    protected final Status status;

    /**
     * Construct an exception with a status message
     *
     * @param sub the subscription
     * @param status the status
     */
    public StatusException(Subscription sub, Status status) {
        super(status.getMessage());
        this.sub = sub;
        this.status = status;
    }

    protected StatusException(String label, Subscription sub, Status status) {
        super(label + ": " + status.getMessage());
        this.sub = sub;
        this.status = status;
    }

    /**
     * Get the subscription this issue occurred on
     *
     * @return the subscription
     */
    public Subscription getSubscription() {
        return sub;
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
