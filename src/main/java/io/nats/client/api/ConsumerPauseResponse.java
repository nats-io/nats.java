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

package io.nats.client.api;

import io.nats.client.Message;
import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.PAUSED;
import static io.nats.client.support.ApiConstants.PAUSE_REMAINING;
import static io.nats.client.support.ApiConstants.PAUSE_UNTIL;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readDate;
import static io.nats.client.support.JsonValueUtils.readLong;
import static io.nats.client.support.JsonValueUtils.readNanos;

public class ConsumerPauseResponse extends ApiResponse<ConsumerPauseResponse> {

    private final boolean paused;
    private final ZonedDateTime pauseUntil;
    private final Duration pauseRemaining;

    public ConsumerPauseResponse(Message msg) {
        super(msg);
        paused = readBoolean(jv, PAUSED);
        pauseUntil = readDate(jv, PAUSE_UNTIL);
        pauseRemaining = readNanos(jv, PAUSE_REMAINING);
    }

    /**
     * Returns true if the consumer was paused
     * @return whether the consumer is paused
     */
    public boolean isPaused() {
        return paused;
    }

    /**
     * Returns the time until the consumer is paused
     * @return pause until time
     */
    public ZonedDateTime getPauseUntil() {
        return pauseUntil;
    }

    /**
     * Returns how much time is remaining for this consumer to be paused
     * @return remaining paused time
     */
    public Duration getPauseRemaining() {
        return pauseRemaining;
    }
}
