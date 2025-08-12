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

package io.nats.client.api;

import java.time.Duration;

public class KeyValuePurgeOptions {

    /**
     * The default time in millis that is used for as the threshold to keep markers.
     */
    public static final long DEFAULT_THRESHOLD_MILLIS = Duration.ofMinutes(30).toMillis();

    private final long deleteMarkersThresholdMillis;

    private KeyValuePurgeOptions(Builder b) {
        this.deleteMarkersThresholdMillis = b.deleteMarkersThresholdMillis;
    }

    /**
     * The value of the delete marker threshold, in milliseconds.
     * @return the threshold
     */
    public long getDeleteMarkersThresholdMillis() {
        return deleteMarkersThresholdMillis;
    }

    /**
     * Creates a builder for the Key Value Purge Options.
     * @return a key value purge options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * KeyValuePurgeOptions is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new KeyValuePurgeOptions.Builder().build()} will create a new KeyValuePurgeOptions.
     *
     */
    public static class Builder {
        private long deleteMarkersThresholdMillis = DEFAULT_THRESHOLD_MILLIS;

        /**
         * Set the delete marker threshold.
         * Null or duration of 0 will assume the default threshold {@link #DEFAULT_THRESHOLD_MILLIS}
         * Duration less than zero will assume no threshold and will not keep any markers.
         * @param deleteMarkersThreshold the threshold duration or null
         * @return The builder
         */
        public Builder deleteMarkersThreshold(Duration deleteMarkersThreshold) {
            this.deleteMarkersThresholdMillis = deleteMarkersThreshold == null
                ? DEFAULT_THRESHOLD_MILLIS : deleteMarkersThreshold.toMillis();
            return this;
        }

        /**
         * Set the delete marker threshold.
         * 0 will assume the default threshold {@link #DEFAULT_THRESHOLD_MILLIS}
         * Less than zero will assume no threshold and will not keep any markers.
         * @param deleteMarkersThresholdMillis the threshold millis
         * @return The builder
         */
        public Builder deleteMarkersThreshold(long deleteMarkersThresholdMillis) {
            this.deleteMarkersThresholdMillis = deleteMarkersThresholdMillis;
            return this;
        }

        /**
         * Set the delete marker threshold to -1 so as to not keep any markers
         * @return The builder
         */
        public Builder deleteMarkersNoThreshold() {
            this.deleteMarkersThresholdMillis = -1;
            return this;
        }

        /**
         * Build the Key Value Purge Options
         * @return the options
         */
        public KeyValuePurgeOptions build() {
            if (deleteMarkersThresholdMillis < 0) {
                deleteMarkersThresholdMillis = -1;
            }
            else if (deleteMarkersThresholdMillis == 0) {
                deleteMarkersThresholdMillis = DEFAULT_THRESHOLD_MILLIS;
            }
            return new KeyValuePurgeOptions(this);
        }
    }
}
