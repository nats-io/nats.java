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

package io.nats.client.utility;

/**
 * The RetryObserver gives an opportunity to inspect an exception
 * and optionally stop retries before the retry config is exhausted
 * <p><b>THIS CLASS IS EXPERIMENTAL AND THE API IS SUBJECT TO CHANGE</b></p>
 */
public interface RetryObserver {
    /**
     * Inspect the exception to determine if the execute should retry.
     * @param e the exception that occurred when executing the action.
     * @return true if the execution should retry (assuming that the retry config is not exhausted)
     */
    boolean shouldRetry(Exception e);
}
