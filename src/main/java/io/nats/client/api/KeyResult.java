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
package io.nats.client.api;

import org.jspecify.annotations.Nullable;

/**
 * Class is used as the result or consuming keys
 */
public class KeyResult {

    private final String key;
    private final Exception e;

    /**
     * Construct a valueless Key Result - used as the done marker without an exception
     */
    public KeyResult() {
        this.key = null;
        this.e = null;
    }

    /**
     * Construct a key result for a specific key
     * @param key the key
     */
    public KeyResult(String key) {
        this.key = key;
        this.e = null;
    }

    /**
     * Construct a key result containing an exception
     * @param e the exception
     */
    public KeyResult(Exception e) {
        this.key = null;
        this.e = e;
    }

    /**
     * Get the key in this result
     * @return the key or null if there is no key
     */
    @Nullable
    public String getKey() {
        return key;
    }

    /**
     * Get the exception in this result
     * @return the exception or null if there is no exception
     */
    @Nullable
    public Exception getException() {
        return e;
    }

    /**
     * Whether this result is a key.
     * @return true if the result is a key
     */
    public boolean isKey() {
        return key != null;
    }

    /**
     * Whether this result is an exception
     * @return true if the result is an exception
     */
    public boolean isException() {
        return e != null;
    }

    /**
     * If there is no key, the result indicates the consume is done, even if there is an exception.
     * @return true if this result indicates the consume is done.
     */
    public boolean isDone() {
        return key == null;
    }
}
