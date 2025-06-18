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

import org.jetbrains.annotations.Nullable;

/**
 * Key Value Operations Enum
 */
public enum KeyValueOperation {
    PUT("PUT"), DELETE("DEL"), PURGE("PURGE");

    private final String headerValue;

    KeyValueOperation(String headerValue) {
        this.headerValue = headerValue;
    }

    public String getHeaderValue() {
        return headerValue;
    }

    @Nullable
    public static KeyValueOperation instance(String s) {
        if (PUT.headerValue.equals(s)) return PUT;
        if (DELETE.headerValue.equals(s)) return DELETE;
        if (PURGE.headerValue.equals(s)) return PURGE;
        return null;
    }

    @Nullable
    public static KeyValueOperation getOrDefault(String s, KeyValueOperation dflt) {
        KeyValueOperation kvo = instance(s);
        return kvo == null ? dflt : kvo;
    }

    @Nullable
    public static KeyValueOperation instanceByMarkerReason(String markerReason) {
        if ("Remove".equals(markerReason)) {
            return DELETE;
        }
        if ("MaxAge".equals(markerReason) || "Purge".equals(markerReason)) {
            return PURGE;
        }
        return null;
    }
}
