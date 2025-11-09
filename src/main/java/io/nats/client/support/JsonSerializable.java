// Copyright 2021-2023 The NATS Authors
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

import org.jspecify.annotations.NonNull;

import java.nio.charset.StandardCharsets;

/**
 * A general interface for an object to be able to converted to JSON
 */
public interface JsonSerializable {
    /**
     * Get the JSON string
     * @return the string
     */
    @NonNull
    String toJson();

    /**
     * Get the JSON string as a byte array
     * @return the byte array
     */
    default byte @NonNull [] serialize() {
        return toJson().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get the JsonValue for the object
     * @return the value
     */
    @NonNull
    default JsonValue toJsonValue() {
        return JsonParser.parseUnchecked(toJson());
    }
}
