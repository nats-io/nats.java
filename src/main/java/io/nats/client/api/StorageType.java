// Copyright 2020 The NATS Authors
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

/**
 * Stream storage types.
 */
public enum StorageType {
    File("file"),
    Memory("memory");

    private final String policy;

    StorageType(String p) {
        policy = p;
    }

    @Override
    public String toString() {
        return policy;
    }

    public static StorageType get(String value) {
        if (File.policy.equalsIgnoreCase(value)) { return File; }
        if (Memory.policy.equalsIgnoreCase(value)) { return Memory; }
        return null;
    }
}
