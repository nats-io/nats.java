// Copyright 2023 The NATS Authors
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

package io.nats.compatibility;

public enum Suite {
    DONE("done"),
    OBJECT_STORE("object-store");

    public final String id;

    Suite(String id) {
        this.id = id;
    }

    public static Suite instance(String text) {
        for (Suite suite : Suite.values()) {
            if (suite.id.equals(text)) {
                return suite;
            }
        }
        System.err.println("Unknown suite: " + text);
        System.exit(-7);
        return null;
    }

    @Override
    public String toString() {
        return id;
    }
}
