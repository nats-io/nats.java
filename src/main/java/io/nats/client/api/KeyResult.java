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

public class KeyResult {

    private final String key;
    private final Exception e;

    public KeyResult() {
        this.key = null;
        this.e = null;
    }

    public KeyResult(String key) {
        this.key = key;
        this.e = null;
    }

    public KeyResult(Exception e) {
        this.key = null;
        this.e = e;
    }

    public String getKey() {
        return key;
    }

    public Exception getException() {
        return e;
    }

    public boolean isKey() {
        return key != null;
    }

    public boolean isException() {
        return e != null;
    }

    public boolean isDone() {
        return key == null;
    }
}
