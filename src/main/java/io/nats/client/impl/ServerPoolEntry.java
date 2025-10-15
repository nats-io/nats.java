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

package io.nats.client.impl;

import io.nats.client.support.NatsUri;

public class ServerPoolEntry {
    public final NatsUri nuri;
    public final boolean isGossiped;
    public int failedAttempts;
    public long lastAttempt;

    public ServerPoolEntry(NatsUri nuri, boolean isGossiped) {
        this.nuri = nuri;
        this.isGossiped = isGossiped;
    }

    @Override
    public String toString() {
        return nuri + " " + isGossiped + "/" + failedAttempts;
    }
}
