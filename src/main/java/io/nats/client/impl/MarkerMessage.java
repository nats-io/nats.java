// Copyright 2025 The NATS Authors
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

import static io.nats.client.support.NatsConstants.EMPTY_BODY;

class MarkerMessage extends NatsMessage {
    // Poison pill is a graphic, but common term for an item that breaks loops or stop something.
    // In this class the poison pill is used to break out of timed waits on the blocking queue.
    // A simple == is used to resolve if any message is exactly the static pill object in question
    static final MarkerMessage POISON_PILL = new MarkerMessage("_poison");

    static final MarkerMessage END_RECONNECT = new MarkerMessage("_end");

    public MarkerMessage(String subject) {
        super(subject, null, EMPTY_BODY);
    }
}
