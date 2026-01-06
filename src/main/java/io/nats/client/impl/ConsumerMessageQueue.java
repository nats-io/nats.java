// Copyright 2015-2025 The NATS Authors
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

import java.time.Duration;

class ConsumerMessageQueue extends MessageQueueBase {

    ConsumerMessageQueue() {
        super();
    }

    void push(NatsMessage msg) {
        if (queue.offer(msg)) {
            length.incrementAndGet();
            sizeInBytes.addAndGet(msg.getSizeInBytes());
        }
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!isRunning()) {
            return null;
        }

        NatsMessage msg = _poll(timeout);

        if (msg == null) {
            return null;
        }

        length.decrementAndGet();
        sizeInBytes.addAndGet(-msg.getSizeInBytes());
        return msg;
    }
}
