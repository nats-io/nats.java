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

package io.nats.client;

import io.nats.client.impl.NatsMessage;

import java.util.concurrent.ExecutorService;

public abstract class WriteBufferListener {
    public final ExecutorService executorService;

    public WriteBufferListener(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public abstract void buffered(NatsMessage msg);
}
