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

import io.nats.client.Message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class WriteListener implements AutoCloseable {
    public final ExecutorService executorService;

    public WriteListener() {
        executorService = Executors.newSingleThreadExecutor();
    }

    final void accept(Message msg, String mode) {
        executorService.submit(() -> buffered(msg, mode));
    }

    public abstract void buffered(Message msg, String mode);

    @Override
    public void close() throws Exception {
        executorService.shutdown();
    }
}
