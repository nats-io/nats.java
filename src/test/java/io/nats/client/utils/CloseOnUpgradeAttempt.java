// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.utils;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import io.nats.client.impl.SocketDataPort;

public class CloseOnUpgradeAttempt extends SocketDataPort {
    public CloseOnUpgradeAttempt() {
        super(); // Start with a very small buffer size
    }

    @Override
    public void upgradeToSecure() throws IOException, ExecutionException, InterruptedException {
        this.close();
        super.upgradeToSecure();
    }
}