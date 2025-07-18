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

package io.nats.client;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TestingDynamicTokenSupplier implements Supplier<char[]> {
    AtomicInteger counter = new AtomicInteger(0);

    @Override
    public char[] get() {
        return ("dynamic-token-" + counter.incrementAndGet()).toCharArray();
    }
}
