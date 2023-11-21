// Copyright 2021-2022 The NATS Authors
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

package io.nats.examples.testapp.support;

public class CommandLineConsumer {
    public final ConsumerType consumerType;
    public final ConsumerKind consumerKind;
    public final int batchSize;
    public final long expiresIn;

    public CommandLineConsumer(String consumerKind) {
        this.consumerType = ConsumerType.Push;
        this.consumerKind = ConsumerKind.instance(consumerKind);
        batchSize = 0;
        expiresIn = 0;
    }

    public CommandLineConsumer(String consumerKind, int batchSize, long expiresIn) {
        this.consumerType = ConsumerType.Simple;
        this.consumerKind = ConsumerKind.instance(consumerKind);
        if (batchSize < 1) {
            throw new IllegalArgumentException("Invalid Batch Size:" + batchSize);
        }
        this.batchSize = batchSize;
        if (expiresIn < 1_000) {
            throw new IllegalArgumentException("Expires must be >= 1000ms");
        }
        this.expiresIn = expiresIn;
    }

    @Override
    public String toString() {
        if (consumerType == ConsumerType.Simple) {
            return consumerType.toString().toLowerCase() +
                " " + consumerKind.toString().toLowerCase() +
                " " + batchSize +
                " " + expiresIn;
        }
        return consumerType.toString().toLowerCase() +
            " " + consumerKind.toString().toLowerCase();
    }
}
