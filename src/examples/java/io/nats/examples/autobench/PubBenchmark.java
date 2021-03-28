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

package io.nats.examples.autobench;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;

public class PubBenchmark extends AutoBenchmark {

    public PubBenchmark(String name, long messageCount, long messageSize) {
        super(name, messageCount, messageSize);
    }

    public void execute(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String subject = getSubject();

        try {
            Connection nc = Nats.connect(connectOptions);
            try {
                this.startTiming();
                for(int i = 0; i < this.getMessageCount(); i++) {
                    nc.publish(subject, payload);
                }
                defaultFlush(nc);
                this.endTiming();
            } finally {
                nc.close();
            }
        } catch (IOException ex) {
            this.setException(ex);
        }
    }
}