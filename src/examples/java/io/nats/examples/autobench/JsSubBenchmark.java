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

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;
import java.time.Duration;

public class JsSubBenchmark extends AutoBenchmark {

    public JsSubBenchmark(String name, long messageCount, long messageSize) {
        super(name, messageCount, messageSize);
    }

    public void execute(Options connectOptions) throws InterruptedException {
        String subject = JsPubBenchmark.getSubject(getMessageCount(), getMessageSize());

        try {
            Connection nc = Nats.connect(connectOptions);

            JetStream js = nc.jetStream();
            ConsumerConfiguration c = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(c).build();
            JetStreamSubscription jsSub = js.subscribe(subject, pso);

            try {
                this.startTiming();
                for (int i = 0; i < this.getMessageCount(); i++) {
                    jsSub.nextMessage(Duration.ofSeconds(1));
                }
                defaultFlush(nc);
                this.endTiming();
            } finally {
                try {
                    nc.jetStreamManagement().deleteStream(JsPubBenchmark.getStream(getMessageCount(), getMessageSize()));
                } catch (IOException | JetStreamApiException ex) {
                    this.setException(ex);
                }
                finally {
                    nc.close();
                }
            }
        } catch (IOException | JetStreamApiException ex) {
            this.setException(ex);
        }
    }
}
