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

package io.nats.examples.chaosTestApp;

import io.nats.client.*;
import io.nats.examples.chaosTestApp.support.CommandLine;
import io.nats.examples.chaosTestApp.support.ConsumerKind;

import java.io.IOException;

public class SimpleFetchConsumer extends ConnectableConsumer implements Runnable {
    final StreamContext sc;
    final ConsumerContext cc;
    FetchConsumer fc;
    final int batchSize;
    final long expiresIn;
    Thread t;

    public SimpleFetchConsumer(CommandLine cmd, ConsumerKind consumerKind, int batchSize, long expiresIn) throws IOException, InterruptedException, JetStreamApiException {
        super(cmd, "fc", consumerKind);
        if (consumerKind == ConsumerKind.Ordered) {
            throw new IllegalArgumentException("Ordered Consumer not supported for App Simple Fetch");
        }

        this.batchSize = batchSize;
        this.expiresIn = expiresIn;

        sc = nc.getStreamContext(cmd.stream);

        ConsumeOptions co = ConsumeOptions.builder()
            .batchSize(batchSize)
            .expiresIn(expiresIn)
            .build();

        cc = sc.createOrUpdateConsumer(newCreateConsumer().build());
        Output.controlMessage(label, cc.getConsumerName());
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {
        //noinspection InfiniteLoopStatement
        while (true) {
            FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(batchSize).expiresIn(expiresIn).build();
            try (FetchConsumer fc = cc.fetch(fco)) {
                Message m = fc.nextMessage();
                while (m != null) {
                    onMessage(m);
                    m = fc.nextMessage();
                }
            }
            catch (Exception e) {
                // do we care if the autocloseable FetchConsumer errors on close?
                // probably not, but maybe log it.
            }

            // simulating some work to be done between fetches
            try {
                Output.workMessage(label, "Fetch Batch Completed, Last Received Seq: " + lastReceivedSequence.get());
                //noinspection BusyWait
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void refreshInfo() {
        if (fc != null) {
            updateNameAndLabel(fc.getConsumerName());
        }
    }
}
