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
    final int batchSize;
    final long expiresIn;
    final Thread t;

    FetchConsumer fc;

    public SimpleFetchConsumer(CommandLine cmd, ConsumerKind consumerKind, int batchSize, long expiresIn) throws IOException, InterruptedException, JetStreamApiException {
        super(cmd, "fc", consumerKind);
        if (consumerKind == ConsumerKind.Ordered) {
            throw new IllegalArgumentException("Ordered Consumer not supported for App Simple Fetch");
        }

        this.batchSize = batchSize;
        this.expiresIn = expiresIn;

        sc = nc.getStreamContext(cmd.stream);

        cc = sc.createOrUpdateConsumer(newCreateConsumer().build());
        Output.controlMessage(label, cc.getConsumerName());
        t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(batchSize).expiresIn(expiresIn).build();
        Output.controlMessage(label, toString(fco));

        //noinspection InfiniteLoopStatement
        while (true) {
            try (FetchConsumer autoCloseableFc = cc.fetch(fco)) {
                fc = autoCloseableFc;
                Message m = fc.nextMessage();
                while (m != null) {
                    onMessage(m);
                    m = fc.nextMessage();
                }
            }
            catch (Exception e) {
                // if there was an error, just try again
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
            updateLabel(fc.getConsumerName());
        }
    }

    public static String toString(FetchConsumeOptions fco) {
        return "FetchConsumeOptions" +
            "\nMax Messages: " + fco.getMaxMessages() +
            "\nMax Bytes: " + fco.getMaxBytes() +
            "\nExpires In: " + fco.getExpiresInMillis() +
            "\nIdleHeartbeat: " + fco.getIdleHeartbeat() +
            "\nThreshold Percent: " + fco.getThresholdPercent();
    }
}
