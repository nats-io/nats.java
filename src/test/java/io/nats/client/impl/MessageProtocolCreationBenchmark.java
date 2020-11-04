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

package io.nats.client.impl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;

public class MessageProtocolCreationBenchmark {
    public static void main(String args[]) throws InterruptedException {
        int warmup = 1_000_000;
        int msgCount = 50_000_000;

        System.out.printf("### Running benchmarks with %s messages.\n", NumberFormat.getInstance().format(msgCount));

        for (int j = 0; j < warmup; j++) {
            new NatsMessage("subject", "replyTo", NatsConnection.EMPTY_BODY, true);
        }

        long start = System.nanoTime();
        for (int j = 0; j < msgCount; j++) {
            new NatsMessage("subject", "replyTo", NatsConnection.EMPTY_BODY, false);
        }
        long end = System.nanoTime();

        System.out.printf("\n### Total time to create %s non-utf8 messages for sending was %s ms\n\t%f ns/op\n\t%s op/sec\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)),
                NumberFormat.getInstance().format(((double)(1_000_000_000L * msgCount))/((double) (end - start))));

        start = System.nanoTime();
        for (int j = 0; j < msgCount; j++) {
            new NatsMessage("subject", "replyTo", NatsConnection.EMPTY_BODY, true);
        }
        end = System.nanoTime();

        System.out.printf("\n### Total time to create %s utf8 messages for sending was %s ms\n\t%f ns/op\n\t%s op/sec\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)),
                NumberFormat.getInstance().format(((double)(1_000_000_000L * msgCount))/((double) (end - start))));
        
        start = System.nanoTime();
        for (int j = 0; j < msgCount; j++) {
            new NatsMessage(ByteBuffer.allocate(0));
        }
        end = System.nanoTime();

        System.out.printf("\n### Total time to create %s a protocol message was %s ms\n\t%f ns/op\n\t%s op/sec\n",
                NumberFormat.getInstance().format(msgCount),
                NumberFormat.getInstance().format((end - start) / 1_000_000L),
                ((double) (end - start)) / ((double) (msgCount)),
                NumberFormat.getInstance().format(((double)(1_000_000_000L * msgCount))/((double) (end - start))));
    }
}