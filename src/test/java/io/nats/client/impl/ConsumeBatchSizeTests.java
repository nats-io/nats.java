// Copyright 2026 The NATS Authors
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

import io.nats.client.*;
import io.nats.client.api.OrderedConsumerConfiguration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A consume reads the whole stream whatever its batch size. Batch size 1 is the edge: the re-pull
 * threshold is the batch size less the re-pull size, and with a batch of 1 that arithmetic lands
 * on 0, which the pending count can never go below, so the consumer pulls once and stops.
 */
public class ConsumeBatchSizeTests extends JetStreamTestBase {

    private static final int COUNT = 5;

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 10})
    public void testConsumeReadsEveryMessageAtAnyBatchSize(int batchSize) throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 1, COUNT);

            AtomicInteger received = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(COUNT);
            MessageHandler handler = m -> {
                received.incrementAndGet();
                latch.countDown();
            };

            StreamContext sctx = nc.getStreamContext(tsc.stream);
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(
                new OrderedConsumerConfiguration().filterSubject(tsc.subject()));

            try (MessageConsumer mc = occtx.consume(ConsumeOptions.builder().batchSize(batchSize).build(), handler)) {
                //noinspection ResultOfMethodCallIgnored
                latch.await(5000, TimeUnit.MILLISECONDS);
            }

            assertEquals(COUNT, received.get(), "batch size " + batchSize);
        });
    }
}
