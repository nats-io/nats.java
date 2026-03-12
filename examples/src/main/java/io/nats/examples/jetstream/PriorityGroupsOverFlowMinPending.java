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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * ----------------------------------------------------------------------
 * Priority groups are a new set of features allowing warm standby and
 * dynamic worker pools processing messages on the same consumer.
 * ----------------------------------------------------------------------
 * This example demonstrates a standby or overflow consumer for priority
 * groups, triggered by a min pending value
 * ----------------------------------------------------------------------
 */
public class PriorityGroupsOverFlowMinPending {
	private static final String SERVER = "nats://localhost:4222";
	private static final String STREAM = "pend-stream";
	private static final String SUBJECT = "pend-subject";
	private static final String CONSUMER_NAME = "pend-consumer";
	private static final String PRIORITY_GROUP = "pend-group";
	private static final String MESSAGE_PREFIX = "pend-msg";

	// overflow threshold if the consumer (not the instance) has this many messages pending
	private static final int OVERFLOW_MIN_PENDING = 50;

	// size of the simplified-consume's batch in messages
	private static final int CONSUME_BATCH_SIZE = 10;

	// delay after each publish. Small at first to load up the stream and trip the min_pending threshold
	// so the server starts sending messages to the overflow. Switch to long delay later so the primary
	// catches up, pending goes down under the threshold and the server stops sending to the overflow
	private static final int INITIAL_PUBLISH_DELAY = 50;
	private static final int LONG_PUBLISH_DELAY = 1000;
	private static final int SWITCH_PUBLISH_DELAY_AT = 50;

	// the amount of "work" to simulate when a message is received by the handler
	private static final long WORK_TIME = 250;

	// The overflow will stop getting messages.
	// Stop the program after it hasn't seen a message for this long
	private static final long STOP_TIME = 5000;

	public static void main(String[] args) {
		Options options = Options.builder().server(SERVER).build();
		try (Connection nc = Nats.connect(options)) {
			JetStreamManagement jsm = nc.jetStreamManagement();
			createOrReplaceStream(jsm, STREAM, SUBJECT);

			// Publishing
			System.out.println("Starting publish...");
			ResilientPublisher publisher = new ResilientPublisher(nc, jsm, STREAM, SUBJECT)
				.basicDataPrefix(MESSAGE_PREFIX)
				.delay(INITIAL_PUBLISH_DELAY);
			Thread pubThread = new Thread(publisher);
			pubThread.start();

			// - get stream context,
			// - create consumer with overflow priority policy
			StreamContext streamContext = nc.getStreamContext(STREAM);
			streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
				.durable(CONSUMER_NAME)
				.priorityGroups(PRIORITY_GROUP)
				.priorityPolicy(PriorityPolicy.Overflow)
				.build());

			// some control variables
			CountDownLatch stopLatch = new CountDownLatch(1);
			CountDownLatch switchPrintingLatch = new CountDownLatch(SWITCH_PUBLISH_DELAY_AT);
			AtomicLong lastOverflowReceived = new AtomicLong(0);

			// set up the consumer instances...
			// a new ConsumerContext for each so they have their own dispatcher

			// set up and start the primary consumer
			ConsumerContext primaryCC = streamContext.getConsumerContext(CONSUMER_NAME);
			ConsumeOptions primaryConsumeOpts = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.build();
			MinPendingConsumer primaryMpc = new MinPendingConsumer(
				"Primary", true, primaryCC, primaryConsumeOpts, stopLatch, switchPrintingLatch, lastOverflowReceived);
			Thread primaryThread = new Thread(primaryMpc);
			primaryThread.start();

			// set up and start the overflow consumer
			ConsumerContext overflowCC = streamContext.getConsumerContext(CONSUMER_NAME);
			ConsumeOptions overflowConsumeOpts = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.minPending(OVERFLOW_MIN_PENDING)
				.build();
			MinPendingConsumer overflowMpc = new MinPendingConsumer(
				"Overflow", false, overflowCC, overflowConsumeOpts, stopLatch, switchPrintingLatch, lastOverflowReceived);
			Thread overflowThread = new Thread(overflowMpc);
			overflowThread.start();

			switchPrintingLatch.await();
			System.out.println("Switch to slower publishing, will reduce message pending...");
			publisher.delay(LONG_PUBLISH_DELAY);

			primaryThread.join();
			overflowThread.join();
			publisher.stop();
			pubThread.join();
		}
		catch (Exception e) {
			System.err.println("Exception in main: " + e.getMessage());
			//noinspection CallToPrintStackTrace
			e.printStackTrace();
		}
	}

	static class MinPendingConsumer implements Runnable {
		final String label;
		final boolean primary;
		final ConsumerContext consumerContext;
		final ConsumeOptions consumeOptions;
		final CountDownLatch stopLatch;
		final CountDownLatch switchPrintingLatch;
		final AtomicLong lastOverflowReceived;
		final AtomicInteger count;

		public MinPendingConsumer(String label, boolean primary, ConsumerContext consumerContext, ConsumeOptions consumeOptions,
								  CountDownLatch stopLatch, CountDownLatch switchPrintingLatch, AtomicLong lastOverflowReceived)
		{
			this.label = label;
			this.primary = primary;
			this.consumerContext = consumerContext;
			this.consumeOptions = consumeOptions;
			this.stopLatch = stopLatch;
			this.switchPrintingLatch = switchPrintingLatch;
			this.lastOverflowReceived = lastOverflowReceived;
			this.count = new AtomicInteger(0);
		}

		@Override
		public void run() {
			try {
				// 1. call consumer with handler and options
				// 2. wait for the latch
				System.out.printf("%-8s | Start consuming...\n", label);
				try (MessageConsumer consumer = consumerContext.consume(consumeOptions, handler())) {
					stopLatch.await();
				}
			}
			catch (Exception e) {
				System.err.printf("%-8s | Exception: %s\n", label, e.getMessage());
			}
		}

		private MessageHandler handler() {
            return msg -> {
                if (primary) {
                    long last = lastOverflowReceived.get();
                    if (last > 0 && ((System.currentTimeMillis() - last) > STOP_TIME) && stopLatch.getCount() > 0) {
                        System.out.println("Pending lower than threshold, messages stopped going to overflow. Exiting.");
                        stopLatch.countDown();
                    }
                }
                else if (lastOverflowReceived.getAndSet(System.currentTimeMillis()) == 0) {
                    System.out.println("Pending has crossed the threshold.");
                }
                Thread.sleep(WORK_TIME);
                msg.ack();
                System.out.printf("%-8s | %4d | Data: %s\n", label, count.incrementAndGet(), new String(msg.getData()));
                if (switchPrintingLatch != null) {
                    if (switchPrintingLatch.getCount() > 0) {
                        switchPrintingLatch.countDown();
                    }
                }
            };
		}
	}
}
