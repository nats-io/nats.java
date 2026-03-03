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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * ----------------------------------------------------------------------
 * Priority groups are a new set of features allowing warm standby and
 * dynamic worker pools processing messages on the same consumer.
 * ----------------------------------------------------------------------
 * This example demonstrates a standby or overflow consumer for priority
 * groups, triggered by a min ack pending value
 * ----------------------------------------------------------------------
 */
public class PriorityGroupsOverFlowMinAckPending {
	private static final String SERVER = "nats://localhost:4222";
	private static final String STREAM = "ack-stream";
	private static final String SUBJECT = "ack-subject";
	private static final String CONSUMER_NAME = "ack-consumer";
	private static final String PRIORITY_GROUP = "ack-group";
	private static final String MESSAGE_PREFIX = "ack-msg-";

	// overflow threshold if the consumer instance has this many outstanding acks
	private static final int OVERFLOW_MIN_ACK_PENDING = 10;

	// size of the simplified-consume's batch in messages
	private static final int CONSUME_BATCH_SIZE = 10;

	// number of messages to seed the publish. We don't need many to demonstrate
	private static final int PUBLISH_COUNT = 100;

	// stall the primary processing causes the threshold to be reached
	// and the server to start sending messages to the overflow
	// do the delay after a number of messages
	private static final long PRIMARY_STALL = 5000;
	private static final int PRIMARY_STALL_AFTER = 20;

	// the amount of "work" to simulate when a message is received by the handler
	private static final long PRIMARY_WORK_TIME = 100;
	private static final long OVERFLOW_WORK_TIME = 500;

	public static void main(String[] args) {
		Options options = Options.builder().server(SERVER).build();
		try (Connection nc = Nats.connect(options)) {
			JetStreamManagement jsm = nc.jetStreamManagement();
			createOrReplaceStream(jsm, STREAM, SUBJECT);

			// Publish messages ahead of time for this example
			System.out.printf("Publishing %d messages.\n", PUBLISH_COUNT);
			for (int x = 1; x <= PUBLISH_COUNT; x++) {
				nc.publish(SUBJECT, (MESSAGE_PREFIX + x).getBytes(StandardCharsets.ISO_8859_1));
			}

			// get stream context, create consumer with overflow  priority policy and get the consumer context
			StreamContext streamContext = nc.getStreamContext(STREAM);
			streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
				.durable(CONSUMER_NAME)
				.priorityGroups(PRIORITY_GROUP)
				.priorityPolicy(PriorityPolicy.Overflow)
				.build());

			// some control variables
			CountDownLatch stopLatch = new CountDownLatch(PUBLISH_COUNT);

			// set up the consumer instances...
			// a new ConsumerContext for each so they have their own dispatcher

			// set up and start the primary consumer
			ConsumerContext primaryCC = streamContext.getConsumerContext(CONSUMER_NAME);
			ConsumeOptions primaryConsumeOpts = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.build();
			MinAckPendingConsumer primaryMapc = new MinAckPendingConsumer("Primary", true, primaryCC, primaryConsumeOpts, PRIMARY_WORK_TIME, stopLatch);
			Thread primaryThread = new Thread(primaryMapc);
			primaryThread.start();

			ConsumerContext overflowCC = streamContext.getConsumerContext(CONSUMER_NAME);
			ConsumeOptions overflowConsumeOpts = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.minAckPending(OVERFLOW_MIN_ACK_PENDING)
				.build();
			MinAckPendingConsumer overflowMapc = new MinAckPendingConsumer("Overflow", false, overflowCC, overflowConsumeOpts, OVERFLOW_WORK_TIME, stopLatch);
			Thread overflowThread = new Thread(overflowMapc);
			overflowThread.start();

			primaryThread.join();
			overflowThread.join();
		}
		catch (Exception e) {
			System.err.println("Exception in main: " + e.getMessage());
			//noinspection CallToPrintStackTrace
			e.printStackTrace();
		}
	}

	static class MinAckPendingConsumer implements Runnable {
		final String label;
		final boolean primary;
		final ConsumerContext consumerContext;
		final ConsumeOptions consumeOptions;
		final long workTime;
		final CountDownLatch stopLatch;
		final int delayOn;
		final AtomicInteger count;

		public MinAckPendingConsumer(String label, boolean primary, ConsumerContext consumerContext, ConsumeOptions consumeOptions, long workTime, CountDownLatch stopLatch)
		{
			this.label = label;
			this.primary = primary;
			this.consumerContext = consumerContext;
			this.consumeOptions = consumeOptions;
			this.workTime = workTime;
			this.stopLatch = stopLatch;
			this.delayOn = PRIMARY_STALL_AFTER + 1;
			this.count = new AtomicInteger(0);
		}

		@Override
		public void run() {
			try {
				System.out.printf("%-8s | Starting...\n", label);

				// 1. call consumer with handler and options
				// 2. wait for the latch
				try (MessageConsumer consumer = consumerContext.consume(consumeOptions, handler())) {
					stopLatch.await();
				}
			}
			catch (Exception e) {
				System.err.println("Exception in " + label + ": " + e.getMessage());
				//noinspection CallToPrintStackTrace
				e.printStackTrace();
			}
		}

		private MessageHandler handler() {
			return msg -> {
				int c = count.incrementAndGet();
				if (primary) {
					if (c == PRIMARY_STALL_AFTER + 1) {
						System.out.printf("%-8s | Stalling primary...\n", label);
						Thread.sleep(PRIMARY_STALL);
					}
				}
				Thread.sleep(workTime);
				msg.ack();
				System.out.printf("%-8s | %4d | Data: %s\n", label, c, new String(msg.getData()));
				stopLatch.countDown();
			};
		}
	}
}
