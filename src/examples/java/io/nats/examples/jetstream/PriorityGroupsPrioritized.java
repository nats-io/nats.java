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
 * This example demonstrates a consumer being prioritized, meaning it is the
 * sole processor of messages, and then being shutdown and then
 * messages moves to another instance of the consumer.
 * ----------------------------------------------------------------------
 */
public class PriorityGroupsPrioritized {
	private static final String SERVER = "nats://localhost:4222";
	private static final String STREAM = "priority-stream";
	private static final String SUBJECT = "priority-subject";
	private static final String CONSUMER_NAME = "priority-consumer";
	private static final String PRIORITY_GROUP = "priority-group";
	private static final String MESSAGE_PREFIX = "priority-";

	// size of the simplified-consume's batch in messages
	private static final int CONSUME_BATCH_SIZE = 10;

	// number of messages to seed the publish. We don't need many to demonstrate
	private static final int PUBLISH_COUNT = 200;

	// the amount of "work" to simulate when a message is received by the handler
	private static final long WORK_TIME = 250;

	public static void main(String[] args) {
		Options options = Options.builder().server(SERVER)
			.build();
		try (Connection nc = Nats.connect(options)) {
			JetStreamManagement jsm = nc.jetStreamManagement();
			createOrReplaceStream(jsm, STREAM, SUBJECT);

			// Publish messages ahead of time for this example
			System.out.printf("Publishing %d messages.\n", PUBLISH_COUNT);
			for (int x = 1; x <= PUBLISH_COUNT ; x++) {
				nc.publish(SUBJECT, (MESSAGE_PREFIX + x).getBytes(StandardCharsets.ISO_8859_1));
			}

			// - get stream context,
			// - create consumer with overflow priority policy
			StreamContext streamContext = nc.getStreamContext(STREAM);
			ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
				.durable(CONSUMER_NAME)
				.priorityGroups(PRIORITY_GROUP)
				.priorityPolicy(PriorityPolicy.Prioritized)
				.build());

			// some control variables
			CountDownLatch stopLatch1 = new CountDownLatch(PUBLISH_COUNT / 8);
			CountDownLatch stopLatch2 = new CountDownLatch(PUBLISH_COUNT / 4);
			CountDownLatch stopLatch3 = new CountDownLatch(PUBLISH_COUNT / 4);

			// set up the consumer instances...
			// a new ConsumerContext for each so they have their own dispatcher
			ConsumeOptions consumeOpts1and3 = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.expiresIn(5000)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.priority(1)
				.build();
			ConsumeOptions consumeOpts2 = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.expiresIn(5000)
				.thresholdPercent(50)
				.group(PRIORITY_GROUP)
				.priority(2)
				.build();

			// set up and start the first consumer
			ConsumerContext cc1 = streamContext.getConsumerContext(CONSUMER_NAME);
			PriorityConsumer pinned1 = new PriorityConsumer("P1", cc1, consumeOpts1and3, stopLatch1);
			Thread thread1 = new Thread(pinned1);
			thread1.start();

			// set up and start the second and third consumer
			// give the first a little time to do some work
			Thread.sleep(2500);
			ConsumerContext cc2 = streamContext.getConsumerContext(CONSUMER_NAME);
			PriorityConsumer pinned2 = new PriorityConsumer("P2", cc2, consumeOpts2, stopLatch2);
			Thread thread2 = new Thread(pinned2);
			thread2.start();

			thread1.join();

			Thread.sleep(2500);
			ConsumerContext cc3 = streamContext.getConsumerContext(CONSUMER_NAME);
			PriorityConsumer pinned3 = new PriorityConsumer("P3", cc3, consumeOpts1and3, stopLatch3);
			Thread thread3 = new Thread(pinned3);
			thread3.start();

			thread2.join();
			thread3.join();
		}
		catch (Exception e) {
			System.err.println("Exception in main: " + e.getMessage());
			//noinspection CallToPrintStackTrace
			e.printStackTrace();
		}
	}

	static class PriorityConsumer implements Runnable {
		final String label;
		final ConsumerContext consumerContext;
		final ConsumeOptions consumeOptions;
		final CountDownLatch stopLatch;
		final AtomicInteger count;

		public PriorityConsumer(String label, ConsumerContext consumerContext, ConsumeOptions consumeOptions, CountDownLatch stopLatch)
		{
			this.label = label;
			this.consumerContext = consumerContext;
			this.consumeOptions = consumeOptions;
			this.stopLatch = stopLatch;
			this.count = new AtomicInteger(0);
		}

		@Override
		public void run() {
			try {
				// 1. call consumer with handler and options
				// 2. wait for the latch
				System.out.printf("%-3s | Start consuming...\n", label);
				try (MessageConsumer consumer = consumerContext.consume(consumeOptions, handler())) {
					stopLatch.await();
				}
				System.out.printf("%-3s | Shutting Down...\n", label);
			}
			catch (Exception e) {
				System.err.printf("%-3s | Exception: %s\n", label, e.getMessage());
			}
		}

		private MessageHandler handler() {
			return msg -> {
				Thread.sleep(WORK_TIME);
				msg.ack();
				System.out.printf("%-3s | %4d | Data: %s\n", label, count.incrementAndGet(), new String(msg.getData()));
				stopLatch.countDown();
			};
		}
	}
}
