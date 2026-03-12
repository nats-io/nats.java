package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsJetStreamConstants.NATS_PIN_ID_HDR;
import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * ----------------------------------------------------------------------
 * Priority groups are a new set of features allowing warm standby and
 * dynamic worker pools processing messages on the same consumer.
 * ----------------------------------------------------------------------
 * This example demonstrates a consumer being pinned, meaning it is the
 * sole processor of messages, and then being unpinned and then processing
 * of messages moves to another instance of the consumer.
 * ----------------------------------------------------------------------
 */
public class PriorityGroupsPinned {
	private static final String SERVER = "nats://localhost:4222";
	private static final String STREAM = "pinned-stream";
	private static final String SUBJECT = "pinned-subject";
	private static final String CONSUMER_NAME = "pinned-consumer";
	private static final String PIN_GROUP = "pinned-group";
	private static final String MESSAGE_PREFIX = "pinned-";

	// size of the simplified-consume's batch in messages
	private static final int CONSUME_BATCH_SIZE = 10;

	// number of messages to seed the publish. We don't need many to demonstrate
	private static final int PUBLISH_COUNT = 200;

	// the amount of "work" to simulate when a message is received by the handler
	private static final long WORK_TIME = 250;

	public static void main(String[] args) {
		Options options = Options.builder().server(SERVER).build();
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
				.priorityGroups(PIN_GROUP)
				.priorityPolicy(PriorityPolicy.PinnedClient)
				.build());

			// some control variables
			CountDownLatch[] unpinLatches = new CountDownLatch[2];
			unpinLatches[0] = new CountDownLatch(PUBLISH_COUNT / 5);
			unpinLatches[1] = new CountDownLatch(PUBLISH_COUNT * 2 / 5);
			CountDownLatch stopLatch = new CountDownLatch(PUBLISH_COUNT);
			AtomicReference<String> currentPin = new AtomicReference<>();

			// set up the consumer instances...
			// a new ConsumerContext for each so they have their own dispatcher
			ConsumeOptions consumeOpts = new ConsumeOptions.Builder()
				.batchSize(CONSUME_BATCH_SIZE)
				.expiresIn(5000) // instead of the default 30 seconds
				.thresholdPercent(50) // instead of the default 25 percent
				.group(PIN_GROUP)
				.build();

			// set up and start the first consumer
			ConsumerContext cc1 = streamContext.getConsumerContext(CONSUMER_NAME);
			PinnedConsumer pinned1 = new PinnedConsumer("First", cc1, consumeOpts, unpinLatches, stopLatch, currentPin);
			Thread thread1 = new Thread(pinned1);
			thread1.start();

			// set up and start the second and third consumer
			// give the first a little time to become the pinned one
			Thread.sleep(500);
			ConsumerContext cc2 = streamContext.getConsumerContext(CONSUMER_NAME);
			PinnedConsumer pinned2 = new PinnedConsumer("Second", cc2, consumeOpts, unpinLatches, stopLatch, currentPin);
			Thread thread2 = new Thread(pinned2);
			thread2.start();

			unpinLatches[0].await();
			System.out.println("Unpinning...");
			// any instance of the ConsumerContext will work
			consumerContext.unpin(PIN_GROUP);

			unpinLatches[1].await();
			System.out.println("Unpinning...");
			// you can also unpin from the management context
			jsm.unpinConsumer(STREAM, CONSUMER_NAME, PIN_GROUP);

			thread1.join();
			thread2.join();
		}
		catch (Exception e) {
			System.err.println("Exception in main: " + e.getMessage());
			//noinspection CallToPrintStackTrace
			e.printStackTrace();
		}
	}

	static class PinnedConsumer implements Runnable {
		final String label;
		final ConsumerContext consumerContext;
		final ConsumeOptions consumeOptions;
		final CountDownLatch[] unpinLatches;
		final CountDownLatch stopLatch;
		final AtomicReference<String> currentPin;
		final AtomicInteger count;

		public PinnedConsumer(String label, ConsumerContext consumerContext, ConsumeOptions consumeOptions,
							  CountDownLatch[] unpinLatches,
                              CountDownLatch stopLatch,
							  AtomicReference<String> currentPin)
		{
			this.label = label;
			this.consumerContext = consumerContext;
			this.consumeOptions = consumeOptions;
			this.unpinLatches = unpinLatches;
			this.stopLatch = stopLatch;
			this.currentPin = currentPin;
			this.count = new AtomicInteger(0);
		}

		@Override
		public void run() {
			try {
				// 1. call consumer with handler and options
				// 2. wait for the latch
				System.out.printf("%-6s | Start consuming...\n", label);
				try (MessageConsumer consumer = consumerContext.consume(consumeOptions, handler())) {
					stopLatch.await();
				}
				System.out.printf("%-6s | Shutting Down...\n", label);
			}
			catch (Exception e) {
				System.err.printf("%-6s | Exception: %s\n", label, e.getMessage());
			}
		}

		private MessageHandler handler() {
			return msg -> {
				Thread.sleep(WORK_TIME);
				msg.ack();
				String natsPinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
				currentPin.set(natsPinId);
				System.out.printf("%-6s | %4d | Pin: %s | Data: %s\n", label, count.incrementAndGet(), natsPinId, new String(msg.getData()));
				for (CountDownLatch unpinLatch : unpinLatches) {
					unpinLatch.countDown();
				}
				stopLatch.countDown();
			};
		}
	}
}
