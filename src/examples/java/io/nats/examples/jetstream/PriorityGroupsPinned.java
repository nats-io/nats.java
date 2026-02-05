package io.nats.examples.jetstream;
//package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;
import io.nats.examples.jetstream.ResilientPublisher;


import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example demonstrates a pinned consumer for priority groups.
 *
 * Priority groups are a new set of features allowing warm standby and
 * dynamic worker pools processing messages on the same consumer.
 *
 * This example shows a standby worker activating when the primary worker
 * becomes unavailable, with messages "pinned" to specific workers based on
 * their priority within the group.
 *
 */
public class PriorityGroupsPinned {
	 private static final String STREAM = "priority-groups-pinned";
	 private static final String SUBJECT = "priority-groups-pinned";
	 private static final String CONSUMER_NAME = "pinned-consumer";
	 private static final String MESSAGE_PREFIX = "consume";
	 private static final String PRIORITY_GROUP = "group";
	 private static final int PUBLISH_DELAY = 500;
	 private static final int PRE_FETCH_SIZE = 10;
	 private static final String SERVER = "nats://localhost:4222";


	 static Thread pubThread;

	 public static void primaryConsumer() {

		 Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         // get stream context, create consumer with pinned priority policy and get the consumer context
	         StreamContext streamContext;
	         streamContext = nc.getStreamContext(STREAM);
	         ConsumerContext consumerContext;
	         consumerContext = streamContext.getConsumerContext(CONSUMER_NAME);

	         MessageHandler handler = msg -> {
	             msg.ack();
	             System.out.println("Consumed message in primary consumer: " + new String(msg.getData()));
	         };

	         System.out.println("Starting primary consumer - should be active right away and handle all messages");

	         ConsumeOptions consumerOptions = new ConsumeOptions.Builder()
	        		 .batchSize(PRE_FETCH_SIZE)
	        		 .thresholdPercent(50)
	        		 .group(PRIORITY_GROUP)
	        		 .build();
	     	 // create the consumer and install handler
	     	 MessageConsumer consumer = consumerContext.consume(consumerOptions, handler);

	     	 Thread.sleep(5000);
	     	 System.out.println("Now terminating the primary consumer. The standby should take over.");

	     	 //Unpin bypasses the timeout. Standby will take
	     	 consumerContext.unpin(PRIORITY_GROUP);
	     	 consumer.stop();
	     	 System.out.println(consumer.isStopped());

	     	 // Wait for the publish thread to terminate
	     	 pubThread.join();

	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not be handled, exiting.");
	         System.exit(-1);
	     }

	 }

	 public static void standbyConsumer() {
		 Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         // get stream context, create consumer with pinned priority policy and get the consumer context
	         StreamContext streamContext;
	         streamContext = nc.getStreamContext(STREAM);
	         ConsumerContext consumerContext;
	         consumerContext = streamContext.getConsumerContext(CONSUMER_NAME);

	         MessageHandler handler = msg -> {
	             msg.ack();
	             System.out.println("WARNING: Consumed message in standby consumer: " + new String(msg.getData()));
	         };

	         System.out.println("Starting standby consumer - should only become active if primary consumer fails or disconnects. The timeout is set to 5 seconds.");

	         ConsumeOptions consumerOptions = new ConsumeOptions.Builder()
	        		 .batchSize(PRE_FETCH_SIZE)
	        		 .group(PRIORITY_GROUP)
	        		 .build();
	     	 // create the consumer and install handler
	     	 MessageConsumer consumer = consumerContext.consume(consumerOptions, handler);

	     	 // Wait for the publish thread to terminate
	     	 pubThread.join();

	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not be handled, exiting.");
	         System.exit(-1);
	     }

	 }

	 public static void main(String[] args) {
	     Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         JetStreamManagement jsm = nc.jetStreamManagement();
	         createOrReplaceStream(jsm, STREAM, SUBJECT);

	         // Utility for filling the stream with some messages
	         System.out.println("Starting publish...");
	         // Publishing
	         ResilientPublisher publisher = new ResilientPublisher(nc, jsm, STREAM, SUBJECT).basicDataPrefix(MESSAGE_PREFIX).delay(PUBLISH_DELAY);

	         pubThread = new Thread(publisher);
	         pubThread.start();

	         // get stream context, create consumer with pinned priority policy and get the consumer context
	         StreamContext streamContext;


	         streamContext = nc.getStreamContext(STREAM);
	         streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
	        		 .durable(CONSUMER_NAME)
	        		 .priorityGroups(PRIORITY_GROUP)
	        		 .priorityPolicy(PriorityPolicy.PinnedClient)  //Pinned policy
	        		 .priorityTimeout(5000)   //Inactivity timeout 5 seconds
	        		 .maxAckPending(20)
	        		 .build());

	         Thread primaryConsumer = new Thread(() -> {
	        	 primaryConsumer();
	         });
	         primaryConsumer.start();

	         Thread.sleep(1000);

	         Thread standbyConsumer = new Thread(() -> {
	        	 standbyConsumer();
	         });
	         standbyConsumer.start();

	         // Wait for the publish thread to terminate
	         pubThread.join();
	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not be handled, exiting.");
	         System.exit(-1);
	     }
	 }
}